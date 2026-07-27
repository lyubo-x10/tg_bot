import os
import requests
from datetime import datetime, timedelta

METABASE_URL = 'https://x10.metabaseapp.com'
METABASE_USERNAME = os.environ['METABASE_USERNAME']
METABASE_PASSWORD = os.environ['METABASE_PASSWORD']
BOT_TOKEN = os.environ['TELEGRAM_BOT_TOKEN']
CHAT_ID = os.environ['TELEGRAM_CHAT_ID']

DATABASE_ID = 100

# exchange_liquidity_stats (table 2575, schema stats)
T_EXCH = 2575
F_PERIOD, F_EXCHANGE, F_MARKET_E = 9439, 9440, 9441
F_ASK_0015, F_ASK_0030 = 9444, 9445
F_BID_0015, F_BID_0030 = 9448, 9449

# depths (table 874)
T_DEPTHS = 874
F_D_TS, F_D_SPREAD, F_D_ASK, F_D_BID, F_D_MKT = 4411, 4407, 4408, 4412, 4410

# assets.markets (table 876)
T_MARKETS, F_M_ID, F_M_NAME = 876, 4323, 4328

# depths.spread_size is in percent units: 0.15 = 15bps, 0.30 = 30bps
SPREAD_SIZE_TO_KEY = {0.15: 0.0015, 0.3: 0.0030}

INDIVIDUAL_COINS = ['BTC', 'ETH', 'SOL', 'XAU', 'XAG', 'WTI', 'XBR']
CAP = 300.0

# report line -> SQL-shaped column
COLS = {
    (0.0030, 'ask'): 'ask_0_003',
    (0.0030, 'bid'): 'bid_0_003',
    (0.0015, 'ask'): 'ask_0_0015',
    (0.0015, 'bid'): 'bid_0_0015',
}


def get_metabase_token():
    res = requests.post(
        f'{METABASE_URL}/api/session',
        json={'username': METABASE_USERNAME, 'password': METABASE_PASSWORD}
    )
    if res.status_code != 200:
        raise Exception(f'Metabase login failed: {res.status_code} {res.text}')
    return res.json()['id']


def run_mbql(token, query):
    res = requests.post(
        f'{METABASE_URL}/api/dataset',
        headers={'X-Metabase-Session': token, 'Content-Type': 'application/json'},
        json={'database': DATABASE_ID, 'type': 'query', 'query': query},
        timeout=600,
    )
    if res.status_code != 202:
        raise Exception(f'Query failed: {res.status_code} {res.text[:500]}')
    return res.json()['data']['rows']


def fetch_markets_map(token):
    rows = run_mbql(token, {
        'source-table': T_MARKETS,
        'fields': [['field', F_M_ID, None], ['field', F_M_NAME, None]],
    })
    return {int(r[0]): r[1] for r in rows}


def fetch_latest_depths_ts(token):
    rows = run_mbql(token, {
        'source-table': T_DEPTHS,
        'aggregation': [['max', ['field', F_D_TS, {'base-type': 'type/DateTime'}]]],
    })
    raw = rows[0][0]
    if raw is None:
        raise Exception('depths table returned no latest timestamp')
    return datetime.fromisoformat(raw.replace('Z', '+00:00'))


def fetch_exchange(token, period):
    """ranked / rn=1 / *0.6 — best exchange per market by bid_0030, then 60% target."""
    rows = run_mbql(token, {
        'source-table': T_EXCH,
        'fields': [['field', f, None] for f in
                   [F_MARKET_E, F_ASK_0015, F_ASK_0030, F_BID_0015, F_BID_0030]],
        'filter': ['and',
                   ['=', ['field', F_PERIOD, None], period],
                   ['=', ['field', F_EXCHANGE, None], 'BINANCE', 'HYPERLIQUID']],
    })
    best = {}
    for market, a15, a30, b15, b30 in rows:
        b30f = float(b30 or 0)
        if market not in best or b30f > best[market][0]:
            best[market] = (b30f, float(a15 or 0), float(a30 or 0), float(b15 or 0), float(b30 or 0))
    return {
        m: {'ask_0015': v[1] * 0.6, 'ask_0030': v[2] * 0.6,
            'bid_0015': v[3] * 0.6, 'bid_0030': v[4] * 0.6}
        for m, v in best.items()
    }


def fetch_partner(token, hours, latest_dt, markets_map):
    """partner_w — AVG depth per market NAME per spread, weighted-merged like GROUP BY m.name."""
    cutoff = (latest_dt - timedelta(hours=hours)).strftime('%Y-%m-%dT%H:%M:%S')
    print(f'Window: > {cutoff} (latest {latest_dt.strftime("%Y-%m-%dT%H:%M:%S")})')
    rows = run_mbql(token, {
        'source-table': T_DEPTHS,
        'aggregation': [
            ['avg', ['field', F_D_ASK, None]],
            ['avg', ['field', F_D_BID, None]],
            ['count'],
        ],
        'breakout': [['field', F_D_MKT, None], ['field', F_D_SPREAD, None]],
        'filter': ['and',
                   ['>', ['field', F_D_TS, {'base-type': 'type/DateTime'}], cutoff],
                   ['=', ['field', F_D_SPREAD, None], 0.15, 0.3]],
    })
    acc = {}  # (name, spread_key) -> [sum_ask, sum_bid, n]
    for mid, spread, avg_ask, avg_bid, n in rows:
        name = markets_map.get(int(mid))
        key = SPREAD_SIZE_TO_KEY.get(round(float(spread), 2))
        if not name or key is None or not n:
            continue
        a = acc.setdefault((name, key), [0.0, 0.0, 0])
        a[0] += float(avg_ask or 0) * n
        a[1] += float(avg_bid or 0) * n
        a[2] += n
    partner = {}
    for (name, key), (s_ask, s_bid, n) in acc.items():
        partner.setdefault(name, {})[key] = {'ask': s_ask / n, 'bid': s_bid / n}
    return partner


def build_rows(partner, ex):
    """The final SELECT: partner_w JOIN ex_w, COALESCE(ROUND(100*p/NULLIF(e,0),2),100)."""
    def pct(p_val, target):
        if target == 0 or p_val is None:
            return 100.0
        return round(100 * p_val / target, 2)

    rows = []
    for market in sorted(partner):
        if market not in ex:
            continue  # inner join
        p, e = partner[market], ex[market]
        p15, p30 = p.get(0.0015), p.get(0.0030)
        rows.append({
            'market': market,
            'ask_0_0015': pct(p15 and p15['ask'], e['ask_0015']),
            'bid_0_0015': pct(p15 and p15['bid'], e['bid_0015']),
            'ask_0_003':  pct(p30 and p30['ask'], e['ask_0030']),
            'bid_0_003':  pct(p30 and p30['bid'], e['bid_0030']),
        })
    return rows


def clean(name):
    return name.upper().replace('-', '').replace('/', '').replace('_', '')


def find_row(rows, coin):
    cands = [r for r in rows if clean(r['market']).startswith(coin.upper())]
    if not cands:
        return None
    return min(cands, key=lambda r: len(r['market']))  # ETH-USD beats ETHSPOT-USD


def is_individual(market):
    return any(clean(market).startswith(c) for c in INDIVIDUAL_COINS)


def fmt(v):
    if v is None:
        return '`n/a`'
    emoji = '✅' if v >= 100 else '❌'
    return f'{emoji} `{v:g}%`'


def coin_block(title, values):
    lines = [f'*{title}*']
    for spread, lbl in [(0.0030, '30bps'), (0.0015, '15bps')]:
        lines.append(f"  {lbl} ask: {fmt(values.get((spread, 'ask')))}  bid: {fmt(values.get((spread, 'bid')))}")
    return '\n'.join(lines)


def build_section(label, rows):
    lines = [f'*── {label} ──*\n']
    for coin in INDIVIDUAL_COINS:
        row = find_row(rows, coin)
        vals = {k: row[col] for k, col in COLS.items()} if row else {}
        lines.append(coin_block(coin, vals))
        lines.append('')
    agg = {k: [] for k in COLS}
    for r in rows:
        if is_individual(r['market']):
            continue
        for k, col in COLS.items():
            agg[k].append(min(r[col], CAP))
    other = {k: round(sum(v) / len(v), 1) if v else None for k, v in agg.items()}
    lines.append(coin_block('Other (avg, capped at 300%)', other))
    lines.append('')
    return lines


def send_telegram(message):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
    requests.post(url, json={'chat_id': CHAT_ID, 'text': message, 'parse_mode': 'Markdown'})


def main():
    token = get_metabase_token()
    markets_map = fetch_markets_map(token)
    latest = fetch_latest_depths_ts(token)

    lines = ['📊 *Daily Liquidity Report — Alber Blanc*',
             '_(numerator: total Extended market liquidity)_\n']

    for label, period, hours in [('1H', '1H', 1), ('12H', '12H', 12)]:
        ex = fetch_exchange(token, period)
        partner = fetch_partner(token, hours, latest, markets_map)
        rows = build_rows(partner, ex)
        print(f'{label}: {len(rows)} joined rows')
        lines += build_section(label, rows)

    lines.append('[Dashboards link](https://x10.metabaseapp.com/public/dashboard/9f5dc6ed-2492-4a8a-a06b-0a4129da7144?tab=232-1-hour)')
    send_telegram('\n'.join(lines))


if __name__ == '__main__':
    main()
