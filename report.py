import os
import requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

METABASE_URL = 'https://x10.metabaseapp.com'
METABASE_USERNAME = os.environ['METABASE_USERNAME']
METABASE_PASSWORD = os.environ['METABASE_PASSWORD']
BOT_TOKEN = os.environ['TELEGRAM_BOT_TOKEN']
CHAT_ID = os.environ['TELEGRAM_CHAT_ID']

DATABASE_ID = 100

# depths (table 874)
T_DEPTHS    = 874
F_D_MARKET_ID = 4410
F_D_SPREAD    = 4407
F_D_TIMESTAMP = 4411
F_D_ASK_DEPTH = 4408
F_D_BID_DEPTH = 4412

# assets.markets (table 876)
T_MARKETS   = 876
F_M_ID      = 4323
F_M_NAME    = 4328

# exchange_liquidity_stats (table 2575)
F_PERIOD    = 9439
F_EXCHANGE  = 9440
F_MARKET_E  = 9441
F_ASK_0015  = 9444
F_ASK_0030  = 9445
F_BID_0015  = 9448
F_BID_0030  = 9449

# depths.spread_size is stored as percent units (0.15 = 15bps, 0.30 = 30bps)
SPREAD_MAP = {
    0.15: ('ask_0015', 'bid_0015'),
    0.30: ('ask_0030', 'bid_0030'),
}

INDIVIDUAL_COINS = ['BTC', 'ETH', 'SOL', 'XAU', 'XAG', 'WTI', 'XBR']
CAP = 300.0


def get_group(market):
    m = market.upper().replace('-', '').replace('/', '').replace('_', '')
    for coin in INDIVIDUAL_COINS:
        if m.startswith(coin):
            return coin
    return 'other'


def get_metabase_token():
    res = requests.post(
        f'{METABASE_URL}/api/session',
        json={'username': METABASE_USERNAME, 'password': METABASE_PASSWORD}
    )
    if res.status_code != 200:
        raise Exception(f'Metabase login failed: {res.status_code} {res.text}')
    return res.json()['id']


def run_dataset(token, payload):
    res = requests.post(
        f'{METABASE_URL}/api/dataset',
        headers={'X-Metabase-Session': token, 'Content-Type': 'application/json'},
        json=payload
    )
    if res.status_code != 202:
        raise Exception(f'Query failed: {res.status_code} {res.text[:500]}')
    data = res.json()
    rows = data['data']['rows']
    cols = [c['name'] for c in data['data']['cols']]
    return [dict(zip(cols, row)) for row in rows]


def fetch_exchange_data(token, period):
    payload = {
        'database': DATABASE_ID,
        'type': 'query',
        'query': {
            'source-table': 2575,
            'filter': ['and',
                ['=', ['field', F_PERIOD, None], period],
                ['=', ['field', F_EXCHANGE, None], 'BINANCE', 'HYPERLIQUID']
            ],
            'fields': [
                ['field', F_MARKET_E, None],
                ['field', F_EXCHANGE, None],
                ['field', F_ASK_0015, None],
                ['field', F_ASK_0030, None],
                ['field', F_BID_0015, None],
                ['field', F_BID_0030, None],
            ]
        }
    }
    rows = run_dataset(token, payload)

    # Per-market pick the exchange with the highest bid_0030
    # (mirrors SQL: ROW_NUMBER() OVER (PARTITION BY market ORDER BY bid_avg_liquidity_0_0030 DESC))
    best = {}
    for r in rows:
        m = r['market']
        bid_0030 = float(r['bid_avg_liquidity_0_0030'] or 0)
        if m not in best or bid_0030 > best[m]['bid_0030_raw']:
            best[m] = {
                'bid_0030_raw': bid_0030,
                'ask_0015': float(r['ask_avg_liquidity_0_0015'] or 0),
                'ask_0030': float(r['ask_avg_liquidity_0_0030'] or 0),
                'bid_0015': float(r['bid_avg_liquidity_0_0015'] or 0),
                'bid_0030': float(r['bid_avg_liquidity_0_0030'] or 0),
            }

    # Apply the 0.6 target multiplier
    result = {}
    for m, v in best.items():
        result[m] = {
            'ask_0015': v['ask_0015'] * 0.6,
            'ask_0030': v['ask_0030'] * 0.6,
            'bid_0015': v['bid_0015'] * 0.6,
            'bid_0030': v['bid_0030'] * 0.6,
        }
    return result


def fetch_partner_data(token, hours):
    """Fetch TOTAL Extended liquidity from `depths` joined with `assets.markets`.
    Replaces the old AB-only `partner_liquidity_stats` query per AB's correction.
    """
    cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=hours)
    cutoff = cutoff_dt.strftime('%Y-%m-%dT%H:%M:%S')
    print(f'Depths cutoff: {cutoff}')

    payload = {
        'database': DATABASE_ID,
        'type': 'query',
        'query': {
            'source-table': T_DEPTHS,
            'filter': ['>=',
                ['field', F_D_TIMESTAMP, {'base-type': 'type/DateTime'}],
                cutoff
            ],
            'joins': [{
                'source-table': T_MARKETS,
                'alias': 'm',
                'condition': ['=',
                    ['field', F_D_MARKET_ID, None],
                    ['field', F_M_ID, {'join-alias': 'm'}]
                ],
                'fields': [['field', F_M_NAME, {'join-alias': 'm'}]]
            }],
            'fields': [
                ['field', F_D_SPREAD, None],
                ['field', F_D_ASK_DEPTH, None],
                ['field', F_D_BID_DEPTH, None],
            ]
        }
    }
    rows = run_dataset(token, payload)
    print(f'Depths rows returned: {len(rows)}')

    sums = defaultdict(lambda: defaultdict(lambda: {'ask_sum': 0, 'bid_sum': 0, 'count': 0}))
    for r in rows:
        m = r.get('name') or r.get('m__name') or r.get('markets__name')
        if m is None:
            continue
        s = float(r['spread_size'])
        sums[m][s]['ask_sum'] += float(r['ask_quote_depth'] or 0)
        sums[m][s]['bid_sum'] += float(r['bid_quote_depth'] or 0)
        sums[m][s]['count'] += 1

    result = {}
    for m, spreads in sums.items():
        result[m] = {}
        for s, v in spreads.items():
            n = v['count'] or 1
            result[m][s] = {'ask': v['ask_sum'] / n, 'bid': v['bid_sum'] / n}
    return result


def compute_pct(p_val, ex_val):
    if ex_val == 0:
        return 100.0
    return round(100 * p_val / ex_val, 1)


def cap_pct(pct):
    return min(pct, CAP)


def fmt(pct):
    emoji = '✅' if pct >= 100 else '❌'
    return f'{emoji} `{pct}%`'


def compute_individual(partner_data, exchange_data, coin):
    matched_key = None
    for m in exchange_data.keys():
        clean = m.upper().replace('-', '').replace('/', '').replace('_', '')
        if clean.startswith(coin.upper()):
            matched_key = m
            break

    result = {}
    for spread in [0.30, 0.15]:
        ask_key, bid_key = SPREAD_MAP[spread]
        ex = exchange_data.get(matched_key, {}) if matched_key else {}
        p_vals = partner_data.get(matched_key, {}).get(spread, None) if matched_key else None
        ex_ask = ex.get(ask_key, 0)
        ex_bid = ex.get(bid_key, 0)
        if p_vals is None:
            result[(spread, 'ask')] = 0.0
            result[(spread, 'bid')] = 0.0
        else:
            result[(spread, 'ask')] = compute_pct(p_vals['ask'], ex_ask)
            result[(spread, 'bid')] = compute_pct(p_vals['bid'], ex_bid)
    return result


def compute_other_avgs(partner_data, exchange_data):
    sums = {
        (0.30, 'ask'): [], (0.30, 'bid'): [],
        (0.15, 'ask'): [], (0.15, 'bid'): [],
    }
    for market, ex in exchange_data.items():
        if get_group(market) != 'other':
            continue
        for spread in [0.30, 0.15]:
            ask_key, bid_key = SPREAD_MAP[spread]
            p_vals = partner_data.get(market, {}).get(spread, None)
            ex_ask = ex[ask_key]
            ex_bid = ex[bid_key]
            if p_vals is None:
                sums[(spread, 'ask')].append(0.0)
                sums[(spread, 'bid')].append(0.0)
            else:
                sums[(spread, 'ask')].append(cap_pct(compute_pct(p_vals['ask'], ex_ask)))
                sums[(spread, 'bid')].append(cap_pct(compute_pct(p_vals['bid'], ex_bid)))

    result = {}
    for key, vals in sums.items():
        result[key] = round(sum(vals) / len(vals), 1) if vals else 0.0
    return result


def format_coin_block(name, data):
    lines = [f'*{name}*']
    for spread, label in [(0.30, '30bps'), (0.15, '15bps')]:
        ask = data.get((spread, 'ask'), 0.0)
        bid = data.get((spread, 'bid'), 0.0)
        lines.append(f'  {label} ask: {fmt(ask)}  bid: {fmt(bid)}')
    return '\n'.join(lines)


def send_telegram(message):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
    requests.post(url, json={'chat_id': CHAT_ID, 'text': message, 'parse_mode': 'Markdown'})


def main():
    token = get_metabase_token()

    lines = ['📊 *Daily Liquidity Report — Alber Blanc*\n']

    for label, period, hours in [('1H', '1H', 1), ('12H', '12H', 12)]:
        ex_data = fetch_exchange_data(token, period)
        p_data = fetch_partner_data(token, hours)

        lines.append(f'*── {label} ──*\n')

        for coin in INDIVIDUAL_COINS:
            ind = compute_individual(p_data, ex_data, coin)
            lines.append(format_coin_block(coin, ind))
            lines.append('')

        other = compute_other_avgs(p_data, ex_data)
        lines.append(format_coin_block('Other (avg, capped at 300%)', other))
        lines.append('')

    lines.append('[Dashboards link](https://x10.metabaseapp.com/public/dashboard/9f5dc6ed-2492-4a8a-a06b-0a4129da7144?tab=232-1-hour)')
    send_telegram('\n'.join(lines))


if __name__ == '__main__':
    main()
