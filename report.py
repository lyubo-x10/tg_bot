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

# exchange_liquidity_stats (table 2575, schema stats)
F_PERIOD    = 9439
F_EXCHANGE  = 9440
F_MARKET_E  = 9441
F_ASK_0015  = 9444
F_ASK_0030  = 9445
F_BID_0015  = 9448
F_BID_0030  = 9449

# depths (table 874) — total Extended orderbook liquidity
T_DEPTHS    = 874
F_D_TS      = 4411
F_D_SPREAD  = 4407
F_D_ASK     = 4408
F_D_BID     = 4412
F_D_MKT     = 4410

# assets.markets (table 876) — market_id -> market name
T_MARKETS   = 876
F_M_ID      = 4323
F_M_NAME    = 4328

SPREAD_MAP = {
    0.0015: ('ask_0015', 'bid_0015'),
    0.0030: ('ask_0030', 'bid_0030'),
}

# depths.spread_size is in percent units: 0.15 = 15bps, 0.30 = 30bps
SPREAD_SIZE_TO_KEY = {0.15: 0.0015, 0.3: 0.0030}

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


def mbql_query(token, table_id, filters, fields=None):
    query = {'source-table': table_id}
    if filters:
        query['filter'] = filters
    if fields:
        query['fields'] = [['field', f, None] for f in fields]
    payload = {
        'database': DATABASE_ID,
        'type': 'query',
        'query': query
    }
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
    rows = mbql_query(
        token,
        table_id=2575,
        filters=['and',
            ['=', ['field', F_PERIOD, None], period],
            ['=', ['field', F_EXCHANGE, None], 'BINANCE', 'HYPERLIQUID']
        ],
        fields=[F_MARKET_E, F_EXCHANGE, F_ASK_0015, F_ASK_0030, F_BID_0015, F_BID_0030]
    )

    # Pick the single best exchange per market (highest bid_0030),
    # mirrors the SQL: ROW_NUMBER() ORDER BY bid_avg_liquidity_0_0030 DESC
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

    # Apply 0.6 — this is the actual target
    result = {}
    for m, v in best.items():
        result[m] = {
            'ask_0015': v['ask_0015'] * 0.6,
            'ask_0030': v['ask_0030'] * 0.6,
            'bid_0015': v['bid_0015'] * 0.6,
            'bid_0030': v['bid_0030'] * 0.6,
        }
    return result


def fetch_markets_map(token):
    """market_id -> market name from assets.markets"""
    rows = mbql_query(token, table_id=T_MARKETS, filters=None, fields=[F_M_ID, F_M_NAME])
    return {int(r['id']): r['name'] for r in rows}


def fetch_total_liquidity(token, hours, markets_map):
    """Total Extended liquidity from depths, averaged over the window, keyed by market name."""
    # Anchor to the latest timestamp in depths
    res = requests.post(
        f'{METABASE_URL}/api/dataset',
        headers={'X-Metabase-Session': token, 'Content-Type': 'application/json'},
        json={
            'database': DATABASE_ID,
            'type': 'query',
            'query': {
                'source-table': T_DEPTHS,
                'aggregation': [['max', ['field', F_D_TS, {'base-type': 'type/DateTime'}]]]
            }
        }
    )
    latest_raw = res.json()['data']['rows'][0][0]
    if latest_raw is None:
        raise Exception('depths table returned no latest timestamp')
    latest_dt = datetime.fromisoformat(latest_raw.replace('Z', '+00:00'))
    cutoff = (latest_dt - timedelta(hours=hours)).strftime('%Y-%m-%dT%H:%M:%S')
    latest_str = latest_dt.strftime('%Y-%m-%dT%H:%M:%S')
    print(f'Depths latest: {latest_str}, cutoff: {cutoff}')

    payload = {
        'database': DATABASE_ID,
        'type': 'query',
        'query': {
            'source-table': T_DEPTHS,
            'aggregation': [
                ['avg', ['field', F_D_ASK, None]],
                ['avg', ['field', F_D_BID, None]]
            ],
            'breakout': [
                ['field', F_D_MKT, None],
                ['field', F_D_SPREAD, None]
            ],
            'filter': ['and',
                ['>', ['field', F_D_TS, {'base-type': 'type/DateTime'}], cutoff],
                ['<=', ['field', F_D_TS, {'base-type': 'type/DateTime'}], latest_str],
                ['=', ['field', F_D_SPREAD, None], 0.15, 0.3]
            ]
        }
    }
    res = requests.post(
        f'{METABASE_URL}/api/dataset',
        headers={'X-Metabase-Session': token, 'Content-Type': 'application/json'},
        json=payload
    )
    if res.status_code != 202:
        raise Exception(f'Depths query failed: {res.status_code} {res.text[:500]}')
    rows = res.json()['data']['rows']
    print(f'Depths aggregated rows: {len(rows)}')

    result = {}
    for market_id, spread_size, avg_ask, avg_bid in rows:
        name = markets_map.get(int(market_id))
        if not name:
            continue
        key = SPREAD_SIZE_TO_KEY.get(round(float(spread_size), 2))
        if key is None:
            continue
        result.setdefault(name, {})[key] = {
            'ask': float(avg_ask or 0),
            'bid': float(avg_bid or 0),
        }
    return result


def compute_pct(p_val, ex_val):
    if ex_val == 0:
        return 100.0
    return round(100 * p_val / ex_val, 1)


def cap_pct(pct):
    return min(pct, CAP)


def fmt(pct):
    if pct is None:
        return '`n/a`'
    emoji = '✅' if pct >= 100 else '❌'
    return f'{emoji} `{pct}%`'


def compute_individual(total_data, exchange_data, coin):
    # Shortest match wins: ETH-USD beats ETHSPOT-USD
    cu = coin.upper()
    candidates = [m for m in exchange_data.keys()
                  if m.upper().replace('-', '').replace('/', '').replace('_', '').startswith(cu)]
    matched_key = min(candidates, key=len) if candidates else None

    result = {}
    for spread in [0.0030, 0.0015]:
        ask_key, bid_key = SPREAD_MAP[spread]
        if not matched_key or matched_key not in total_data:
            # No depths data in the window -> market wouldn't appear on the dashboard either
            result[(spread, 'ask')] = None
            result[(spread, 'bid')] = None
            continue
        ex = exchange_data[matched_key]
        p_vals = total_data[matched_key].get(spread)
        if p_vals is None:
            # mirrors the dashboard's COALESCE(..., 100)
            result[(spread, 'ask')] = 100.0
            result[(spread, 'bid')] = 100.0
        else:
            result[(spread, 'ask')] = compute_pct(p_vals['ask'], ex[ask_key])
            result[(spread, 'bid')] = compute_pct(p_vals['bid'], ex[bid_key])
    return result


def compute_other_avgs(total_data, exchange_data):
    sums = {
        (0.0030, 'ask'): [], (0.0030, 'bid'): [],
        (0.0015, 'ask'): [], (0.0015, 'bid'): [],
    }
    for market, ex in exchange_data.items():
        if get_group(market) != 'other':
            continue
        if market not in total_data:
            continue  # mirrors the dashboard's inner join
        for spread in [0.0030, 0.0015]:
            ask_key, bid_key = SPREAD_MAP[spread]
            p_vals = total_data[market].get(spread)
            if p_vals is None:
                ask_pct = bid_pct = 100.0  # mirrors COALESCE(..., 100)
            else:
                ask_pct = compute_pct(p_vals['ask'], ex[ask_key])
                bid_pct = compute_pct(p_vals['bid'], ex[bid_key])
            sums[(spread, 'ask')].append(cap_pct(ask_pct))
            sums[(spread, 'bid')].append(cap_pct(bid_pct))

    result = {}
    for key, vals in sums.items():
        result[key] = round(sum(vals) / len(vals), 1) if vals else 0.0
    return result


def format_coin_block(name, data):
    lines = [f'*{name}*']
    for spread, label in [(0.0030, '30bps'), (0.0015, '15bps')]:
        ask = data.get((spread, 'ask'))
        bid = data.get((spread, 'bid'))
        lines.append(f'  {label} ask: {fmt(ask)}  bid: {fmt(bid)}')
    return '\n'.join(lines)


def send_telegram(message):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
    requests.post(url, json={'chat_id': CHAT_ID, 'text': message, 'parse_mode': 'Markdown'})


def main():
    token = get_metabase_token()
    markets_map = fetch_markets_map(token)

    lines = ['📊 *Daily Liquidity Report — Alber Blanc*',
             '_(numerator: total Extended market liquidity)_\n']

    for label, period, hours in [('1H', '1H', 1), ('12H', '12H', 12)]:
        ex_data = fetch_exchange_data(token, period)
        total_data = fetch_total_liquidity(token, hours, markets_map)

        # DEBUG — remove once ETH is confirmed fixed
        print(f"{label} ETH-ish benchmark markets: {[m for m in ex_data if 'ETH' in m.upper()]}")
        print(f"{label} ETH-USD in depths totals: {'ETH-USD' in total_data}")

        lines.append(f'*── {label} ──*\n')

        for coin in INDIVIDUAL_COINS:
            ind = compute_individual(total_data, ex_data, coin)
            lines.append(format_coin_block(coin, ind))
            lines.append('')

        other = compute_other_avgs(total_data, ex_data)
        lines.append(format_coin_block('Other (avg, capped at 300%)', other))
        lines.append('')

    lines.append('[Dashboards link](https://x10.metabaseapp.com/public/dashboard/9f5dc6ed-2492-4a8a-a06b-0a4129da7144?tab=232-1-hour)')
    send_telegram('\n'.join(lines))


if __name__ == '__main__':
    main()
