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

F_PARTNER   = 5219
F_MARKET_P  = 5222
F_SPREAD    = 5223
F_ASK_LIQ  = 5220
F_BID_LIQ  = 5224
F_TIMESTAMP = 5221

F_PERIOD    = 9439
F_EXCHANGE  = 9440
F_MARKET_E  = 9441
F_ASK_0001  = 9442
F_ASK_0003  = 9443
F_ASK_0015  = 9444
F_ASK_0030  = 9445
F_BID_0001  = 9446
F_BID_0003  = 9447
F_BID_0015  = 9448
F_BID_0030  = 9449

SPREAD_MAP = {
    0.0015: ('ask_0015', 'bid_0015'),
    0.0030: ('ask_0030', 'bid_0030'),
}

# Individual coins to show separately
INDIVIDUAL_COINS = ['BTC', 'ETH', 'SOL', 'XAU', 'XAG']


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


def mbql_query(token, table_id, filters, fields):
    payload = {
        'database': DATABASE_ID,
        'type': 'query',
        'query': {
            'source-table': table_id,
            'fields': [['field', f, None] for f in fields],
            'filter': filters
        }
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
        fields=[F_MARKET_E, F_EXCHANGE,
                F_ASK_0015, F_ASK_0030,
                F_BID_0015, F_BID_0030]
    )
    result = defaultdict(lambda: {
        'ask_0015': 0, 'ask_0030': 0,
        'bid_0015': 0, 'bid_0030': 0
    })
    for r in rows:
        m = r['market']
        result[m]['ask_0015'] = max(result[m]['ask_0015'], float(r['ask_avg_liquidity_0_0015'] or 0))
        result[m]['ask_0030'] = max(result[m]['ask_0030'], float(r['ask_avg_liquidity_0_0030'] or 0))
        result[m]['bid_0015'] = max(result[m]['bid_0015'], float(r['bid_avg_liquidity_0_0015'] or 0))
        result[m]['bid_0030'] = max(result[m]['bid_0030'], float(r['bid_avg_liquidity_0_0030'] or 0))

    # Apply 0.6 — this is the actual target
    for m in result:
        for k in result[m]:
            result[m][k] *= 0.6

    return result


def fetch_partner_data(token, hours):
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime('%Y-%m-%dT%H:%M:%S')
    rows = mbql_query(
        token,
        table_id=925,
        filters=['and',
            ['=', ['field', F_PARTNER, None], 'Albert Blanc'],
            ['>', ['field', F_TIMESTAMP, {'base-type': 'type/DateTime'}], cutoff]
        ],
        fields=[F_MARKET_P, F_SPREAD, F_ASK_LIQ, F_BID_LIQ]
    )
    sums = defaultdict(lambda: defaultdict(lambda: {'ask_sum': 0, 'bid_sum': 0, 'count': 0}))
    for r in rows:
        m = r['market']
        s = float(r['spread'])
        sums[m][s]['ask_sum'] += float(r['ask_liquidity'] or 0)
        sums[m][s]['bid_sum'] += float(r['bid_liquidity'] or 0)
        sums[m][s]['count'] += 1
    result = {}
    for m, spreads in sums.items():
        result[m] = {}
        for s, v in spreads.items():
            n = v['count'] or 1
            result[m][s] = {'ask': v['ask_sum'] / n, 'bid': v['bid_sum'] / n}
    return result


def compute_pct(p_val, ex_val):
    """% of target (100% = meeting the 60% threshold)"""
    if ex_val == 0:
        return 100.0
    return round(100 * p_val / ex_val, 1)


def fmt(pct):
    emoji = '✅' if pct >= 100 else '❌'
    return f'{emoji} `{pct}%`'


def compute_individual(partner_data, exchange_data, coin):
    """Returns {(spread, side): pct} for a specific coin"""
    matched_key = None
    for m in exchange_data.keys():
        clean = m.upper().replace('-', '').replace('/', '').replace('_', '')
        if clean.startswith(coin.upper()):
            matched_key = m
            break

    result = {}
    for spread in [0.0030, 0.0015]:
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
    """Average % across all 'other' markets for each spread/side"""
    sums = {
        (0.0030, 'ask'): [], (0.0030, 'bid'): [],
        (0.0015, 'ask'): [], (0.0015, 'bid'): [],
    }
    for market, ex in exchange_data.items():
        if get_group(market) != 'other':
            continue
        for spread in [0.0030, 0.0015]:
            ask_key, bid_key = SPREAD_MAP[spread]
            p_vals = partner_data.get(market, {}).get(spread, None)
            ex_ask = ex[ask_key]
            ex_bid = ex[bid_key]
            if p_vals is None:
                sums[(spread, 'ask')].append(0.0)
                sums[(spread, 'bid')].append(0.0)
            else:
                sums[(spread, 'ask')].append(compute_pct(p_vals['ask'], ex_ask))
                sums[(spread, 'bid')].append(compute_pct(p_vals['bid'], ex_bid))

    result = {}
    for key, vals in sums.items():
        result[key] = round(sum(vals) / len(vals), 1) if vals else 0.0
    return result


def format_coin_block(name, data):
    lines = [f'*{name}*']
    for spread, label in [(0.0030, '30bps'), (0.0015, '15bps')]:
        ask = data.get((spread, 'ask'), 0.0)
        bid = data.get((spread, 'bid'), 0.0)
        lines.append(f'  {label} ask: {fmt(ask)}  bid: {fmt(bid)}')
    return '\n'.join(lines)


def main():
    token = get_metabase_token()

    lines = ['📊 *Daily Liquidity Report — Albert Blanc*\n']

    for label, period, hours in [('1H', '1H', 1), ('12H', '12H', 12)]:
        ex_data = fetch_exchange_data(token, period)
        p_data = fetch_partner_data(token, hours)

        lines.append(f'*── {label} ──*\n')

        # Individual coins
        for coin in INDIVIDUAL_COINS:
            ind = compute_individual(p_data, ex_data, coin)
            lines.append(format_coin_block(coin, ind))
            lines.append('')

        # Other (averaged)
        other = compute_other_avgs(p_data, ex_data)
        lines.append(format_coin_block('Other (avg)', other))
        lines.append('')

    send_telegram('\n'.join(lines))


def send_telegram(message):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
    requests.post(url, json={'chat_id': CHAT_ID, 'text': message, 'parse_mode': 'Markdown'})


if __name__ == '__main__':
    main()
