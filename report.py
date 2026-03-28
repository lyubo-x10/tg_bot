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

BTC_ETH_MARKETS = {'BTC', 'ETH', 'BTCUSDT', 'ETHUSDT', 'BTC-USD', 'ETH-USD',
                   'BTC/USD', 'ETH/USD', 'BTCUSD', 'ETHUSD'}

SPREAD_MAP = {
    0.0001: ('ask_0001', 'bid_0001'),
    0.0003: ('ask_0003', 'bid_0003'),
    0.0015: ('ask_0015', 'bid_0015'),
    0.0030: ('ask_0030', 'bid_0030'),
}


def is_btc_eth(market):
    m = market.upper()
    return any(x in m for x in ['BTC', 'ETH'])


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
                F_ASK_0001, F_ASK_0003, F_ASK_0015, F_ASK_0030,
                F_BID_0001, F_BID_0003, F_BID_0015, F_BID_0030]
    )
    result = defaultdict(lambda: {
        'ask_0001': 0, 'ask_0003': 0, 'ask_0015': 0, 'ask_0030': 0,
        'bid_0001': 0, 'bid_0003': 0, 'bid_0015': 0, 'bid_0030': 0
    })
    for r in rows:
        m = r['market']
        result[m]['ask_0001'] = max(result[m]['ask_0001'], float(r['ask_avg_liquidity_0_0001'] or 0))
        result[m]['ask_0003'] = max(result[m]['ask_0003'], float(r['ask_avg_liquidity_0_0003'] or 0))
        result[m]['ask_0015'] = max(result[m]['ask_0015'], float(r['ask_avg_liquidity_0_0015'] or 0))
        result[m]['ask_0030'] = max(result[m]['ask_0030'], float(r['ask_avg_liquidity_0_0030'] or 0))
        result[m]['bid_0001'] = max(result[m]['bid_0001'], float(r['bid_avg_liquidity_0_0001'] or 0))
        result[m]['bid_0003'] = max(result[m]['bid_0003'], float(r['bid_avg_liquidity_0_0003'] or 0))
        result[m]['bid_0015'] = max(result[m]['bid_0015'], float(r['bid_avg_liquidity_0_0015'] or 0))
        result[m]['bid_0030'] = max(result[m]['bid_0030'], float(r['bid_avg_liquidity_0_0030'] or 0))
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


def compute_breakdown(partner_data, exchange_data):
    # Groups: (is_btc_eth, spread) -> (passing, total)
    stats = {
        ('btceth', 0.0030): [0, 0],
        ('btceth', 0.0015): [0, 0],
        ('other',  0.0030): [0, 0],
        ('other',  0.0015): [0, 0],
    }
    for market, spreads in partner_data.items():
        if market not in exchange_data:
            continue
        ex = exchange_data[market]
        group = 'btceth' if is_btc_eth(market) else 'other'
        for spread in [0.0015, 0.0030]:
            if spread not in spreads:
                continue
            ask_key, bid_key = SPREAD_MAP[spread]
            ex_ask = ex[ask_key]
            ex_bid = ex[bid_key]
            vals = spreads[spread]
            ask_pct = 100 if ex_ask == 0 else 100 * vals['ask'] / ex_ask
            bid_pct = 100 if ex_bid == 0 else 100 * vals['bid'] / ex_bid
            key = (group, spread)
            stats[key][0] += (1 if ask_pct >= 60 else 0) + (1 if bid_pct >= 60 else 0)
            stats[key][1] += 2
    return stats


def format_stat(stats, group, spread):
    passing, total = stats[(group, spread)]
    if total == 0:
        return 'N/A'
    rate = round(100 * passing / total, 1)
    emoji = '✅' if rate >= 80 else '❌'
    return f'{emoji} `{rate}%` ({passing}/{total})'


def send_telegram(message):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
    requests.post(url, json={'chat_id': CHAT_ID, 'text': message, 'parse_mode': 'Markdown'})


def main():
    token = get_metabase_token()

    lines = ['📊 *Daily Liquidity Report — Albert Blanc*\n']

    for label, period, hours in [('1H', '1H', 1), ('12H', '12H', 12)]:
        ex_data = fetch_exchange_data(token, period)
        p_data = fetch_partner_data(token, hours)
        stats = compute_breakdown(p_data, ex_data)

        lines.append(f'*── {label} ──*')
        lines.append(f'ETH/BTC 30bps: {format_stat(stats, "btceth", 0.0030)}')
        lines.append(f'ETH/BTC 15bps: {format_stat(stats, "btceth", 0.0015)}')
        lines.append(f'Other   30bps: {format_stat(stats, "other",  0.0030)}')
        lines.append(f'Other   15bps: {format_stat(stats, "other",  0.0015)}\n')

    send_telegram('\n'.join(lines))


if __name__ == '__main__':
    main()
