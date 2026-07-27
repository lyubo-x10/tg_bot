import os
import requests

METABASE_URL = 'https://x10.metabaseapp.com'
METABASE_USERNAME = os.environ['METABASE_USERNAME']
METABASE_PASSWORD = os.environ['METABASE_PASSWORD']
BOT_TOKEN = os.environ['TELEGRAM_BOT_TOKEN']
CHAT_ID = os.environ['TELEGRAM_CHAT_ID']

DATABASE_ID = 100

MODE = 'card'          # 'card' = run the saved questions; 'native' = post the SQL below
CARD_ID_1H  = 2113        # <- question id from its URL, e.g. /question/2201-... -> 2201
CARD_ID_12H = 2114        # <- same for the 12h question

INDIVIDUAL_COINS = ['BTC', 'ETH', 'SOL', 'XAU', 'XAG', 'WTI', 'XBR']
CAP = 300.0

COLS = {
    (0.0030, 'ask'): 'ask_0_003',
    (0.0030, 'bid'): 'bid_0_003',
    (0.0015, 'ask'): 'ask_0_0015',
    (0.0015, 'bid'): 'bid_0_0015',
}

SQL_TEMPLATE = """
WITH ranked AS (
    SELECT
        market,
        exchange_name,
        ask_avg_liquidity_0_0001,
        ask_avg_liquidity_0_0003,
        ask_avg_liquidity_0_0015,
        ask_avg_liquidity_0_0030,
        bid_avg_liquidity_0_0001,
        bid_avg_liquidity_0_0003,
        bid_avg_liquidity_0_0015,
        bid_avg_liquidity_0_0030,
        ROW_NUMBER() OVER (
            PARTITION BY market
            ORDER BY bid_avg_liquidity_0_0030 DESC NULLS LAST
        ) AS rn
    FROM stats.exchange_liquidity_stats
    WHERE period = '{period}'
      AND exchange_name IN ('BINANCE','HYPERLIQUID')
),
ex_w AS (
    SELECT
        market,
        0.6 * ask_avg_liquidity_0_0015 AS ask_sum_0_0015,
        0.6 * ask_avg_liquidity_0_0030 AS ask_sum_0_0030,
        0.6 * bid_avg_liquidity_0_0015 AS bid_sum_0_0015,
        0.6 * bid_avg_liquidity_0_0030 AS bid_sum_0_0030
    FROM ranked
    WHERE rn = 1
),
partner_w AS (
  SELECT
    m.name as market,
    AVG(ask_quote_depth) FILTER (WHERE spread_size = 0.15) AS p_ask_0_0015,
    AVG(ask_quote_depth) FILTER (WHERE spread_size = 0.30) AS p_ask_0_0030,
    AVG(bid_quote_depth) FILTER (WHERE spread_size = 0.15) AS p_bid_0_0015,
    AVG(bid_quote_depth) FILTER (WHERE spread_size = 0.30) AS p_bid_0_0030
  FROM depths,
    assets.markets m
  WHERE "timestamp" >= now() - interval '{window}'
  AND  m.id = market_id
  GROUP BY m.name
)
SELECT
  p.market AS market,
  COALESCE(ROUND(100 * p.p_ask_0_0015 / NULLIF(e.ask_sum_0_0015, 0), 2), 100) AS ask_0_0015,
  COALESCE(ROUND(100 * p.p_bid_0_0015 / NULLIF(e.bid_sum_0_0015, 0), 2), 100) AS bid_0_0015,
  COALESCE(ROUND(100 * p.p_ask_0_0030 / NULLIF(e.ask_sum_0_0030, 0), 2), 100) AS ask_0_003,
  COALESCE(ROUND(100 * p.p_bid_0_0030 / NULLIF(e.bid_sum_0_0030, 0), 2), 100) AS bid_0_003
FROM partner_w p
JOIN ex_w e
  ON e.market = p.market
ORDER BY market
"""

SECTIONS = [
    ('1H',  CARD_ID_1H,  SQL_TEMPLATE.format(period='1H',  window='1 hour')),
    ('12H', CARD_ID_12H, SQL_TEMPLATE.format(period='12H', window='12 hour')),
]


def get_metabase_token():
    res = requests.post(
        f'{METABASE_URL}/api/session',
        json={'username': METABASE_USERNAME, 'password': METABASE_PASSWORD}
    )
    if res.status_code != 200:
        raise Exception(f'Metabase login failed: {res.status_code} {res.text}')
    return res.json()['id']


def norm_key(k):
    return str(k).lower().replace(' ', '_')


def fetch_rows_card(token, card_id):
    r = requests.post(
        f'{METABASE_URL}/api/card/{card_id}/query/json',
        headers={'X-Metabase-Session': token, 'Content-Type': 'application/json'},
        json={'parameters': []},
        timeout=600,
    )
    if r.status_code != 200:
        raise Exception(
            f'Card {card_id} failed: {r.status_code} {r.text[:300]} '
            f"(403 = mb_exporter can't see this question's collection -> ask Dima for View "
            f"access on it, or get native rights and set MODE='native')"
        )
    return [{norm_key(k): v for k, v in row.items()} for row in r.json()]


def fetch_rows_native(token, sql):
    r = requests.post(
        f'{METABASE_URL}/api/dataset',
        headers={'X-Metabase-Session': token, 'Content-Type': 'application/json'},
        json={'database': DATABASE_ID, 'type': 'native', 'native': {'query': sql}},
        timeout=600,
    )
    if r.status_code != 202:
        raise Exception(
            f'Native query failed: {r.status_code} {r.text[:300]} '
            f'(permission denied = mb_exporter needs native query rights on the DB)'
        )
    body = r.json()
    if body.get('error'):
        raise Exception(f'SQL error: {str(body["error"])[:300]}')
    data = body['data']
    cols = [norm_key(c['name']) for c in data['cols']]
    return [dict(zip(cols, row)) for row in data['rows']]


def val(row, key):
    v = row.get(key)
    if v is None or v == '':
        return 100.0  # matches the SQL's COALESCE(..., 100)
    return float(v)


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
        vals = {k: val(row, col) for k, col in COLS.items()} if row else {}
        lines.append(coin_block(coin, vals))
        lines.append('')
    agg = {k: [] for k in COLS}
    for r in rows:
        if is_individual(r['market']):
            continue
        for k, col in COLS.items():
            agg[k].append(min(val(r, col), CAP))
    other = {k: round(sum(v) / len(v), 1) if v else None for k, v in agg.items()}
    lines.append(coin_block('Other (avg, capped at 300%)', other))
    lines.append('')
    return lines


def send_telegram(message):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
    requests.post(url, json={'chat_id': CHAT_ID, 'text': message, 'parse_mode': 'Markdown'})


def main():
    token = get_metabase_token()
    lines = ['📊 *Daily Liquidity Report — Alber Blanc*',
             '_(numerator: total Extended market liquidity)_\n']
    for label, card_id, sql in SECTIONS:
        rows = fetch_rows_card(token, card_id) if MODE == 'card' else fetch_rows_native(token, sql)
        print(f'{label}: {len(rows)} rows')
        lines += build_section(label, rows)
    lines.append('[Dashboards link](https://x10.metabaseapp.com/public/dashboard/9f5dc6ed-2492-4a8a-a06b-0a4129da7144?tab=232-1-hour)')
    send_telegram('\n'.join(lines))


if __name__ == '__main__':
    main()
