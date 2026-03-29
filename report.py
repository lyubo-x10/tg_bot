import os
import requests
from collections import defaultdict

METABASE_URL = 'https://x10.metabaseapp.com'
METABASE_USERNAME = os.environ['METABASE_USERNAME']
METABASE_PASSWORD = os.environ['METABASE_PASSWORD']
BOT_TOKEN = os.environ['TELEGRAM_BOT_TOKEN']
CHAT_ID = os.environ['TELEGRAM_CHAT_ID']

QUESTION_1H  = 2113
QUESTION_12H = 2114

INDIVIDUAL_COINS = ['BTC', 'ETH', 'SOL', 'XAU', 'XAG']
CAP = 300.0


def get_metabase_token():
    res = requests.post(
        f'{METABASE_URL}/api/session',
        json={'username': METABASE_USERNAME, 'password': METABASE_PASSWORD}
    )
    if res.status_code != 200:
        raise Exception(f'Metabase login failed: {res.status_code} {res.text}')
    return res.json()['id']


def fetch_question(token, question_id):
    """Run a saved Metabase question and return rows as list of dicts"""
    res = requests.post(
        f'{METABASE_URL}/api/card/{question_id}/query',
        headers={'X-Metabase-Session': token, 'Content-Type': 'application/json'},
        json={}
    )
    if res.status_code != 202:
        raise Exception(f'Question {question_id} failed: {res.status_code} {res.text[:500]}')
    data = res.json()
    rows = data['data']['rows']
    cols = [c['name'] for c in data['data']['cols']]
    return [dict(zip(cols, row)) for row in rows]


def get_group(market):
    m = market.upper().replace('-', '').replace('/', '').replace('_', '')
    for coin in INDIVIDUAL_COINS:
        if m.startswith(coin):
            return coin
    return 'other'


def compute_pct(p_val, ex_val):
    if ex_val == 0:
        return 100.0
    return round(100 * p_val / ex_val, 1)


def cap_pct(pct):
    return min(pct, CAP)


def fmt(pct):
    emoji = '✅' if pct >= 100 else '❌'
    return f'{emoji} `{pct}%`'


def parse_rows(rows):
    """Convert raw rows into {market: {ask_0015, bid_0015, ask_003, bid_003}}"""
    result = {}
    for r in rows:
        market = r['market']
        result[market] = {
            'ask_0015': float(r.get('ask_0_0015') or 0),
            'bid_0015': float(r.get('bid_0_0015') or 0),
            'ask_003':  float(r.get('ask_0_003')  or 0),
            'bid_003':  float(r.get('bid_0_003')   or 0),
        }
    return result


def get_individual(data, coin):
    matched = None
    for m in data.keys():
        clean = m.upper().replace('-', '').replace('/', '').replace('_', '')
        if clean.startswith(coin.upper()):
            matched = m
            break
    if not matched:
        return None
    return data[matched]


def compute_other_avgs(data):
    sums = {
        ('ask', '003'):  [],
        ('bid', '003'):  [],
        ('ask', '0015'): [],
        ('bid', '0015'): [],
    }
    for market, vals in data.items():
        if get_group(market) != 'other':
            continue
        sums[('ask', '003')].append(cap_pct(vals['ask_003']))
        sums[('bid', '003')].append(cap_pct(vals['bid_003']))
        sums[('ask', '0015')].append(cap_pct(vals['ask_0015']))
        sums[('bid', '0015')].append(cap_pct(vals['bid_0015']))

    result = {}
    for key, vals in sums.items():
        result[key] = round(sum(vals) / len(vals), 1) if vals else 0.0
    return result


def format_coin_block(name, ask_003, bid_003, ask_0015, bid_0015):
    lines = [f'*{name}*']
    lines.append(f'  30bps ask: {fmt(ask_003)}  bid: {fmt(bid_003)}')
    lines.append(f'  15bps ask: {fmt(ask_0015)}  bid: {fmt(bid_0015)}')
    return '\n'.join(lines)


def send_telegram(message):
    url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
    requests.post(url, json={'chat_id': CHAT_ID, 'text': message, 'parse_mode': 'Markdown'})


def main():
    token = get_metabase_token()

    lines = ['📊 *Daily Liquidity Report — Albert Blanc*\n']

    for label, question_id in [('1H', QUESTION_1H), ('12H', QUESTION_12H)]:
        rows = fetch_question(token, question_id)
        data = parse_rows(rows)

        lines.append(f'*── {label} ──*\n')

        # Individual coins
        for coin in INDIVIDUAL_COINS:
            vals = get_individual(data, coin)
            if vals:
                lines.append(format_coin_block(
                    coin,
                    vals['ask_003'], vals['bid_003'],
                    vals['ask_0015'], vals['bid_0015']
                ))
            else:
                lines.append(f'*{coin}*: no data')
            lines.append('')

        # Other averaged with 300% cap
        other = compute_other_avgs(data)
        lines.append(format_coin_block(
            'Other (avg, capped at 300%)',
            other[('ask', '003')], other[('bid', '003')],
            other[('ask', '0015')], other[('bid', '0015')]
        ))
        lines.append('')

    send_telegram('\n'.join(lines))


if __name__ == '__main__':
    main()
