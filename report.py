import os
import requests

METABASE_URL = 'https://x10.metabaseapp.com'
METABASE_USERNAME = os.environ['METABASE_USERNAME']
METABASE_PASSWORD = os.environ['METABASE_PASSWORD']

DATABASE_ID = 100
F_PARTNER = 5219

def get_metabase_token():
    res = requests.post(
        f'{METABASE_URL}/api/session',
        json={'username': METABASE_USERNAME, 'password': METABASE_PASSWORD}
    )
    return res.json()['id']

def main():
    token = get_metabase_token()
    payload = {
        'database': DATABASE_ID,
        'type': 'query',
        'query': {
            'source-table': 925,
            'aggregation': [['count']],
            'breakout': [['field', F_PARTNER, None]]
        }
    }
    res = requests.post(
        f'{METABASE_URL}/api/dataset',
        headers={'X-Metabase-Session': token, 'Content-Type': 'application/json'},
        json=payload
    )
    print('Partners in table:')
    for row in res.json()['data']['rows']:
        print(row)

if __name__ == '__main__':
    main()
