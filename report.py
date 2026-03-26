import os
import psycopg2
import requests

DB_URL = os.environ["DATABASE_URL"]
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

QUERY = """
WITH
ex_1h AS (
  SELECT market,
    MAX(ask_avg_liquidity_0_0001) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS ask_sum_0_0001,
    MAX(ask_avg_liquidity_0_0003) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS ask_sum_0_0003,
    MAX(ask_avg_liquidity_0_0015) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS ask_sum_0_0015,
    MAX(ask_avg_liquidity_0_0030) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS ask_sum_0_0030,
    MAX(bid_avg_liquidity_0_0001) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS bid_sum_0_0001,
    MAX(bid_avg_liquidity_0_0003) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS bid_sum_0_0003,
    MAX(bid_avg_liquidity_0_0015) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS bid_sum_0_0015,
    MAX(bid_avg_liquidity_0_0030) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS bid_sum_0_0030
  FROM stats.exchange_liquidity_stats
  WHERE period = '1H' AND exchange_name IN ('BINANCE','HYPERLIQUID')
  GROUP BY market
),
ex_12h AS (
  SELECT market,
    MAX(ask_avg_liquidity_0_0001) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS ask_sum_0_0001,
    MAX(ask_avg_liquidity_0_0003) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS ask_sum_0_0003,
    MAX(ask_avg_liquidity_0_0015) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS ask_sum_0_0015,
    MAX(ask_avg_liquidity_0_0030) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS ask_sum_0_0030,
    MAX(bid_avg_liquidity_0_0001) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS bid_sum_0_0001,
    MAX(bid_avg_liquidity_0_0003) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS bid_sum_0_0003,
    MAX(bid_avg_liquidity_0_0015) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS bid_sum_0_0015,
    MAX(bid_avg_liquidity_0_0030) FILTER (WHERE exchange_name IN ('BINANCE','HYPERLIQUID')) AS bid_sum_0_0030
  FROM stats.exchange_liquidity_stats
  WHERE period = '12H' AND exchange_name IN ('BINANCE','HYPERLIQUID')
  GROUP BY market
),
partner_1h AS (
  SELECT market,
    AVG(ask_liquidity) FILTER (WHERE spread = 0.0001) AS p_ask_0_0001,
    AVG(ask_liquidity) FILTER (WHERE spread = 0.0003) AS p_ask_0_0003,
    AVG(ask_liquidity) FILTER (WHERE spread = 0.0015) AS p_ask_0_0015,
    AVG(ask_liquidity) FILTER (WHERE spread = 0.0030) AS p_ask_0_0030,
    AVG(bid_liquidity) FILTER (WHERE spread = 0.0001) AS p_bid_0_0001,
    AVG(bid_liquidity) FILTER (WHERE spread = 0.0003) AS p_bid_0_0003,
    AVG(bid_liquidity) FILTER (WHERE spread = 0.0015) AS p_bid_0_0015,
    AVG(bid_liquidity) FILTER (WHERE spread = 0.0030) AS p_bid_0_0030
  FROM partner_liquidity_stats
  WHERE partner = 'Albert Blanc' AND "timestamp" >= now() - interval '1 hour'
  GROUP BY market
),
partner_12h AS (
  SELECT market,
    AVG(ask_liquidity) FILTER (WHERE spread = 0.0001) AS p_ask_0_0001,
    AVG(ask_liquidity) FILTER (WHERE spread = 0.0003) AS p_ask_0_0003,
    AVG(ask_liquidity) FILTER (WHERE spread = 0.0015) AS p_ask_0_0015,
    AVG(ask_liquidity) FILTER (WHERE spread = 0.0030) AS p_ask_0_0030,
    AVG(bid_liquidity) FILTER (WHERE spread = 0.0001) AS p_bid_0_0001,
    AVG(bid_liquidity) FILTER (WHERE spread = 0.0003) AS p_bid_0_0003,
    AVG(bid_liquidity) FILTER (WHERE spread = 0.0015) AS p_bid_0_0015,
    AVG(bid_liquidity) FILTER (WHERE spread = 0.0030) AS p_bid_0_0030
  FROM partner_liquidity_stats
  WHERE partner = 'Albert Blanc' AND "timestamp" >= now() - interval '12 hours'
  GROUP BY market
),
per_market_1h AS (
  SELECT p.market,
    COALESCE(ROUND(100 * p.p_ask_0_0001 / NULLIF(e.ask_sum_0_0001, 0), 2), 100) AS ask_0_0001,
    COALESCE(ROUND(100 * p.p_bid_0_0001 / NULLIF(e.bid_sum_0_0001, 0), 2), 100) AS bid_0_0001,
    COALESCE(ROUND(100 * p.p_ask_0_0003 / NULLIF(e.ask_sum_0_0003, 0), 2), 100) AS ask_0_0003,
    COALESCE(ROUND(100 * p.p_bid_0_0003 / NULLIF(e.bid_sum_0_0003, 0), 2), 100) AS bid_0_0003,
    COALESCE(ROUND(100 * p.p_ask_0_0015 / NULLIF(e.ask_sum_0_0015, 0), 2), 100) AS ask_0_0015,
    COALESCE(ROUND(100 * p.p_bid_0_0015 / NULLIF(e.bid_sum_0_0015, 0), 2), 100) AS bid_0_0015,
    COALESCE(ROUND(100 * p.p_ask_0_0030 / NULLIF(e.ask_sum_0_0030, 0), 2), 100) AS ask_0_003,
    COALESCE(ROUND(100 * p.p_bid_0_0030 / NULLIF(e.bid_sum_0_0030, 0), 2), 100) AS bid_0_003
  FROM partner_1h p JOIN ex_1h e ON e.market = p.market
),
per_market_12h AS (
  SELECT p.market,
    COALESCE(ROUND(100 * p.p_ask_0_0001 / NULLIF(e.ask_sum_0_0001, 0), 2), 100) AS ask_0_0001,
    COALESCE(ROUND(100 * p.p_bid_0_0001 / NULLIF(e.bid_sum_0_0001, 0), 2), 100) AS bid_0_0001,
    COALESCE(ROUND(100 * p.p_ask_0_0003 / NULLIF(e.ask_sum_0_0003, 0), 2), 100) AS ask_0_0003,
    COALESCE(ROUND(100 * p.p_bid_0_0003 / NULLIF(e.bid_sum_0_0003, 0), 2), 100) AS bid_0_0003,
    COALESCE(ROUND(100 * p.p_ask_0_0015 / NULLIF(e.ask_sum_0_0015, 0), 2), 100) AS ask_0_0015,
    COALESCE(ROUND(100 * p.p_bid_0_0015 / NULLIF(e.bid_sum_0_0015, 0), 2), 100) AS bid_0_0015,
    COALESCE(ROUND(100 * p.p_ask_0_0030 / NULLIF(e.ask_sum_0_0030, 0), 2), 100) AS ask_0_003,
    COALESCE(ROUND(100 * p.p_bid_0_0030 / NULLIF(e.bid_sum_0_0030, 0), 2), 100) AS bid_0_003
  FROM partner_12h p JOIN ex_12h e ON e.market = p.market
),
unpivoted_1h AS (
  SELECT market, ask_0_0001 AS pct FROM per_market_1h UNION ALL
  SELECT market, bid_0_0001 FROM per_market_1h UNION ALL
  SELECT market, ask_0_0003 FROM per_market_1h UNION ALL
  SELECT market, bid_0_0003 FROM per_market_1h UNION ALL
  SELECT market, ask_0_0015 FROM per_market_1h UNION ALL
  SELECT market, bid_0_0015 FROM per_market_1h UNION ALL
  SELECT market, ask_0_003  FROM per_market_1h UNION ALL
  SELECT market, bid_0_003  FROM per_market_1h
),
unpivoted_12h AS (
  SELECT market, ask_0_0001 AS pct FROM per_market_12h UNION ALL
  SELECT market, bid_0_0001 FROM per_market_12h UNION ALL
  SELECT market, ask_0_0003 FROM per_market_12h UNION ALL
  SELECT market, bid_0_0003 FROM per_market_12h UNION ALL
  SELECT market, ask_0_0015 FROM per_market_12h UNION ALL
  SELECT market, bid_0_0015 FROM per_market_12h UNION ALL
  SELECT market, ask_0_003  FROM per_market_12h UNION ALL
  SELECT market, bid_0_003  FROM per_market_12h
)
SELECT '1H' AS period,
  ROUND(100.0 * COUNT(*) FILTER (WHERE pct >= 60) / NULLIF(COUNT(*), 0), 2) AS success_rate_pct,
  COUNT(*) FILTER (WHERE pct >= 60) AS passing_count,
  COUNT(*) AS total_count
FROM unpivoted_1h
UNION ALL
SELECT '12H',
  ROUND(100.0 * COUNT(*) FILTER (WHERE pct >= 60) / NULLIF(COUNT(*), 0), 2),
  COUNT(*) FILTER (WHERE pct >= 60),
  COUNT(*)
FROM unpivoted_12h
ORDER BY period;
"""

def send_telegram(message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"})

def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    cur.execute(QUERY)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    lines = ["📊 *Daily Liquidity Report — Albert Blanc*\n"]
    for period, success_rate, passing, total in rows:
        emoji = "✅" if success_rate >= 60 else "❌"
        lines.append(
            f"{emoji} *{period}*: `{success_rate}%` success "
            f"({passing}/{total} pairs ≥60%)"
        )

    send_telegram("\n".join(lines))

if __name__ == "__main__":
    main()
