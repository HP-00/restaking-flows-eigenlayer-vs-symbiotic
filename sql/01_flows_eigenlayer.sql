WITH flows AS (
    SELECT
        date_trunc('day', date)              AS day,
        strategy                              AS strategy_addr,
        SUM(shares)                           AS net_shares
    FROM   eigenlayer_ethereum.strategy_shares_netflow_by_day
    GROUP  BY 1, 2
),

xr AS (
    SELECT
        strategy                              AS strategy_addr,
        price_eth                             AS exch_rate_eth   -- ETH per share
    FROM   dune.firstset.result_eigenlayer_strategy_latest_exchange_rates
),

flows_eth AS (
    SELECT
        f.day,
        CASE WHEN f.net_shares > 0
             THEN  (f.net_shares / 1e18) * xr.exch_rate_eth END      AS eth_in,
        CASE WHEN f.net_shares < 0
             THEN -(f.net_shares / 1e18) * xr.exch_rate_eth END      AS eth_out,
        (f.net_shares / 1e18) * xr.exch_rate_eth                     AS net_eth
    FROM   flows f
    JOIN   xr   ON f.strategy_addr = xr.strategy_addr
)

SELECT
    day,
    COALESCE(SUM(eth_in),  0)                                   AS eth_in,
    COALESCE(SUM(eth_out), 0)                                   AS eth_out,
    COALESCE(SUM(net_eth), 0)                                   AS net_eth,
    SUM(SUM(net_eth)) OVER (ORDER BY day)                       AS cum_net_eth
FROM flows_eth
GROUP BY day
ORDER BY day;