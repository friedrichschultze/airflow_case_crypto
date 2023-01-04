SELECT crypto_curr.id as id,
    crypto_curr.symbol as symbol,
    crypto_curr.name as name,
    crypto_curr.snapshot_date as snapshot_date,
    crypto_curr.current_price_usd as current_price_usd,
    crypto_curr.current_price_usd - crypto_7d.current_price_usd as price_delta_current_vs_7d,
    crypto_curr.current_price_usd - crypto_14d.current_price_usd as price_delta_current_vs_14d,
    crypto_curr.market_cap_usd as market_cap_usd,
    crypto_curr.market_cap_usd - crypto_7d.market_cap_usd as market_cap_delta_current_vs_7d,
    crypto_curr.market_cap_usd - crypto_14d.market_cap_usd as market_cap_usd_current_vs_14d
    
FROM `data-case-study-322621.renan.crypto_currency` AS crypto_curr
LEFT JOIN `data-case-study-322621.renan.crypto_currency` AS crypto_7d 
    ON crypto_curr.snapshot_date = DATE_ADD(crypto_7d.snapshot_date, INTERVAL 7 DAY) AND crypto_curr.id = crypto_7d.id
LEFT JOIN `data-case-study-322621.renan.crypto_currency` AS crypto_14d 
    ON crypto_curr.snapshot_date = DATE_ADD(crypto_14d.snapshot_date, INTERVAL 14 DAY) AND crypto_curr.id = crypto_14d.id
    
WHERE crypto_7d.current_price_usd is not null
    AND crypto_14d.current_price_usd is not null

ORDER BY crypto_curr.snapshot_date DESC,
    crypto_curr.id ASC