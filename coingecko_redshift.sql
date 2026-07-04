CREATE TABLE coingecko_curated.coins (
    id VARCHAR(256),
    symbol VARCHAR(50),
    name VARCHAR(256),
    current_price DOUBLE PRECISION,
    last_updated TIMESTAMP,
    price_change_24h DOUBLE PRECISION,
    price_change_percentage_24h DOUBLE PRECISION,
    total_volume BIGINT,
    market_cap BIGINT,
    market_cap_rank INTEGER,
    ath DOUBLE PRECISION,
    circulating_supply DOUBLE PRECISION,
    total_supply DOUBLE PRECISION,
    max_supply DOUBLE PRECISION
)
DISTSTYLE AUTO;

CREATE TABLE coingecko_curated.exchanges (
    name VARCHAR(265),
    country VARCHAR(100),
    trust_score INT,
    trust_score_rank INT,
    trade_volume_24h_btc DOUBLE PRECISION,
    date_updated DATE
)
DISTSTYLE AUTO;

CREATE TABLE coingecko_curated.global_metrics (
    active_cryptocurrencies INT,
    usd DOUBLE PRECISION,
    btc DOUBLE PRECISION,
    market_cap_change_percentage_24h_usd DOUBLE PRECISION,
    volume_change_percentage_24h_usd DOUBLE PRECISION,
    updated_at TIMESTAMP
)
DISTSTYLE AUTO;
