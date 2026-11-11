-- ============================================================================
-- Trading Engine & Order Book Schema (Spring Data R2DBC / PostgreSQL)
-- Formation : Java Avancé – Programmation Réactive avec Spring WebFlux
-- ============================================================================

CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL UNIQUE,
    account_id VARCHAR(64) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    side VARCHAR(8) NOT NULL,       -- BUY, SELL
    type VARCHAR(16) NOT NULL,      -- LIMIT, MARKET
    price NUMERIC(18, 4) NOT NULL,
    quantity NUMERIC(18, 6) NOT NULL,
    status VARCHAR(16) NOT NULL,    -- PENDING, FILLED, REJECTED, CANCELLED
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_symbol_status ON orders(symbol, status);
CREATE INDEX IF NOT EXISTS idx_orders_account ON orders(account_id);

CREATE TABLE IF NOT EXISTS trades (
    id BIGSERIAL PRIMARY KEY,
    trade_id VARCHAR(64) NOT NULL UNIQUE,
    buy_order_id VARCHAR(64) NOT NULL,
    sell_order_id VARCHAR(64) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    price NUMERIC(18, 4) NOT NULL,
    quantity NUMERIC(18, 6) NOT NULL,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol, executed_at DESC);

CREATE TABLE IF NOT EXISTS market_quotes (
    symbol VARCHAR(16) PRIMARY KEY,
    last_price NUMERIC(18, 4) NOT NULL,
    bid NUMERIC(18, 4) NOT NULL,
    ask NUMERIC(18, 4) NOT NULL,
    volume_24h NUMERIC(18, 2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Données initiales de marché (Cotations de référence)
INSERT INTO market_quotes (symbol, last_price, bid, ask, volume_24h) VALUES
('BTC-USD', 96500.00, 96480.00, 96520.00, 1450000000.00),
('ETH-USD', 3420.50, 3419.00, 3422.00, 680000000.00),
('NVDA', 142.80, 142.75, 142.85, 3200000000.00),
('AAPL', 234.50, 234.40, 234.60, 2100000000.00)
ON CONFLICT (symbol) DO NOTHING;
