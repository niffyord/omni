import psycopg2

conn = psycopg2.connect("postgresql://algouser:mysecretpassword@localhost:5432/algodb")
cur = conn.cursor()
cur.execute("""
ALTER TABLE derivatives_metrics
  ADD COLUMN IF NOT EXISTS oi_5m_delta_pct DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS oi_15m_delta_pct DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS oi_4h_delta_pct DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS ls_5m_delta_pct DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS ls_15m_delta_pct DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS ls_4h_delta_pct DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS oi_z_24h DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS funding_8h_avg DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS funding_z DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS perp_quarterly_basis_pct DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS basis_annualised DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS liq_imbalance_8h DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS oi_turnover_ratio DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS core_eth_funding_spread DOUBLE PRECISION;
""")
conn.commit()
cur.close()
conn.close()
print("Migration complete.")