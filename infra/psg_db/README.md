# Disposable PostgreSQL for Runtime Testing

Lightweight PostgreSQL setup for local testing with one user and disposable storage.

## Files

- `docker-compose.yml` - service definition
- `Dockerfile` - extends official Postgres image
- `init/01-init.sql` - creates seed schema/table/data
- `.env.example` - sample credentials

## Quick Start

1. Optional: copy env template

```powershell
Copy-Item .env.example .env
```

2. Start

```powershell
docker compose up -d --build
```

3. Test connection

```powershell
docker compose exec postgres-test psql -U test_user -d testdb -c "SELECT * FROM runtime.sample_events;"
```

## Power BI Connection

Use these values in Power BI PostgreSQL connector:

- Server: `localhost`
- Port: `5432`
- Database: `testdb`
- Username: `test_user`
- Password: `test_pass`

If you use `.env`, use your overridden values instead.

## Disposable Behavior

Data directory is mounted as `tmpfs`, so data is not persistent across container recreation/restart.

## JDBC SQL Test Queries

Run these in any JDBC SQL client (DBeaver, Power BI, etc.) after connecting to `testdb`.

```sql
-- sanity read of seeded table
SELECT * FROM runtime.sample_events ORDER BY id;
```

```sql
-- create a fresh test table
CREATE TABLE IF NOT EXISTS runtime.powerbi_test (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  amount NUMERIC(12,2) NOT NULL,
  category TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
```

```sql
-- insert sample rows
INSERT INTO runtime.powerbi_test (name, amount, category) VALUES
('alpha', 10.50, 'A'),
('beta', 20.00, 'B'),
('gamma', 42.42, 'A');
```

```sql
-- read table
SELECT * FROM runtime.powerbi_test ORDER BY id;
```

```sql
-- basic aggregation for BI testing
SELECT
  category,
  COUNT(*) AS row_count,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_amount
FROM runtime.powerbi_test
GROUP BY category
ORDER BY category;
```

```sql
-- optional: view metadata
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'runtime'
ORDER BY table_name;
```
