# Database Module

## Purpose

Manages PostgreSQL connection and schema definitions for the EliteResearchAgent system. All data flows through this database.

## Structure

```
database/
├── connection.py       # Connection pooling
└── schema/
    └── sources.sql     # Sources schema definition
```

## Database Architecture

**Single database:** `eliteresearch`

**Current schemas:**
- `sources` - Raw search results and web pages

**Future schemas:**
- `master` - Validated person and organization records
- Service schemas (birthfinder, careerfinder, etc.)

## Sources Schema

**Tables:**

`sources.persons_searched`
- Tracks each search operation
- Fields: person_name, search_query, searched_at

`sources.search_results`
- Individual URLs fetched per search
- Fields: person_search_id, rank, url, title, fetch_status, fetch_error, full_text, fetched_at

## Common Operations

### Initialize Database

```bash
createdb -U postgres eliteresearch
psql -U postgres -d eliteresearch -f database\schema\sources.sql
```

### View Data

```bash
psql -U postgres -d eliteresearch
```

```sql
-- List all persons searched
SELECT * FROM sources.persons_searched;

-- Count results per person
SELECT person_name, COUNT(*) as num_results 
FROM sources.persons_searched ps
JOIN sources.search_results sr ON ps.id = sr.person_search_id
GROUP BY person_name;

-- See successful fetches
SELECT person_name, url, title 
FROM sources.persons_searched ps
JOIN sources.search_results sr ON ps.id = sr.person_search_id
WHERE fetch_status = 'success';
```

### Clear Data

```sql
-- Empty tables but keep structure
TRUNCATE sources.search_results, sources.persons_searched CASCADE;

-- Or drop and recreate schema
DROP SCHEMA sources CASCADE;
-- Then re-run: psql -U postgres -d eliteresearch -f database\schema\sources.sql
```

### Backup

```bash
pg_dump -U postgres eliteresearch > backup.sql
```

## Connection Configuration

Set in `.env`:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=eliteresearch
DB_USER=postgres
DB_PASSWORD=your_password
```

## Connection Pooling

`connection.py` provides:
- `get_connection()` - Get connection from pool
- `release_connection(conn)` - Return to pool
- `close_all_connections()` - Cleanup

Always release connections after use:
```python
conn = get_connection()
try:
    # do work
finally:
    release_connection(conn)
```

## Adding New Schemas

1. Create `schema/new_schema.sql`
2. Define tables with proper foreign keys
3. Run: `psql -U postgres -d eliteresearch -f database\schema\new_schema.sql`
4. Update this README

## Notes

- All schemas live in one database for easy joins
- Foreign keys enforce data integrity
- Indexes optimize common queries
- Use schemas to logically separate concerns