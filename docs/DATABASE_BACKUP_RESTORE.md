# Database backup and restore (PostgreSQL)

FarndaCred stores the **system business date** in PostgreSQL: `system_business_config.current_system_date` (single row, `id = 1`). The app **no longer lets you edit that date in the UI**; it advances when **EOD Date advance** completes successfully, and it reverts to whatever was in the backup when you **restore** a dump taken earlier.

Use **native PostgreSQL tools** for backups—this is the usual production approach (scheduled jobs, DBA runbook, or your cloud provider’s automated backups).

## Prerequisites

- **PostgreSQL client tools** on the machine that runs backups: `pg_dump`, `pg_restore`, and usually `psql`. On Windows, install from the same major version as your server (e.g. PostgreSQL 16/18 installer → include “Command Line Tools”).
- A **connection URL** the tools can use. The app reads (in order):
  - `FARNDACRED_DATABASE_URL`
  - `LMS_DATABASE_URL`
  - or builds from `FARNDACRED_DB_*` / `LMS_DB_*` (see `config.get_database_url()` in `config.py`).

Never commit real URLs or passwords; use environment variables or a secrets manager.

## Recommended backup format

Use **custom format** (`-Fc`). It compresses well, allows parallel restore, and is the standard for production-style restores.

### Manual backup (label in the filename)

**This repo:** use the `dump/` folder under the project root (tracked in git as an empty folder; `*.dump` files inside are **gitignored**). From `FarndaCred` in PowerShell:

```powershell
mkdir dump -Force
pg_dump -U postgres -F c -v -f "dump/farndacred_db_2026-04-10_before-change.dump" farndacred_db
```

Restore from the same path (stop Streamlit first; `-c` overwrites objects in the target DB):

```powershell
pg_restore -U postgres -d farndacred_db -c --if-exists -v --no-owner --no-acl "dump/farndacred_db_2026-04-10_before-change.dump"
```

`pg_dump` does **not** create the `dump` folder automatically—run `mkdir dump` once (or use `-Force` as above).

---

You can instead use a folder **outside** the repo (e.g. `D:\Backups\FarndaCred\`). Use a name that encodes **when** and **why**:

```text
farndacred_db_2026-04-09_before-migration.dump
```

**PowerShell** (URL in env var—adjust variable name to match how you store it):

```powershell
$env:PGPASSWORD = "<password>"   # only if not embedded in URL; prefer URL with encoded password
pg_dump --format=custom --blobs --verbose --file "D:\Backups\FarndaCred\farndacred_db_2026-04-09_manual.dump" $env:FARNDACRED_DATABASE_URL
```

If you use a full URL string:

```powershell
pg_dump --format=custom --blobs --verbose --file "D:\Backups\FarndaCred\farndacred_db_2026-04-09_manual.dump" "postgresql://user:pass@localhost:5432/farndacred_db"
```

Optional **plain SQL** (human-readable, larger files; restore with `psql`):

```powershell
pg_dump --format=plain --no-owner --no-acl --file "D:\Backups\FarndaCred\farndacred_db_2026-04-09.sql" "postgresql://..."
```

### When to back up

- Before **schema migrations**, **bulk data fixes**, or **risky EOD experiments**.
- On a **schedule** (e.g. nightly) via Windows Task Scheduler or your host’s backup product.

## Restore (replace database contents)

Restoring **overwrites** data. **Stop the Streamlit app** (and anything else using the DB) first so no one writes during restore.

### Option A: Restore into an empty database (safest mental model)

1. Create a new empty database (or drop and recreate the target DB—**destructive**).
2. Restore:

```powershell
pg_restore --verbose --no-owner --no-acl --dbname="postgresql://user:pass@localhost:5432/farndacred_db_restored" "D:\Backups\FarndaCred\farndacred_db_2026-04-09_manual.dump"
```

If the dump was created with a single database as source, you may need `--create` and a connection to `postgres` maintenance DB (advanced; follow PostgreSQL docs for your version).

3. Point `FARNDACRED_DATABASE_URL` at the restored database and start the app.
4. Confirm **system business date** after restore:

```sql
SELECT id, current_system_date FROM system_business_config WHERE id = 1;
```

That value is whatever it was **at backup time**—no separate “sync date” step.

### Option B: Restore over existing database (`--clean`)

`pg_restore --clean` drops objects before recreating them. **All connections to that database must be closed.** This is powerful and easy to get wrong; prefer Option A for learning, or use your DBA runbook.

```powershell
pg_restore --verbose --clean --if-exists --no-owner --no-acl --dbname="postgresql://..." "D:\Backups\FarndaCred\...\dump"
```

Resolve errors about active sessions by stopping the app and terminating connections (PostgreSQL: `pg_terminate_backend` on other sessions, or restart the PostgreSQL service during a maintenance window).

### Plain SQL dump restore

```powershell
psql "postgresql://..." -v ON_ERROR_STOP=1 -f "D:\Backups\FarndaCred\farndacred_db_2026-04-09.sql"
```

## Break-glass: change system date without restoring

Only for **exceptional** recovery when you must adjust `current_system_date` and you accept consistency risk (e.g. misaligned `loan_daily_state` vs journals). Prefer **restore from backup** or **EOD backfill** per operational procedure.

```sql
UPDATE system_business_config
SET current_system_date = DATE '2026-04-01', updated_at = NOW()
WHERE id = 1;
```

Run as a privileged DB user; document who did it and why.

## Hosted PostgreSQL

If you use RDS, Azure Database for PostgreSQL, Supabase, Neon, etc., use their **automated backups** and **point-in-time recovery** where offered; keep this document as a supplement for self-hosted or manual logical dumps.

## Security

- Backup files contain **full business data**; encrypt at rest and restrict filesystem permissions.
- Do not store dumps in the repository or public shares.
