# Bundled accounting defaults

JSON files in this folder are the **source of truth** for:

- **Chart of accounts** (when you click *Initialize Default Chart of Accounts* — `account_template` is rebuilt from here, then copied into empty `accounts`).
- **Transaction templates** (*Reset Default Transaction Templates*).
- **Receipt → GL mapping** (*Initialize Default Mappings* / *Reset to Defaults*).

If a file is **missing**, the app falls back to `accounting_builtin_defaults.py`.

## Refresh from your perfected database

```bash
python scripts/export_accounting_defaults.py
```

Commit the updated JSON so every environment restores the same setup.

## Regenerate JSON from Python fallbacks only

```bash
python scripts/bootstrap_accounting_defaults_from_builtin.py
```

Use this on a fresh clone when you only need the built-in chart (no DB export yet).

## `seed_accounting.py`

`python scripts/seed_accounting.py` upserts `account_template` using the same chart tuples as the loader (JSON if present, else built-in).
