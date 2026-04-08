# Bundled accounting defaults

JSON files in this folder are the **source of truth** for:

- **Chart of accounts** (when you click *Initialize Default Chart of Accounts* — `account_template` is rebuilt from here, then copied into empty `accounts`).
- **Transaction templates** (*Reset Default Transaction Templates*).
- **Receipt → GL mapping** (*Initialize Default Mappings* / *Reset to Defaults*).

If a file is **missing**, the app falls back to `accounting.builtin_defaults`.

The **Accounting** page includes an expander **Download bundled default templates** so you can save `chart_of_accounts.json`, `transaction_templates.json`, and `receipt_gl_mapping.json` without using the shell.

Keep **`accounting/builtin_defaults.py`** in sync with these JSON files: the loader prefers JSON when present, but seeds and bootstrap use the same tuple definitions in Python.

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
