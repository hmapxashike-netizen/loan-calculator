# LMS Database Schema (PostgreSQL 18)

## Setup order

1. **Create the database** (connect to `postgres` or any existing DB):
   ```bash
   psql -U postgres -f 01_create_database.sql
   ```
   Or in `psql`: `\i 01_create_database.sql` (after connecting to `postgres`).

2. **Create tables** (connect to `lms_db`):
   ```bash
   psql -U postgres -d lms_db -f 02_schema.sql
   ```

3. **Customer module** (individuals, corporates, addresses, contact persons, directors, shareholders):
   ```bash
   psql -U postgres -d lms_db -f 03_customers.sql
   ```
   This adds `type` and `status` to `customers`, creates related tables, and migrates existing name/email/phone into `individuals`.

4. **Loan repayments and loan-detail fields** (for Capture Loan and recording actual payments):
   ```bash
   psql -U postgres -d lms_db -f 04_loan_repayments.sql
   ```
   Adds `first_repayment_date` and `payment_timing` to `loans`, and creates `loan_repayments` for payment/receipt details.

## Tables

| Table            | Purpose |
|------------------|---------|
| `customers`      | id, type (individual\|corporate), status (active\|inactive). No delete; set status to inactive. |
| `individuals`    | Individual details: name, national_id, employer_details, phone1, phone2, email1, email2. |
| `corporates`     | Corporate details: legal_name, trading_name, reg_number, tin. |
| `customer_addresses` | Addresses per customer: address_type, line1, line2, city, region, postal_code, country. |
| `corporate_contact_persons` | Contact person(s): full_name, national_id, designation, phone1, phone2, email, address. |
| `corporate_directors` | Directors: full_name, national_id, designation, phone1, phone2, email, address. |
| `corporate_shareholders` | Shareholders: same + shareholding_pct. |
| `loans`          | Loan contracts. Field names match `app.py` loan_record. |
| `loan_schedules` | One row per schedule version per loan (version 1 = original, 2+ = reschedule). |
| `schedule_lines` | One row per period (instalments). Columns match app.py schedule rows. |
| `loan_repayments` | Actual payments/receipts (payment date, amount, reference); distinct from planned schedule. |
| `config`         | Optional key-value configuration. |

## Relationships

- **customers** 1 → many **loans**
- **loans** 1 → many **loan_schedules** (original + reschedules)
- **loan_schedules** 1 → many **schedule_lines**
