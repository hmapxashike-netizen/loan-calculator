import os
import sys
import psycopg2

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import config

ACCOUNTS = [
    # Asset Parent
    ("A100000", "CASH AND CASH EQUIVALENTS", "ASSET", None, None),
    ("A100001", "BANK - MAIN OPERATING ACCOUNT", "ASSET", "cash_operating", "A100000"),
    ("A100002", "PETTY CASH", "ASSET", "petty_cash", "A100000"),
    ("A110000", "CUSTOMER LOAN PORTFOLIO (PRINCIPAL)", "ASSET", None, None),
    ("A110001", "PRINCIPAL - NOT YET DUE", "ASSET", "loan_principal", "A110000"),
    ("A110002", "PRINCIPAL ARREARS", "ASSET", "principal_arrears", "A110000"),
    ("A120000", "LOAN INTEREST & FEES RECEIVABLE", "ASSET", None, None),
    ("A120001", "REGULAR INTEREST ACCRUED (UNBILLED)", "ASSET", "regular_interest_accrued", "A120000"),
    ("A120002", "REGULAR INTEREST ARREARS", "ASSET", "regular_interest_arrears", "A120000"),
    ("A120003", "PENALTY INTEREST ASSET", "ASSET", "penalty_interest_asset", "A120000"),
    ("A120004", "DEFAULT INTEREST ASSET", "ASSET", "default_interest_asset", "A120000"),
    ("A120005", "FEES AND CHARGES ARREARS", "ASSET", "fees_charges_arrears", "A120000"),
    ("A130000", "PORTFOLIO CONTRA-ASSETS (PROVISION)", "ASSET", None, None),
    ("A130001", "ALLOWANCE FOR CREDIT LOSSES", "ASSET", "allowance_credit_losses", "A130000"),
    ("A130002", "PENALTY INTEREST IN SUSPENSE", "ASSET", "penalty_interest_suspense", "A130000"),
    ("A130003", "DEFAULT INTEREST IN SUSPENSE", "ASSET", "default_interest_suspense", "A130000"),
    ("A130004", "REGULAR INTEREST IN SUSPENSE", "ASSET", "regular_interest_suspense", "A130000"),
    ("A140000", "FIXED & OTHER ASSETS", "ASSET", None, None),
    ("A140001", "FIXED ASSETS - COST", "ASSET", "fixed_assets_cost", "A140000"),
    ("A140002", "ACCUMULATED DEPRECIATION", "ASSET", "accumulated_depreciation", "A140000"),
    ("A140003", "DEFERRED FEE AND COMMISSION ASSET", "ASSET", "deferred_fee_commission_asset", "A140000"),
    ("A150000", "DEFERRED BORROWING COSTS", "ASSET", None, None),
    ("A150001", "DEFERRED FEE ASSET (BORROWINGS)", "ASSET", "deferred_fee_asset_borrowings", "A150000"),
    # Liabilities
    ("L200000", "CUSTOMER & SHORT-TERM LIABILITIES", "LIABILITY", None, None),
    ("L200001", "DEFERRED FEE LIABILITY", "LIABILITY", "deferred_fee_liability", "L200000"),
    ("L200002", "REGULAR INTEREST INCOME HOLDING", "LIABILITY", "regular_interest_income_holding", "L200000"),
    ("L210000", "BORROWINGS & EXTERNAL DEBT", "LIABILITY", None, None),
    ("L210001", "BORROWINGS - LOAN PRINCIPAL", "LIABILITY", "borrowings_loan_principal", "L210000"),
    ("L210002", "INTEREST PAYABLE", "LIABILITY", "interest_payable", "L210000"),
    ("L220000", "OPERATIONAL LIABILITIES", "LIABILITY", None, None),
    ("L220001", "ACCOUNTS PAYABLE", "LIABILITY", "accounts_payable", "L220000"),
    ("L220002", "ACCRUED EXPENSES", "LIABILITY", "accrued_expenses", "L220000"),
    ("L220003", "TAX LIABILITY", "LIABILITY", "tax_liability", "L220000"),
    # Equity
    ("C300000", "EQUITY AND CAPITAL", "EQUITY", None, None),
    ("C300001", "SHARE CAPITAL", "EQUITY", "share_capital", "C300000"),
    ("C300002", "SHARE PREMIUM", "EQUITY", "share_premium", "C300000"),
    ("C300003", "RETAINED EARNINGS", "EQUITY", "retained_earnings", "C300000"),
    ("C300004", "DIVIDENDS PAID", "EQUITY", "dividends_paid", "C300000"),
    # Revenue
    ("R400000", "LENDING INCOME (REVENUE)", "INCOME", None, None),
    ("R400001", "REGULAR INTEREST INCOME", "INCOME", "regular_interest_income", "R400000"),
    ("R400002", "DEFERRED FEE INCOME", "INCOME", "deferred_fee_income", "R400000"),
    ("R400003", "PENALTY INTEREST INCOME", "INCOME", "penalty_interest_income", "R400000"),
    ("R400004", "DEFAULT INTEREST INCOME", "INCOME", "default_interest_income", "R400000"),
    ("R400005", "BAD DEBTS RECOVERED", "INCOME", "bad_debts_recovered", "R400000"),
    # Expense
    ("E500000", "FINANCIAL & LENDING EXPENSES", "EXPENSE", None, None),
    ("E500001", "INTEREST EXPENSE", "EXPENSE", "interest_expense", "E500000"),
    ("E500002", "IMPAIRMENT LOSS EXPENSE", "EXPENSE", "impairment_loss_expense", "E500000"),
    ("E500003", "BAD DEBTS EXPENSE", "EXPENSE", "bad_debts_expense", "E500000"),
    ("E500004", "FEES AND COMMISSION EXPENSE", "EXPENSE", "fees_commission_expense", "E500000"),
    ("E500005", "AMORTIZATION OF BORROWING FEES", "EXPENSE", "amortization_borrowing_fees", "E500000"),
    ("E510000", "ADMINISTRATIVE EXPENSES", "EXPENSE", None, None),
    ("E510001", "STAFF COSTS", "EXPENSE", "staff_costs", "E510000"),
    ("E510002", "PREMISES & UTILITIES", "EXPENSE", "premises_utilities", "E510000"),
    ("E510003", "COMMUNICATION & IT", "EXPENSE", "communication_it", "E510000"),
    ("E510004", "DEPRECIATION EXPENSE", "EXPENSE", "depreciation_expense", "E510000"),
    ("E510005", "TRAVEL & MOTOR", "EXPENSE", "travel_motor", "E510000"),
    ("E510006", "PROFESSIONAL FEES", "EXPENSE", "professional_fees", "E510000"),
    ("E510007", "OFFICE ADMIN", "EXPENSE", "office_admin", "E510000"),
    ("E510008", "MARKETING", "EXPENSE", "marketing", "E510000"),
]

def seed_db():
    conn = psycopg2.connect(config.get_database_url())
    with conn.cursor() as cur:
        # 1. Insert into account_template
        for code, name, cat, tag, parent in ACCOUNTS:
            cur.execute("""
                INSERT INTO account_template (code, name, category, system_tag, parent_code)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (code) DO UPDATE SET
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    system_tag = EXCLUDED.system_tag,
                    parent_code = EXCLUDED.parent_code
            """, (code, name, cat, tag, parent))
        
    conn.commit()
    conn.close()
    print("Seed complete.")

if __name__ == "__main__":
    seed_db()
