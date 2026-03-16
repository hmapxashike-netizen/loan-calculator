from accounting_dal import get_conn, AccountingRepository
from datetime import date
from decimal import Decimal

class AccountingService:
    def __init__(self):
        pass

    def is_transaction_templates_initialized(self):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.is_transaction_templates_initialized()
        finally:
            conn.close()

    def initialize_default_transaction_templates(self):
        DEFAULT_TEMPLATES = [
            ("LOAN_APPROVAL", "loan_principal", "DEBIT", "Creation of Loan - Principal", "EVENT"),
            ("LOAN_APPROVAL", "cash_operating", "CREDIT", "Creation of Loan - Disbursed Amount", "EVENT"),
            ("LOAN_APPROVAL", "deferred_fee_liability", "CREDIT", "Creation of Loan - Fees", "EVENT"),

            # 2 / 2a. Fee amortisation + recognise deferred fee income
            ("FEE_AMORTISATION", "deferred_fee_liability", "DEBIT", "Amortisation of fees", "EOM"),
            ("FEE_AMORTISATION", "deferred_fee_income", "CREDIT", "Amortisation of fees", "EOM"),
            ("DEFERRED_FEE_INCOME_RECOGNITION", "deferred_fee_liability", "DEBIT", "Recognise deferred fee income", "EOM"),
            ("DEFERRED_FEE_INCOME_RECOGNITION", "deferred_fee_income", "CREDIT", "Recognise deferred fee income", "EOM"),

            # 3–5. Principal billing / receipts / direct payment not yet due
            ("BILLING_PRINCIPAL_ARREARS", "principal_arrears", "DEBIT", "Billing of principal arrears", "EOD"),
            ("BILLING_PRINCIPAL_ARREARS", "loan_principal", "CREDIT", "Billing of principal arrears", "EOD"),
            ("PAYMENT_PRINCIPAL", "cash_operating", "DEBIT", "Payment of principal", "EVENT"),
            ("PAYMENT_PRINCIPAL", "principal_arrears", "CREDIT", "Payment of principal", "EVENT"),
            ("PAYMENT_PRINCIPAL_NOT_YET_DUE", "cash_operating", "DEBIT", "Direct payment of principal not yet due", "EVENT"),
            ("PAYMENT_PRINCIPAL_NOT_YET_DUE", "loan_principal", "CREDIT", "Direct payment of principal not yet due", "EVENT"),

            # 6–10. Regular interest (accrual, billing, receipts, direct payment not yet due)
            ("ACCRUAL_REGULAR_INTEREST", "regular_interest_accrued", "DEBIT", "Accrual of REGULAR interest", "EOD"),
            ("ACCRUAL_REGULAR_INTEREST", "regular_interest_income_holding", "CREDIT", "Accrual of REGULAR interest", "EOD"),
            ("ACCRUAL_PENALTY_INTEREST", "penalty_interest_asset", "DEBIT", "Daily penalty interest accrual (11)", "EOD"),
            ("ACCRUAL_PENALTY_INTEREST", "penalty_interest_suspense", "CREDIT", "Daily penalty interest accrual (11)", "EOD"),
            ("ACCRUAL_DEFAULT_INTEREST", "default_interest_asset", "DEBIT", "Daily default interest accrual (14)", "EOD"),
            ("ACCRUAL_DEFAULT_INTEREST", "default_interest_suspense", "CREDIT", "Daily default interest accrual (14)", "EOD"),
            ("CLEAR_DAILY_ACCRUAL", "regular_interest_income_holding", "DEBIT", "Clear the Daily Accrual Accounts", "EOD"),
            ("CLEAR_DAILY_ACCRUAL", "regular_interest_accrued", "CREDIT", "Clear the Daily Accrual Accounts", "EOD"),
            ("BILLING_REGULAR_INTEREST", "regular_interest_arrears", "DEBIT", "Billing of REGULAR interest arrears", "EOD"),
            ("BILLING_REGULAR_INTEREST", "regular_interest_income", "CREDIT", "Billing of REGULAR interest arrears", "EOD"),
            ("PAYMENT_REGULAR_INTEREST", "cash_operating", "DEBIT", "Payment of REGULAR interest", "EVENT"),
            ("PAYMENT_REGULAR_INTEREST", "regular_interest_arrears", "CREDIT", "Payment of REGULAR interest", "EVENT"),
            ("PAYMENT_REGULAR_INTEREST_NOT_YET_DUE", "cash_operating", "DEBIT", "Direct payment of regular interest not yet due", "EVENT"),
            ("PAYMENT_REGULAR_INTEREST_NOT_YET_DUE", "regular_interest_accrued", "CREDIT", "Direct payment of regular interest not yet due", "EVENT"),

            # 11–13. Penalty interest (accrual, payment, recognise income, reversal)
            ("PAYMENT_PENALTY_INTEREST", "cash_operating", "DEBIT", "Payment of penalty interest (12)", "EVENT"),
            ("PAYMENT_PENALTY_INTEREST", "penalty_interest_asset", "CREDIT", "Payment of penalty interest (12)", "EVENT"),
            ("PAYMENT_PENALTY_INTEREST", "penalty_interest_suspense", "DEBIT", "Recognise penalty income (12a)", "EVENT"),
            ("PAYMENT_PENALTY_INTEREST", "penalty_interest_income", "CREDIT", "Recognise penalty income (12a)", "EVENT"),
            ("REVERSAL_PENALTY_INTEREST", "penalty_interest_income", "DEBIT", "Reversal of penalty interest", "EVENT"),
            ("REVERSAL_PENALTY_INTEREST", "penalty_interest_asset", "CREDIT", "Reversal of penalty interest", "EVENT"),

            # 14–16. Default interest (accrual, payment, recognise income, reversal)
            ("PAYMENT_DEFAULT_INTEREST", "cash_operating", "DEBIT", "Payment of default interest (15)", "EVENT"),
            ("PAYMENT_DEFAULT_INTEREST", "default_interest_asset", "CREDIT", "Payment of default interest (15)", "EVENT"),
            ("PAYMENT_DEFAULT_INTEREST", "default_interest_suspense", "DEBIT", "Recognise default income (15a)", "EVENT"),
            ("PAYMENT_DEFAULT_INTEREST", "default_interest_income", "CREDIT", "Recognise default income (15a)", "EVENT"),
            ("REVERSAL_DEFAULT_INTEREST", "default_interest_income", "DEBIT", "Reversal of default interest", "EVENT"),
            ("REVERSAL_DEFAULT_INTEREST", "default_interest_asset", "CREDIT", "Reversal of default interest", "EVENT"),

            # 17–22. Regular interest in suspense / reversals (configured for completeness)
            ("ACCRUAL_REGULAR_INTEREST_SUSPENSE", "regular_interest_accrued", "DEBIT", "Daily accrual of regular interest into suspense", "EOD"),
            ("ACCRUAL_REGULAR_INTEREST_SUSPENSE", "regular_interest_suspense", "CREDIT", "Daily accrual of regular interest into suspense", "EOD"),
            ("PAYMENT_REGULAR_INTEREST_SUSPENSE", "cash_operating", "DEBIT", "Payment of regular interest in suspense", "EVENT"),
            ("PAYMENT_REGULAR_INTEREST_SUSPENSE", "regular_interest_suspense", "CREDIT", "Payment of regular interest in suspense", "EVENT"),
            ("PAYMENT_REGULAR_INTEREST_ACCRUED", "cash_operating", "DEBIT", "Payment of regular interest accrued (unbilled)", "EVENT"),
            ("PAYMENT_REGULAR_INTEREST_ACCRUED", "regular_interest_accrued", "CREDIT", "Payment of regular interest accrued (unbilled)", "EVENT"),
            ("RECOGNISE_REGULAR_INTEREST_INCOME", "regular_interest_income_holding", "DEBIT", "Recognise regular interest income", "EVENT"),
            ("RECOGNISE_REGULAR_INTEREST_INCOME", "regular_interest_income", "CREDIT", "Recognise regular interest income", "EVENT"),
            ("REVERSAL_REGULAR_INTEREST_ACCRUAL", "regular_interest_income_holding", "DEBIT", "Reversal of regular interest accrual", "EVENT"),
            ("REVERSAL_REGULAR_INTEREST_ACCRUAL", "regular_interest_accrued", "CREDIT", "Reversal of regular interest accrual", "EVENT"),

            # 23–24. Provisions (impairment)
            ("PROVISION_RAISE", "impairment_loss_expense", "DEBIT", "Raising a provision (increasing risk)", "EOM"),
            ("PROVISION_RAISE", "allowance_credit_losses", "CREDIT", "Raising a provision (increasing risk)", "EOM"),
            ("PROVISION_REVERSAL", "allowance_credit_losses", "DEBIT", "Reversing a provision (quality improves)", "EOM"),
            ("PROVISION_REVERSAL", "impairment_loss_expense", "CREDIT", "Reversing a provision (quality improves)", "EOM"),

            # 25–27. Write-offs and recoveries
            ("PRINCIPAL_WRITEOFF", "allowance_credit_losses", "DEBIT", "Principal write-off (final loss)", "EVENT"),
            ("PRINCIPAL_WRITEOFF", "loan_principal", "CREDIT", "Principal write-off (final loss)", "EVENT"),
            ("INTEREST_WRITEOFF", "bad_debts_expense", "DEBIT", "Interest write-off (no suspense balance)", "EVENT"),
            ("INTEREST_WRITEOFF", "regular_interest_arrears", "CREDIT", "Interest write-off (no suspense balance)", "EVENT"),
            ("WRITEOFF_RECOVERY", "cash_operating", "DEBIT", "Receipt from a fully written-off loan", "EVENT"),
            ("WRITEOFF_RECOVERY", "bad_debts_recovered", "CREDIT", "Receipt from a fully written-off loan", "EVENT"),

            # 28–28b. Restructuring (high-level configured for completeness)
            ("LOAN_RESTRUCTURE_CAPITALISE", "loan_principal", "DEBIT", "Capitalisation of interest and arrears (restructure)", "EVENT"),
            ("LOAN_RESTRUCTURE_CAPITALISE", "principal_arrears", "CREDIT", "Capitalisation of principal arrears (restructure)", "EVENT"),
            ("LOAN_RESTRUCTURE_CAPITALISE", "regular_interest_arrears", "CREDIT", "Capitalisation of regular interest arrears (restructure)", "EVENT"),
            ("LOAN_RESTRUCTURE_CAPITALISE", "penalty_interest_asset", "CREDIT", "Capitalisation of penalty interest (restructure)", "EVENT"),
            ("LOAN_RESTRUCTURE_CAPITALISE", "default_interest_asset", "CREDIT", "Capitalisation of default interest (restructure)", "EVENT"),
            ("LOAN_RESTRUCTURE_CAPITALISE", "fees_charges_arrears", "CREDIT", "Capitalisation of fees and charges arrears (restructure)", "EVENT"),
            ("RESTRUCTURE_FEE_CHARGE", "loan_principal", "DEBIT", "Restructure fee (charged to customer)", "EVENT"),
            ("RESTRUCTURE_FEE_CHARGE", "deferred_fee_liability", "CREDIT", "Restructure fee (deferred)", "EVENT"),
            ("RESTRUCTURE_FEE_AMORTISATION", "deferred_fee_liability", "DEBIT", "Restructure fee amortisation (monthly)", "EOM"),
            ("RESTRUCTURE_FEE_AMORTISATION", "deferred_fee_income", "CREDIT", "Restructure fee amortisation (monthly)", "EOM"),

            # 29–33. Pass-through costs / commission (configured for completeness)
            ("PASS_THROUGH_COST_DISBURSEMENT", "deferred_fee_commission_asset", "DEBIT", "Payment of third-party cost (pass-through)", "EVENT"),
            ("PASS_THROUGH_COST_DISBURSEMENT", "cash_operating", "CREDIT", "Payment of third-party cost (pass-through)", "EVENT"),
            ("PASS_THROUGH_COST_RECOVERY", "cash_operating", "DEBIT", "Receipt of pass-through costs (recovery)", "EVENT"),
            ("PASS_THROUGH_COST_RECOVERY", "deferred_fee_commission_asset", "CREDIT", "Receipt of pass-through costs (recovery)", "EVENT"),
            ("FEES_CHARGES_WRITEOFF", "allowance_credit_losses", "DEBIT", "Fees and charges arrears write-off (final loss)", "EVENT"),
            ("FEES_CHARGES_WRITEOFF", "fees_charges_arrears", "CREDIT", "Fees and charges arrears write-off (final loss)", "EVENT"),
            ("AGENT_COMMISSION_PAYMENT", "fees_commission_expense", "DEBIT", "Payment of agent commission", "EVENT"),
            ("AGENT_COMMISSION_PAYMENT", "cash_operating", "CREDIT", "Payment of agent commission", "EVENT"),
            ("COMMISSION_AMORTISATION", "fees_commission_expense", "DEBIT", "Monthly amortisation of commission", "EOM"),
            ("COMMISSION_AMORTISATION", "deferred_fee_commission_asset", "CREDIT", "Monthly amortisation of commission", "EOM"),

            # 34–37. Borrowings (configured for completeness)
            ("BORROWING_DRAWDOWN", "cash_operating", "DEBIT", "Drawdown on a borrowing from a financier", "EVENT"),
            ("BORROWING_DRAWDOWN", "borrowings_loan_principal", "CREDIT", "Drawdown on a borrowing from a financier", "EVENT"),
            ("INTEREST_EXPENSE_ACCRUAL", "interest_expense", "DEBIT", "Monthly accrual of interest expense", "EOM"),
            ("INTEREST_EXPENSE_ACCRUAL", "interest_payable", "CREDIT", "Monthly accrual of interest expense", "EOM"),
            ("BORROWING_FEES_AMORTISATION", "amortization_borrowing_fees", "DEBIT", "Amortization of borrowing fees", "EOM"),
            ("BORROWING_FEES_AMORTISATION", "deferred_fee_asset_borrowings", "CREDIT", "Amortization of borrowing fees", "EOM"),
            ("BORROWING_REPAYMENT", "borrowings_loan_principal", "DEBIT", "Payment of borrowings", "EVENT"),
            ("BORROWING_REPAYMENT", "cash_operating", "CREDIT", "Payment of borrowings", "EVENT"),
        ]
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.initialize_default_transaction_templates(DEFAULT_TEMPLATES)
            return True
        finally:
            conn.close()

    def is_coa_initialized(self):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.is_coa_initialized()
        finally:
            conn.close()

    def initialize_default_coa(self):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            if not repo.is_coa_initialized():
                repo.initialize_default_coa()
                return True
            return False
        finally:
            conn.close()

    def list_accounts(self):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.list_accounts()
        finally:
            conn.close()

    def is_parent_account(self, account_code: str) -> bool:
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.is_parent_account(account_code)
        finally:
            conn.close()

    def get_child_account_summaries(self, parent_code: str, start_date: date, end_date: date):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.get_child_account_summaries(parent_code, start_date, end_date)
        finally:
            conn.close()

    def list_all_transaction_templates(self):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.list_all_transaction_templates()
        finally:
            conn.close()

    def link_journal(self, event_type, system_tag, direction, description, trigger_type="EVENT"):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.link_journal(event_type, system_tag, direction, description, trigger_type)
            return True
        finally:
            conn.close()

    def create_account(self, code, name, category, system_tag=None, parent_id=None):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            repo.create_account(code, name, category, system_tag, parent_id)
            return True
        finally:
            conn.close()

    def get_trial_balance(self, as_of_date: date = None):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.get_trial_balance(as_of_date)
        finally:
            conn.close()

    def get_journal_entries(self, start_date: date = None, end_date: date = None, account_code: str = None):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.get_journal_entries(start_date, end_date, account_code)
        finally:
            conn.close()

    def get_account_ledger(self, account_code: str, start_date: date = None, end_date: date = None):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            return repo.get_account_ledger(account_code, start_date, end_date)
        finally:
            conn.close()

    def get_profit_and_loss(self, start_date: date, end_date: date):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            # P&L is Income and Expense
            balances = repo.get_balances_by_category(['INCOME', 'EXPENSE'], start_date, end_date)
            return balances
        finally:
            conn.close()

    def get_balance_sheet(self, as_of_date: date):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            # Balance sheet is Asset, Liability, Equity
            balances = repo.get_balances_by_category(['ASSET', 'LIABILITY', 'EQUITY'], end_date=as_of_date)
            return balances
        finally:
            conn.close()

    def get_statement_of_changes_in_equity(self, start_date: date, end_date: date):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            # Equity categories
            balances = repo.get_balances_by_category(['EQUITY'], start_date, end_date)
            return balances
        finally:
            conn.close()

    def get_cash_flow_statement(self, start_date: date, end_date: date):
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            balances = repo.get_balances_by_category(['ASSET', 'LIABILITY'], start_date, end_date)
            pnl = repo.get_balances_by_category(['INCOME', 'EXPENSE'], start_date, end_date)
            return {"balances": balances, "pnl": pnl}
        finally:
            conn.close()

    def simulate_event(self, event_type: str, amount: Decimal = None, payload: dict = None, is_reversal: bool = False):
        if payload is None:
            payload = {}
        
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            templates = repo.get_transaction_templates(event_type)
            if not templates:
                return []
            
            lines = []
            for tmpl in templates:
                account = repo.get_account_by_tag(tmpl["system_tag"])
                account_name = account["name"] if account else f"Missing Account ({tmpl['system_tag']})"
                account_code = account["code"] if account else "???"
                
                line_amount = payload.get(tmpl["system_tag"], amount)
                if line_amount is None:
                    line_amount = Decimal("0.0")
                
                direction = tmpl["direction"]
                if is_reversal:
                    direction = "CREDIT" if direction == "DEBIT" else "DEBIT"
                
                debit = line_amount if direction == 'DEBIT' else Decimal("0.0")
                credit = line_amount if direction == 'CREDIT' else Decimal("0.0")
                
                if debit > 0 or credit > 0:
                    lines.append({
                        "account_name": account_name,
                        "account_code": account_code,
                        "debit": debit,
                        "credit": credit,
                        "memo": tmpl["description"]
                    })
            return lines
        finally:
            conn.close()

    def post_event(self, event_type: str, reference: str, description: str, event_id: str, created_by: str, entry_date: date = None, amount: Decimal = None, payload: dict = None, is_reversal: bool = False):
        if entry_date is None:
            entry_date = date.today()
        
        if payload is None:
            payload = {}
        
        conn = get_conn()
        try:
            repo = AccountingRepository(conn)
            templates = repo.get_transaction_templates(event_type)
            if not templates:
                return
            
            lines = []
            for tmpl in templates:
                account = repo.get_account_by_tag(tmpl["system_tag"])
                if not account:
                    raise ValueError(f"Account not found for system tag: {tmpl['system_tag']}")
                
                line_amount = payload.get(tmpl["system_tag"], amount)
                if line_amount is None:
                    line_amount = Decimal("0.0")
                
                direction = tmpl["direction"]
                if is_reversal:
                    direction = "CREDIT" if direction == "DEBIT" else "DEBIT"
                
                debit = line_amount if direction == 'DEBIT' else Decimal("0.0")
                credit = line_amount if direction == 'CREDIT' else Decimal("0.0")
                
                if debit > 0 or credit > 0:
                    lines.append({
                        "account_id": account["id"],
                        "debit": debit,
                        "credit": credit,
                        "memo": tmpl["description"] or description
                    })
            
            if lines:
                repo.save_journal_entry(entry_date, reference, description, event_id, event_type, created_by, lines)
        except Exception as e:
            raise e
        finally:
            conn.close()
