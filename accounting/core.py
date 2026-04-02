from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal, getcontext, ROUND_HALF_UP
from enum import Enum
from typing import Dict, List, Optional, Iterable, Tuple, Any, Set


# -----------------------------
# Decimal / money configuration
# -----------------------------

getcontext().prec = 28
getcontext().rounding = ROUND_HALF_UP

MONEY_QUANT = Decimal("0.01")


def as_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_QUANT)


# -----------------------------
# Core accounting primitives
# -----------------------------


class AccountCategory(str, Enum):
    ASSET = "ASSET"
    LIABILITY = "LIABILITY"
    EQUITY = "EQUITY"
    INCOME = "INCOME"
    EXPENSE = "EXPENSE"


class UserRole(str, Enum):
    ACCOUNTING_ADMIN = "ACCOUNTING_ADMIN"
    LOAN_OFFICER = "LOAN_OFFICER"


class JournalStatus(str, Enum):
    PENDING = "PENDING"
    POSTED = "POSTED"
    REVERSED = "REVERSED"


class EventStatus(str, Enum):
    QUEUED = "QUEUED"
    POSTED = "POSTED"
    FAILED = "FAILED"


class SystemEventTag(str, Enum):
    # Core loan lifecycle events (extendable)
    LOAN_DISBURSEMENT = "LOAN_DISBURSEMENT"
    DAILY_INTEREST_ACCRUAL = "DAILY_INTEREST_ACCRUAL"
    MONTHLY_INTEREST_ACCRUAL = "MONTHLY_INTEREST_ACCRUAL"
    DEFAULT_INTEREST_ACCRUAL = "DEFAULT_INTEREST_ACCRUAL"
    PENALTY_INTEREST_ACCRUAL = "PENALTY_INTEREST_ACCRUAL"
    PRINCIPAL_DUE = "PRINCIPAL_DUE"
    PAYMENT_RECEIVED = "PAYMENT_RECEIVED"
    INTEREST_SUSPENSION = "INTEREST_SUSPENSION"
    INTEREST_SUSPENSION_REVERSAL = "INTEREST_SUSPENSION_REVERSAL"
    PROVISIONING_ADJUSTMENT = "PROVISIONING_ADJUSTMENT"
    WRITE_OFF = "WRITE_OFF"
    WRITE_OFF_RECOVERY = "WRITE_OFF_RECOVERY"
    RESCHEDULING = "RESCHEDULING"
    FX_REVALUATION = "FX_REVALUATION"
    BACKDATED_ADJUSTMENT = "BACKDATED_ADJUSTMENT"


class PostingSide(str, Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


@dataclass
class Account:
    """
    Dynamic Chart of Accounts element.
    Categories are fixed; accounts are user-defined and mappable.
    """

    id: str
    name: str
    category: AccountCategory
    parent_id: Optional[str] = None
    is_active: bool = True

    # Optional dimensional segments (e.g. branch, product)
    branch: Optional[str] = None
    product_line: Optional[str] = None


# -----------------------------
# COA coding rules & helpers
# -----------------------------

# Prefix per class of account.
# Note: we use "C" for Equity to avoid clashing with Expense ("E").
ACCOUNT_PREFIX_BY_CATEGORY: Dict[AccountCategory, str] = {
    AccountCategory.ASSET: "A",
    AccountCategory.LIABILITY: "L",
    AccountCategory.EQUITY: "C",  # Capital / Equity
    AccountCategory.INCOME: "R",  # Revenue
    AccountCategory.EXPENSE: "E",
}

# Stem ranges per category (4-digit stem, inclusive).
ACCOUNT_STEM_RANGE_BY_CATEGORY: Dict[AccountCategory, Tuple[int, int]] = {
    AccountCategory.ASSET: (1000, 1999),
    AccountCategory.LIABILITY: (2000, 2999),
    AccountCategory.EQUITY: (3000, 3999),
    AccountCategory.INCOME: (4000, 4999),
    AccountCategory.EXPENSE: (5000, 5999),
}


def split_account_code(code: str) -> Tuple[str, int | None]:
    """
    Split a stored account code into (seven_char_base, grandchild_suffix_or_none).

    Grandchild format (visual sub-ledger): ``BASE-NN`` where BASE is exactly 7 characters
    and NN is 01–99. Example: ``A100001-03`` → base ``A100001``, suffix 3.
    Plain 7-char codes return (code, None).
    """
    s = (code or "").strip().upper()
    if not s:
        raise ValueError("Account code is empty.")
    if "-" not in s:
        return s, None
    base, suf_part = s.split("-", 1)
    if len(base) != 7:
        raise ValueError(
            f"Account code '{code}': grandchild form must use a 7-character base before '-'."
        )
    if len(suf_part) != 2 or not suf_part.isdigit():
        raise ValueError(
            f"Account code '{code}': grandchild suffix must be two digits (01-99) after '-'."
        )
    g = int(suf_part)
    if not (1 <= g <= 99):
        raise ValueError(f"Account code '{code}': grandchild suffix must be between 01 and 99.")
    return base, g


def coa_grandchild_prefix_matches_immediate_parent(
    *, child_code: str, parent_code: str | None
) -> Tuple[bool, str | None]:
    """
    For grandchild codes ``BASE-NN``, **BASE** must equal the **immediate parent's** 7-character
    account code. Otherwise the code lies about which COA branch the row belongs to (e.g.
    ``A100001-02`` under parent ``A120001`` — ``A100001`` is the bank operating stem, not interest accrued).

    Plain 7-character child codes are not checked here (separate rollup rules apply).
    """
    cc = (child_code or "").strip().upper()
    if not cc:
        return False, "Account code is empty."
    try:
        base, suff = split_account_code(cc)
    except ValueError as exc:
        return False, str(exc)
    if suff is None:
        return True, None
    if not parent_code or not str(parent_code).strip():
        return False, (
            f"Grandchild code {cc!r} requires an immediate parent row whose code is the 7-character base {base!r}."
        )
    pc = str(parent_code).strip().upper()
    try:
        _pb, ps = split_account_code(pc)
    except ValueError:
        return False, f"Invalid parent code {parent_code!r} for child {cc!r}."
    if ps is not None:
        return (
            False,
            f"Grandchild {cc!r} must sit under a 7-character parent, not under another grandchild {pc!r}.",
        )
    if _pb != base:
        return (
            False,
            f"COA mismatch: code {cc!r} implies parent {base!r}, but immediate parent code is {pc!r}. "
            f"Rename to {pc}-{{NN:02d}} or move the row under account {base!r}.",
        )
    return True, None


def assert_coa_grandchild_matches_parent(*, child_code: str, parent_code: str | None) -> None:
    ok, msg = coa_grandchild_prefix_matches_immediate_parent(
        child_code=child_code, parent_code=parent_code
    )
    if not ok:
        raise ValueError(msg or "Grandchild account code does not match immediate parent in COA.")


def account_chain_code_parent_consistent(
    *, leaf_account_id: str, by_id: dict[str, dict]
) -> Tuple[bool, str | None]:
    """
    Walk from leaf to root via ``parent_id`` and ensure every step satisfies
    ``coa_grandchild_prefix_matches_immediate_parent`` (so ``A100001-02`` cannot hang under ``A120001``).
    """
    cur: dict | None = by_id.get(str(leaf_account_id))
    guard = 0
    while cur is not None and guard < 64:
        pid = cur.get("parent_id")
        if pid is None:
            return True, None
        parent = by_id.get(str(pid))
        if not parent:
            return False, f"missing parent row for account {cur.get('code')!r}"
        ok, msg = coa_grandchild_prefix_matches_immediate_parent(
            child_code=str(cur.get("code") or ""),
            parent_code=str(parent.get("code") or "").strip() or None,
        )
        if not ok:
            return False, msg
        cur = parent
        guard += 1
    return True, None


def build_coa_path_label(*, leaf_account_id: str, by_id: dict[str, dict]) -> Tuple[str, bool]:
    """
    Human-readable path for UI. If any parent/child code link is inconsistent, returns
    ``"{code} — {name} [COA: …]"`` instead of a misleading ``A120001 › A100001-02`` trail.
    """
    leaf = by_id.get(str(leaf_account_id))
    if not leaf:
        return "?", False
    lc = str(leaf.get("code") or "").strip()
    ln = str(leaf.get("name") or "").strip()
    base = f"{lc} — {ln}" if ln else lc
    ok, msg = account_chain_code_parent_consistent(leaf_account_id=str(leaf_account_id), by_id=by_id)
    if not ok:
        hint = (msg or "code/parent mismatch").replace("\n", " ")
        return f"{base} [COA: {hint}]", False
    parts: list[str] = []
    cur: dict | None = leaf
    guard = 0
    while cur is not None and guard < 64:
        parts.append(str(cur.get("code") or "").strip() or "?")
        pid = cur.get("parent_id")
        if pid is None:
            break
        parent = by_id.get(str(pid))
        if not parent:
            return f"{base} [COA: broken hierarchy]", False
        cur = parent
        guard += 1
    parts.reverse()
    trail = " › ".join(parts)
    return (f"{trail} — {ln}" if ln else trail), True


def parse_seven_char_account_code(seven: str) -> Tuple[str, int, int]:
    """
    Parse a strict 7-character account code into (prefix, stem, suffix_two_digit_int).
    Suffix 0 means ..00 (rollup parent); 1–99 means ..01–..99.
    """
    if len(seven) != 7:
        raise ValueError(f"Account code '{seven}' must be exactly 7 characters.")

    prefix = seven[0]
    body = seven[1:]
    stem_str = body[:4]
    suffix_str = body[4:]

    if not (stem_str.isdigit() and suffix_str.isdigit()):
        raise ValueError(f"Account code '{seven}' must have numeric stem and suffix.")

    stem = int(stem_str)
    suffix = int(suffix_str)
    return prefix, stem, suffix


def parse_account_code(code: str) -> Tuple[str, int, int]:
    """
    Split **7-character** codes only (same as parse_seven_char_account_code).

    For grandchild codes like ``A100001-01``, use ``split_account_code`` and parse the base:
    ``parse_seven_char_account_code(split_account_code(code)[0])``.
    """
    base, grand = split_account_code(code)
    if grand is not None:
        raise ValueError(
            f"Account code '{code}' uses a grandchild suffix (-NN). "
            f"Use split_account_code() for the base, or parse_seven_char_account_code() on the 7-char base only."
        )
    return parse_seven_char_account_code(base)


def validate_account_code(account: Account, accounts: Dict[str, Account]) -> None:
    """
    Enforce COA coding rules:
    - Prefix must match account category.
    - Stem must fall within category range.
    - Suffix 00 for parents, 01-99 for children (7-char codes).
    - Grandchild codes ``BASE-NN``: parent account id in registry must equal BASE (same category).
    - Category consistency between parent and child.
    """
    code = account.id
    base, grand = split_account_code(code)
    if grand is not None:
        prefix, stem, suff7 = parse_seven_char_account_code(base)
        expected_prefix = ACCOUNT_PREFIX_BY_CATEGORY[account.category]
        if prefix != expected_prefix:
            raise ValueError(
                f"Account {code}: prefix '{prefix}' does not match expected "
                f"'{expected_prefix}' for category {account.category}."
            )
        stem_min, stem_max = ACCOUNT_STEM_RANGE_BY_CATEGORY[account.category]
        if not (stem_min <= stem <= stem_max):
            raise ValueError(
                f"Account {code}: stem {stem} must be between {stem_min} and {stem_max} "
                f"for category {account.category}."
            )
        parent = accounts.get(account.parent_id) if account.parent_id else None
        if parent is None:
            raise ValueError(
                f"Account {code}: grandchild account must reference a parent in the registry."
            )
        if parent.id != base:
            raise ValueError(
                f"Account {code}: parent account id must be the 7-char base '{base}' for grandchild codes."
            )
        if parent.category != account.category:
            raise ValueError(
                f"Account {code}: category {account.category} does not match parent "
                f"{parent.id} category {parent.category}."
            )
        return

    prefix, stem, suffix = parse_seven_char_account_code(base)

    expected_prefix = ACCOUNT_PREFIX_BY_CATEGORY[account.category]
    if prefix != expected_prefix:
        raise ValueError(
            f"Account {code}: prefix '{prefix}' does not match expected "
            f"'{expected_prefix}' for category {account.category}."
        )

    stem_min, stem_max = ACCOUNT_STEM_RANGE_BY_CATEGORY[account.category]
    if not (stem_min <= stem <= stem_max):
        raise ValueError(
            f"Account {code}: stem {stem} must be between {stem_min} and {stem_max} "
            f"for category {account.category}."
        )

    if account.parent_id is None:
        # Parent accounts must end with suffix 00
        if suffix != 0:
            raise ValueError(
                f"Account {code}: parent accounts must have suffix '00'."
            )
    else:
        # Child accounts must have suffix 01-99
        if suffix == 0 or not (1 <= suffix <= 99):
            raise ValueError(
                f"Account {code}: child accounts must have suffix between '01' and '99'."
            )
        parent = accounts.get(account.parent_id)
        if parent is None:
            raise ValueError(
                f"Account {code}: parent account {account.parent_id} does not exist."
            )
        if parent.category != account.category:
            raise ValueError(
                f"Account {code}: category {account.category} does not match parent "
                f"{parent.id} category {parent.category}."
            )


def suggest_next_parent_account_code(
    category: AccountCategory, accounts: Dict[str, Account], step: int = 10
) -> str:
    """
    Suggest the next available parent account code within the category range,
    leaving gaps (e.g. 1010, 1020, 1030) for future inserts.
    """
    prefix = ACCOUNT_PREFIX_BY_CATEGORY[category]
    stem_min, stem_max = ACCOUNT_STEM_RANGE_BY_CATEGORY[category]

    used_stems = set()
    for acc in accounts.values():
        if acc.category != category or acc.parent_id is not None:
            continue
        b, g = split_account_code(acc.id)
        if g is not None:
            continue
        p, stem, _ = parse_seven_char_account_code(b)
        if p == prefix:
            used_stems.add(stem)

    for stem in range(stem_min, stem_max + 1, step):
        if stem not in used_stems:
            return f"{prefix}{stem:04d}00"

    raise ValueError(f"No available stems left for category {category}.")


def suggest_next_child_account_code(
    parent_account_id: str, accounts: Dict[str, Account]
) -> str:
    """
    Suggest the next available **7-character** child code under a given parent,
    using suffixes 01-99 on the parent's stem.

    ``parent_account_id`` is the parent's **id** in the registry (typically the 7-char code).
    Best suited when the parent is a rollup (suffix 00); for intermediate nodes (e.g. A100001),
    prefer ``suggest_next_grandchild_account_code`` instead to avoid code collisions.
    """
    b, g = split_account_code(parent_account_id)
    if g is not None:
        raise ValueError("suggest_next_child_account_code: parent must be a 7-character code.")
    prefix, stem, _ = parse_seven_char_account_code(b)
    existing_suffixes = set()

    for acc in accounts.values():
        if acc.parent_id != parent_account_id:
            continue
        ab, ag = split_account_code(acc.id)
        if ag is not None:
            continue
        p, s, suffix = parse_seven_char_account_code(ab)
        if p == prefix and s == stem:
            existing_suffixes.add(suffix)

    for suffix in range(1, 100):
        if suffix not in existing_suffixes:
            return f"{prefix}{stem:04d}{suffix:02d}"

    raise ValueError(f"No child suffixes left under parent {parent_account_id}.")


def suggest_next_grandchild_account_code(parent_base_code: str, existing_codes: Iterable[str]) -> str:
    """
    Next grandchild code ``{parent_base_code}-NN`` not present in ``existing_codes``.
    ``parent_base_code`` must be exactly 7 characters (the immediate parent account code).
    """
    b, g = split_account_code(parent_base_code)
    if g is not None:
        raise ValueError("Grandchild parent must be a 7-character base code.")
    parse_seven_char_account_code(b)
    used: Set[int] = set()
    prefix = f"{b}-"
    for raw in existing_codes:
        try:
            bb, gg = split_account_code(str(raw).strip())
        except ValueError:
            continue
        if gg is not None and bb == b:
            used.add(gg)
    for n in range(1, 100):
        if n not in used:
            return f"{b}-{n:02d}"
    raise ValueError(f"No grandchild suffixes left under parent code {b}.")


@dataclass
class JournalLine:
    account_id: str
    debit: Decimal = Decimal("0")
    credit: Decimal = Decimal("0")
    transaction_currency: str = "USD"
    base_currency: str = "USD"
    fx_rate: Decimal = Decimal("1")  # transaction -> base
    memo: Optional[str] = None

    def __post_init__(self) -> None:
        self.debit = as_money(self.debit)
        self.credit = as_money(self.credit)
        if self.debit > 0 and self.credit > 0:
            raise ValueError("JournalLine cannot have both debit and credit amounts.")

    @property
    def debit_base(self) -> Decimal:
        return as_money(self.debit * self.fx_rate)

    @property
    def credit_base(self) -> Decimal:
        return as_money(self.credit * self.fx_rate)


@dataclass
class JournalEntry:
    """
    Immutable once posted: do not mutate lines or amounts after posting.
    Use create_reversing_entry to correct mistakes.
    """

    id: str
    entry_date: date
    created_at: datetime
    created_by: str
    status: JournalStatus
    lines: List[JournalLine]
    description: Optional[str] = None
    event_id: Optional[str] = None
    event_tag: Optional[SystemEventTag] = None
    reversal_of_id: Optional[str] = None

    def total_debits(self) -> Decimal:
        return sum((l.debit_base for l in self.lines), Decimal("0"))

    def total_credits(self) -> Decimal:
        return sum((l.credit_base for l in self.lines), Decimal("0"))


def create_reversing_entry(original: JournalEntry, new_id: str, user_id: str) -> JournalEntry:
    """
    Generate a full reversing entry for an existing posted entry.
    """
    if original.status != JournalStatus.POSTED:
        raise ValueError("Only POSTED journal entries can be reversed.")

    reversed_lines = [
        JournalLine(
            account_id=l.account_id,
            debit=l.credit,
            credit=l.debit,
            transaction_currency=l.transaction_currency,
            base_currency=l.base_currency,
            fx_rate=l.fx_rate,
            memo=f"Reversal of line in entry {original.id}",
        )
        for l in original.lines
    ]

    return JournalEntry(
        id=new_id,
        entry_date=date.today(),
        created_at=datetime.utcnow(),
        created_by=user_id,
        status=JournalStatus.POSTED,
        lines=reversed_lines,
        description=f"Reversing entry for {original.id}",
        event_id=None,
        event_tag=original.event_tag,
        reversal_of_id=original.id,
    )


# -----------------------------
# Mapping engine
# -----------------------------


class MappingCategory(str, Enum):
    """
    Semantic tags that link loan events to accounting roles.
    Each mapping can be validated against expected AccountCategory.
    """

    # Examples (extend as needed)
    PRINCIPAL_RECEIVABLE = "PRINCIPAL_RECEIVABLE"  # Asset
    INTEREST_RECEIVABLE = "INTEREST_RECEIVABLE"  # Asset
    DEFAULT_INTEREST_RECEIVABLE = "DEFAULT_INTEREST_RECEIVABLE"  # Asset
    PENALTY_INTEREST_RECEIVABLE = "PENALTY_INTEREST_RECEIVABLE"  # Asset
    INTEREST_INCOME = "INTEREST_INCOME"  # Income
    DEFAULT_INTEREST_INCOME = "DEFAULT_INTEREST_INCOME"  # Income
    PENALTY_INTEREST_INCOME = "PENALTY_INTEREST_INCOME"  # Income
    FEE_INCOME = "FEE_INCOME"  # Income
    CASH_AT_BANK = "CASH_AT_BANK"  # Asset
    LOAN_PRINCIPAL_OUTSTANDING = "LOAN_PRINCIPAL_OUTSTANDING"  # Asset
    PROVISION_ACCOUNT = "PROVISION_ACCOUNT"  # Expense/Liability depending on policy


EXPECTED_CATEGORY_BY_MAPPING: Dict[MappingCategory, AccountCategory] = {
    MappingCategory.PRINCIPAL_RECEIVABLE: AccountCategory.ASSET,
    MappingCategory.INTEREST_RECEIVABLE: AccountCategory.ASSET,
    MappingCategory.DEFAULT_INTEREST_RECEIVABLE: AccountCategory.ASSET,
    MappingCategory.PENALTY_INTEREST_RECEIVABLE: AccountCategory.ASSET,
    MappingCategory.INTEREST_INCOME: AccountCategory.INCOME,
    MappingCategory.DEFAULT_INTEREST_INCOME: AccountCategory.INCOME,
    MappingCategory.PENALTY_INTEREST_INCOME: AccountCategory.INCOME,
    MappingCategory.FEE_INCOME: AccountCategory.INCOME,
    MappingCategory.CASH_AT_BANK: AccountCategory.ASSET,
    MappingCategory.LOAN_PRINCIPAL_OUTSTANDING: AccountCategory.ASSET,
    MappingCategory.PROVISION_ACCOUNT: AccountCategory.EXPENSE,
}


@dataclass
class EventAccountMapping:
    """
    Configuration row that links a SystemEventTag + logical role to a concrete account.
    Example:
        LOAN_DISBURSEMENT, DEBIT, CASH_AT_BANK -> 1010 (Cash & Bank)
        LOAN_DISBURSEMENT, CREDIT, LOAN_PRINCIPAL_OUTSTANDING -> 1300 (Loans)
    """

    event_tag: SystemEventTag
    side: PostingSide
    mapping_category: MappingCategory
    account_id: str


@dataclass
class MappingChangeAudit:
    timestamp: datetime
    user_id: str
    old_mapping: Optional[EventAccountMapping]
    new_mapping: Optional[EventAccountMapping]


@dataclass
class MappingRegistry:
    """
    Holds event-to-account mappings and validates them against the chart of accounts.
    """

    accounts: Dict[str, Account] = field(default_factory=dict)
    mappings: List[EventAccountMapping] = field(default_factory=list)
    mapping_audit_log: List[MappingChangeAudit] = field(default_factory=list)

    def add_or_update_account(self, account: Account) -> None:
        """
        Add or update an account, enforcing COA coding rules.
        """
        validate_account_code(account, self.accounts)
        # Soft-delete rule: do not overwrite existing is_active flags if account has history.
        self.accounts[account.id] = account

    def soft_deactivate_account(self, account_id: str) -> None:
        if account_id not in self.accounts:
            raise KeyError(f"Account {account_id} not found.")
        self.accounts[account_id].is_active = False

    def add_or_update_mapping(
        self, mapping: EventAccountMapping, user_id: str
    ) -> None:
        expected_category = EXPECTED_CATEGORY_BY_MAPPING.get(mapping.mapping_category)
        account = self.accounts.get(mapping.account_id)
        if account is None:
            raise ValueError(f"Account {mapping.account_id} does not exist.")
        if expected_category and account.category != expected_category:
            raise ValueError(
                f"Account {mapping.account_id} category {account.category} "
                f"does not match expected {expected_category} for {mapping.mapping_category}."
            )

        old = None
        for i, existing in enumerate(self.mappings):
            if (
                existing.event_tag == mapping.event_tag
                and existing.side == mapping.side
                and existing.mapping_category == mapping.mapping_category
            ):
                old = existing
                self.mappings[i] = mapping
                break
        if old is None:
            self.mappings.append(mapping)

        self.mapping_audit_log.append(
            MappingChangeAudit(
                timestamp=datetime.utcnow(), user_id=user_id, old_mapping=old, new_mapping=mapping
            )
        )

    def resolve_accounts(
        self, event_tag: SystemEventTag
    ) -> List[EventAccountMapping]:
        return [m for m in self.mappings if m.event_tag == event_tag]


# -----------------------------
# Event queue and posting engine
# -----------------------------


@dataclass
class AccountingEvent:
    """
    Loan and other modules enqueue high-level events here.
    They do NOT refer to concrete account_ids, only tags and amounts.
    """

    id: str
    tag: SystemEventTag
    event_date: date
    created_at: datetime
    created_by: str
    payload: Dict[str, Any]
    status: EventStatus = EventStatus.QUEUED
    error_message: Optional[str] = None
    backdated_reason: Optional[str] = None


@dataclass
class PostingEngine:
    """
    Responsible for:
    - Translating events into balanced journal entries using mappings.
    - Enforcing zero-sum constraint, immutability, and safety rules.
    """

    mapping_registry: MappingRegistry
    journal_entries: Dict[str, JournalEntry] = field(default_factory=dict)
    processed_event_ids: Set[str] = field(default_factory=set)

    def _ensure_permissions(self, user_role: UserRole) -> None:
        if user_role not in (UserRole.ACCOUNTING_ADMIN, UserRole.LOAN_OFFICER):
            raise PermissionError("User not permitted to post accounting entries.")

    # ---- Pre-posting simulation ----

    def simulate_posting(
        self, event: AccountingEvent
    ) -> List[JournalLine]:
        """
        Dry-run: compute journal lines that WOULD be posted for this event,
        without mutating state.
        """
        lines, _ = self._build_journal_lines_for_event(event)
        self._assert_balanced(lines)
        return lines

    # ---- Posting ----

    def post_event(
        self,
        event: AccountingEvent,
        entry_id: str,
        user_role: UserRole,
    ) -> JournalEntry:
        """
        Atomically post a journal entry for a given event.
        Either the full entry is created and stored, or an exception is raised and nothing changes.
        """
        self._ensure_permissions(user_role)

        if event.id in self.processed_event_ids:
            raise ValueError(f"Event {event.id} has already been posted (duplicate detected).")

        lines, description = self._build_journal_lines_for_event(event)
        self._assert_balanced(lines)

        entry = JournalEntry(
            id=entry_id,
            entry_date=event.event_date,
            created_at=datetime.utcnow(),
            created_by=event.created_by,
            status=JournalStatus.POSTED,
            lines=lines,
            description=description,
            event_id=event.id,
            event_tag=event.tag,
        )

        # "Atomic" within this in-memory model: only mutate state after validation passes
        self.journal_entries[entry.id] = entry
        self.processed_event_ids.add(event.id)
        event.status = EventStatus.POSTED
        return entry

    # ---- Helpers ----

    def _build_journal_lines_for_event(
        self, event: AccountingEvent
    ) -> Tuple[List[JournalLine], str]:
        """
        Translate a high-level event into journal lines using the mapping registry.
        The payload is interpreted according to event.tag.
        """
        mappings = self.mapping_registry.resolve_accounts(event.tag)
        if not mappings:
            raise ValueError(f"No mappings configured for event tag {event.tag}.")

        amount: Decimal = as_money(Decimal(str(event.payload.get("amount", "0"))))
        currency: str = event.payload.get("currency", "USD")
        base_currency: str = event.payload.get("base_currency", currency)
        fx_rate = Decimal(str(event.payload.get("fx_rate", "1")))

        # Basic pattern: positive amount is magnitude; side is defined by mapping rows
        lines: List[JournalLine] = []
        description = event.payload.get("description", event.tag.value)

        for mapping in mappings:
            if mapping.side == PostingSide.DEBIT:
                debit = amount
                credit = Decimal("0")
            else:
                debit = Decimal("0")
                credit = amount

            lines.append(
                JournalLine(
                    account_id=mapping.account_id,
                    debit=debit,
                    credit=credit,
                    transaction_currency=currency,
                    base_currency=base_currency,
                    fx_rate=fx_rate,
                    memo=description,
                )
            )

        return lines, description

    def _assert_balanced(self, lines: Iterable[JournalLine]) -> None:
        total_debits = sum((l.debit_base for l in lines), Decimal("0"))
        total_credits = sum((l.credit_base for l in lines), Decimal("0"))
        if total_debits != total_credits:
            raise AssertionError(
                f"Unbalanced journal: debits {total_debits} != credits {total_credits}"
            )


# -----------------------------
# Reporting helpers
# -----------------------------


def trial_balance(entries: Iterable[JournalEntry]) -> Dict[str, Dict[str, Decimal]]:
    """
    Aggregate debits and credits per account in base currency.
    Returns {account_id: {"debit": x, "credit": y}}.
    """
    totals: Dict[str, Dict[str, Decimal]] = {}
    for entry in entries:
        if entry.status != JournalStatus.POSTED:
            continue
        for line in entry.lines:
            acc = totals.setdefault(
                line.account_id, {"debit": Decimal("0"), "credit": Decimal("0")}
            )
            acc["debit"] += line.debit_base
            acc["credit"] += line.credit_base
    return totals


def balance_sheet(
    entries: Iterable[JournalEntry],
    accounts: Dict[str, Account],
) -> Dict[AccountCategory, Decimal]:
    """
    Summarise balances by category in base currency.
    Returns {ASSET: x, LIABILITY: y, EQUITY: z}.
    """
    tb = trial_balance(entries)
    category_totals: Dict[AccountCategory, Decimal] = {
        c: Decimal("0") for c in AccountCategory
    }

    for account_id, sums in tb.items():
        account = accounts.get(account_id)
        if account is None:
            continue

        balance = sums["debit"] - sums["credit"]
        category_totals[account.category] += balance

    return category_totals


def income_statement(
    entries: Iterable[JournalEntry],
    accounts: Dict[str, Account],
    start_date: date,
    end_date: date,
) -> Dict[str, Decimal]:
    """
    Summarise income and expenses for the given period.
    Returns {"income": x, "expense": y, "net_income": x - y}.
    """
    income_total = Decimal("0")
    expense_total = Decimal("0")

    for entry in entries:
        if entry.status != JournalStatus.POSTED:
            continue
        if not (start_date <= entry.entry_date <= end_date):
            continue

        for line in entry.lines:
            account = accounts.get(line.account_id)
            if account is None:
                continue
            balance = line.credit_base - line.debit_base

            if account.category == AccountCategory.INCOME:
                income_total += balance
            elif account.category == AccountCategory.EXPENSE:
                expense_total += balance

    return {
        "income": income_total,
        "expense": expense_total,
        "net_income": income_total - expense_total,
    }


def rollup_by_parent(
    entries: Iterable[JournalEntry],
    accounts: Dict[str, Account],
) -> Dict[str, Decimal]:
    """
    Roll up balances from child accounts to their parents.
    Returns {account_id_or_parent_id: balance}.
    """
    tb = trial_balance(entries)
    balances: Dict[str, Decimal] = {}

    for account_id, sums in tb.items():
        account = accounts.get(account_id)
        if account is None:
            continue

        balance = sums["debit"] - sums["credit"]
        target_id = account.parent_id or account_id
        balances[target_id] = balances.get(target_id, Decimal("0")) + balance

    return balances


__all__ = [
    # Core types
    "Account",
    "AccountCategory",
    "UserRole",
    "JournalLine",
    "JournalEntry",
    "JournalStatus",
    "SystemEventTag",
    "PostingSide",
    "AccountingEvent",
    "EventStatus",
    # COA helpers
    "ACCOUNT_PREFIX_BY_CATEGORY",
    "ACCOUNT_STEM_RANGE_BY_CATEGORY",
    "split_account_code",
    "coa_grandchild_prefix_matches_immediate_parent",
    "assert_coa_grandchild_matches_parent",
    "account_chain_code_parent_consistent",
    "build_coa_path_label",
    "parse_seven_char_account_code",
    "parse_account_code",
    "validate_account_code",
    "suggest_next_parent_account_code",
    "suggest_next_child_account_code",
    "suggest_next_grandchild_account_code",
    # Mapping
    "MappingCategory",
    "EventAccountMapping",
    "MappingRegistry",
    # Engine
    "PostingEngine",
    "create_reversing_entry",
    # Reporting
    "trial_balance",
    "balance_sheet",
    "income_statement",
    "rollup_by_parent",
]

