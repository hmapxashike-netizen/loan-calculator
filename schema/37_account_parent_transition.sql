-- 37_account_parent_transition.sql
-- 
-- Enhancements for dynamic parent/child account hierarchies:
-- - Add is_parent and transitioned_to_parent_at to accounts.
-- - Enforce no cycles in the accounts.parent_id hierarchy.
-- - Prevent postings to parent accounts after they transition to parent mode.
-- - Provide a convert_to_parent(account_id) helper.

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS is_parent BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS transitioned_to_parent_at TIMESTAMPTZ NULL;

-- Backfill is_parent for any account that currently has children.
UPDATE accounts p
SET is_parent = TRUE
WHERE EXISTS (
    SELECT 1
    FROM accounts c
    WHERE c.parent_id = p.id
)
AND p.is_parent = FALSE;

-- Ensure that if transitioned_to_parent_at is set, is_parent must be TRUE.
ALTER TABLE accounts
    ADD CONSTRAINT IF NOT EXISTS accounts_transition_implies_parent
    CHECK (
        transitioned_to_parent_at IS NULL
        OR is_parent = TRUE
    );


-- ---------------------------------------------------------------------------
-- Prevent circular references in accounts.parent_id using a recursive CTE.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION prevent_account_cycles()
RETURNS TRIGGER AS $$
DECLARE
    cycle_found BOOLEAN;
BEGIN
    -- If parent_id is null, no cycle possible.
    IF NEW.parent_id IS NULL THEN
        RETURN NEW;
    END IF;

    -- If parent_id unchanged on update, do nothing.
    IF TG_OP = 'UPDATE' AND NEW.parent_id = OLD.parent_id THEN
        RETURN NEW;
    END IF;

    -- Walk up the ancestor chain from the proposed parent.
    WITH RECURSIVE ancestors AS (
        SELECT id, parent_id
        FROM accounts
        WHERE id = NEW.parent_id

        UNION ALL

        SELECT a.id, a.parent_id
        FROM accounts a
        JOIN ancestors an ON a.id = an.parent_id
    )
    SELECT EXISTS (
        SELECT 1 FROM ancestors WHERE id = NEW.id
    ) INTO cycle_found;

    IF cycle_found THEN
        RAISE EXCEPTION
            'Circular reference detected: account % cannot be parent/ancestor of itself.',
            NEW.id
            USING ERRCODE = '23514'; -- check_violation
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_account_cycles_ins ON accounts;
DROP TRIGGER IF EXISTS trg_prevent_account_cycles_upd ON accounts;

CREATE TRIGGER trg_prevent_account_cycles_ins
BEFORE INSERT ON accounts
FOR EACH ROW
EXECUTE FUNCTION prevent_account_cycles();

CREATE TRIGGER trg_prevent_account_cycles_upd
BEFORE UPDATE OF parent_id ON accounts
FOR EACH ROW
EXECUTE FUNCTION prevent_account_cycles();


-- ---------------------------------------------------------------------------
-- Prevent postings to parent accounts after they transition to parent mode.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION prevent_post_to_parent_after_transition()
RETURNS TRIGGER AS $$
DECLARE
    acc RECORD;
    je  RECORD;
BEGIN
    -- Load account and journal entry for the new line.
    SELECT * INTO acc FROM accounts WHERE id = NEW.account_id;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;

    SELECT * INTO je FROM journal_entries WHERE id = NEW.entry_id;
    IF NOT FOUND THEN
        RETURN NEW;
    END IF;

    -- If account is a parent and has transitioned, block postings after the transition timestamp.
    IF acc.is_parent
       AND acc.transitioned_to_parent_at IS NOT NULL
       AND je.entry_date::timestamptz > acc.transitioned_to_parent_at
    THEN
        RAISE EXCEPTION
            'Cannot post to parent account % (code %) after transition date % (entry_date %).',
            acc.id, acc.code, acc.transitioned_to_parent_at, je.entry_date
            USING ERRCODE = '23514'; -- check_violation
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_post_to_parent_after_transition ON journal_items;

CREATE TRIGGER trg_prevent_post_to_parent_after_transition
BEFORE INSERT ON journal_items
FOR EACH ROW
EXECUTE FUNCTION prevent_post_to_parent_after_transition();


-- ---------------------------------------------------------------------------
-- Helper to convert a standalone account into a parent account.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION convert_to_parent(p_account_id UUID)
RETURNS VOID AS $$
DECLARE
    acc RECORD;
BEGIN
    SELECT * INTO acc FROM accounts WHERE id = p_account_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Account % not found', p_account_id
            USING ERRCODE = '02000'; -- no_data_found
    END IF;

    -- Disallow converting child accounts directly into parents.
    IF acc.parent_id IS NOT NULL THEN
        RAISE EXCEPTION
            'Account % (code %) is a child of another account and cannot be converted to a parent.',
            acc.id, acc.code
            USING ERRCODE = '23514'; -- check_violation
    END IF;

    -- Idempotent: if already parent with a transition timestamp, do nothing.
    IF acc.is_parent AND acc.transitioned_to_parent_at IS NOT NULL THEN
        RETURN;
    END IF;

    UPDATE accounts
    SET
        is_parent = TRUE,
        transitioned_to_parent_at = COALESCE(transitioned_to_parent_at, NOW())
    WHERE id = p_account_id;
END;
$$ LANGUAGE plpgsql;

