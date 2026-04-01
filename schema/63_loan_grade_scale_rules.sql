-- Configurable loan grades: regulatory DPD scale vs standard (IFRS-facing) DPD scale.
-- Resolution: first matching row in sort_order for active rules.

CREATE TABLE IF NOT EXISTS loan_grade_scale_rules (
    id SERIAL PRIMARY KEY,
    sort_order INTEGER NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    grade_name VARCHAR(128) NOT NULL,
    performance_status VARCHAR(64) NOT NULL,
    regulatory_dpd_min INTEGER NOT NULL DEFAULT 0,
    regulatory_dpd_max INTEGER,
    standard_dpd_min INTEGER NOT NULL DEFAULT 0,
    standard_dpd_max INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_loan_grade_scale_rules_sort
    ON loan_grade_scale_rules (is_active, sort_order, id);

COMMENT ON TABLE loan_grade_scale_rules IS
    'Maps DPD to grade and performing / non-performing; regulatory_* for supervisory reports, standard_* for IFRS-facing classification.';

COMMENT ON COLUMN loan_grade_scale_rules.regulatory_dpd_max IS
    'NULL means no upper bound (e.g. 91+ dpd).';
COMMENT ON COLUMN loan_grade_scale_rules.standard_dpd_max IS
    'NULL means no upper bound (e.g. 361+ dpd).';

-- Seed defaults only when table is empty (do not overwrite user edits).
INSERT INTO loan_grade_scale_rules (
    sort_order, grade_name, performance_status,
    regulatory_dpd_min, regulatory_dpd_max, standard_dpd_min, standard_dpd_max
)
SELECT so, g, p, rmin, rmax, smin, smax
FROM (
    VALUES
        (10, 'Pass', 'Performing', 0, 0, 0, 0),
        (20, 'Special Mention', 'Performing', 1, 30, 1, 90),
        (30, 'Sub standard', 'NonPerforming', 31, 60, 91, 180),
        (40, 'Doubtful', 'NonPerforming', 61, 90, 181, 360),
        (50, 'Loss', 'NonPerforming', 91, NULL::integer, 361, NULL::integer)
) AS t (so, g, p, rmin, rmax, smin, smax)
WHERE NOT EXISTS (SELECT 1 FROM loan_grade_scale_rules LIMIT 1);
