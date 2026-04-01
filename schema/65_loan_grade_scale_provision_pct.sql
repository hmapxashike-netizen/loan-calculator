-- Provision rates (% of total exposure) per grade rule: regulatory vs standard (IFRS-facing grade).
-- Stored at NUMERIC(22,10). Application also ensures columns via grade_scale_config._ensure_table.

ALTER TABLE loan_grade_scale_rules
    ADD COLUMN IF NOT EXISTS regulatory_provision_pct NUMERIC(22,10) NOT NULL DEFAULT 0;
ALTER TABLE loan_grade_scale_rules
    ADD COLUMN IF NOT EXISTS standard_provision_pct NUMERIC(22,10) NOT NULL DEFAULT 0;

COMMENT ON COLUMN loan_grade_scale_rules.regulatory_provision_pct IS
    'Supervisory/regulatory provision rate (% of total exposure) when loan matches this grade on the regulatory DPD scale.';
COMMENT ON COLUMN loan_grade_scale_rules.standard_provision_pct IS
    'Standard-scale grade provision rate (% of total exposure); shown on ECL report separately from PD-band × unsecured.';
