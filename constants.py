# Document type name whitelists per entity (must match document_categories.name).
# Used by customer KYC uploads and agent document staging in the Streamlit app.

INDIVIDUAL_DOC_TYPES = {
    "National ID",
    "Payslip",
    "Proof of Residence",
    "Confirmation of Employment",
    "Other",
}

CORPORATE_DOC_TYPES = {
    "CR5",
    "CR6",
    "Memorandum and Articles",
    "Certificate of Incorporation",
    "CR2",
    "Other",
}

AGENT_INDIVIDUAL_DOC_TYPES = INDIVIDUAL_DOC_TYPES.union({"Tax Clearance"})

AGENT_CORPORATE_DOC_TYPES = CORPORATE_DOC_TYPES.union(
    {
        # Director KYC (same as individual)
        "National ID",
        "Payslip",
        "Proof of Residence",
        "Confirmation of Employment",
    }
)
