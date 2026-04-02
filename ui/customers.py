"""Customers UI (vertical slices). Session/widget keys unchanged."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from ui.components import render_green_page_title

from constants import (
    AGENT_CORPORATE_DOC_TYPES,
    AGENT_INDIVIDUAL_DOC_TYPES,
    CORPORATE_DOC_TYPES,
    INDIVIDUAL_DOC_TYPES,
)


def _fmt_status_filter(v: str) -> str:
    """Title-case display for status filter options; values remain lowercase."""
    return {"all": "All", "active": "Active", "inactive": "Inactive"}.get(str(v), str(v))


def _fmt_customer_type_filter(v: str) -> str:
    """Title-case display for customer type filter; values remain lowercase."""
    return {
        "all": "All",
        "individual": "Individual",
        "corporate": "Corporate",
        "agent": "Agent",
    }.get(str(v), str(v))


def _fmt_active_inactive(v: str) -> str:
    """Title-case display; values remain lowercase for persistence."""
    return {"active": "Active", "inactive": "Inactive"}.get(str(v), str(v))


def _agent_table_id_cell(v: object) -> str:
    """String id for agents dataframe: avoids numeric column right-align vs header."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    try:
        return str(int(v))
    except (TypeError, ValueError):
        return str(v)


def render_add_individual_tab(
    *,
    customers_available: bool,
    documents_available: bool,
    list_sectors,
    list_subsectors,
    create_individual,
    list_document_categories,
    upload_document,
) -> None:
    st.subheader("New Individual Customer")
    with st.form("individual_form", clear_on_submit=True):
        ir1a, ir1b, ir1c, ir1d = st.columns(4)
        with ir1a:
            name = st.text_input("Full Name *", placeholder="e.g. John Doe", key="ind_full_name")
        with ir1b:
            national_id = st.text_input("National ID", placeholder="Optional", key="ind_national_id")
        with ir1c:
            phone1 = st.text_input("Phone 1", placeholder="Optional", key="ind_phone1")
        with ir1d:
            phone2 = st.text_input("Phone 2", placeholder="Optional", key="ind_phone2")

        sector_id, subsector_id = None, None
        ir2a, ir2b, ir2c, ir2d = st.columns(4)
        with ir2a:
            email1 = st.text_input("Email 1", placeholder="Optional", key="ind_email1")
        with ir2b:
            email2 = st.text_input("Email 2", placeholder="Optional", key="ind_email2")
        if customers_available:
            sectors_list = list_sectors()
            subsectors_list = list_subsectors()
            if sectors_list:
                with ir2c:
                    sector_names = ["(None)"] + [s["name"] for s in sectors_list]
                    sel_sector_name = st.selectbox("Sector", sector_names, key="ind_sector")
                sector_id = (
                    next((s["id"] for s in sectors_list if s["name"] == sel_sector_name), None)
                    if sel_sector_name != "(None)"
                    else None
                )
                subs_by_sector = [ss for ss in subsectors_list if sector_id and ss["sector_id"] == sector_id]
                sub_names = ["(None)"] + [s["name"] for s in subs_by_sector]
                with ir2d:
                    sel_subsector_name = st.selectbox("Subsector", sub_names, key="ind_subsector")
                subsector_id = (
                    next((s["id"] for s in subs_by_sector if s["name"] == sel_subsector_name), None)
                    if sel_subsector_name != "(None)"
                    else None
                )
                ir3a, ir3b, ir3c, ir3d = st.columns(4)
                with ir3a:
                    employer_details = st.text_input(
                        "Employer Details", placeholder="Optional", key="ind_employer_details"
                    )
            else:
                with ir2c:
                    employer_details = st.text_input(
                        "Employer Details", placeholder="Optional", key="ind_employer_details"
                    )
        else:
            with ir2c:
                employer_details = st.text_input(
                    "Employer Details", placeholder="Optional", key="ind_employer_details"
                )

        with st.expander("Addresses (Optional)"):
            iadr1a, iadr1b, iadr1c, iadr1d = st.columns(4)
            with iadr1a:
                addr_type = st.text_input("Address Type", placeholder="e.g. physical, postal", key="ind_addr_type")
            with iadr1b:
                line1 = st.text_input("Address Line 1", key="ind_addr_line1")
            with iadr1c:
                line2 = st.text_input("Address Line 2", key="ind_addr_line2")
            with iadr1d:
                city = st.text_input("City", key="ind_addr_city")
            iadr2a, iadr2b, iadr2c, iadr2d = st.columns(4)
            with iadr2a:
                region = st.text_input("Region", key="ind_addr_region")
            with iadr2b:
                postal_code = st.text_input("Postal Code", key="ind_addr_postal_code")
            with iadr2c:
                country = st.text_input("Country", key="ind_addr_country")
            with iadr2d:
                use_addr = st.checkbox("Include This Address", value=False, key="ind_use_addr")

        # Individual customer documents: single dropdown + uploader + staged list
        if "ind_docs_staged" not in st.session_state:
            st.session_state["ind_docs_staged"] = []
        with st.expander("Documents (Optional)"):
            staged_ind_docs = st.session_state["ind_docs_staged"]
            if documents_available:
                st.write("Upload Individual KYC Documents Here. Max Size 200MB Per File.")
                doc_cats = list_document_categories(active_only=True)
                name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in INDIVIDUAL_DOC_TYPES}
                if not name_to_cat:
                    st.info("No Matching Document Categories (Individual KYC) Configured.")
                else:
                    d1a, d1b, d1c, d1d = st.columns(4)
                    with d1a:
                        doc_type = st.selectbox(
                            "Document Type",
                            sorted(name_to_cat.keys()),
                            key="ind_doc_type",
                        )
                    other_label = ""
                    if doc_type == "Other":
                        with d1b:
                            other_label = st.text_input(
                                "If Other, Describe The Document",
                                key="ind_doc_other_label",
                            )
                    with d1c:
                        f = st.file_uploader(
                            "Choose File",
                            type=["pdf", "png", "jpg", "jpeg"],
                            key="ind_doc_file",
                        )
                    with d1d:
                        notes = st.text_input("Notes (Optional)", key="ind_doc_notes")
                    doc_add = st.form_submit_button("Save Document To List", key="ind_doc_add")
                    if doc_add and f is not None:
                        cat = name_to_cat[doc_type]
                        label = other_label.strip() if doc_type == "Other" else notes.strip()
                        staged_ind_docs.append(
                            {
                                "category_id": cat["id"],
                                "file": f,
                                "notes": label or "",
                            }
                        )
                        st.session_state["ind_docs_staged"] = staged_ind_docs
                        st.success(f"Staged {f.name} as {doc_type}.")
                if staged_ind_docs:
                    st.markdown("**Staged Documents:**")
                    for idx, row in enumerate(staged_ind_docs, start=1):
                        st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'No Notes'})")
            else:
                st.info("Document Module Is Unavailable.")

        submitted = st.form_submit_button("Create Individual")
        if submitted and name.strip():
            addresses = None
            if use_addr and line1.strip():
                addresses = [{"address_type": addr_type or None, "line1": line1 or None, "line2": line2 or None, "city": city or None, "region": region or None, "postal_code": postal_code or None, "country": country or None}]
            try:
                cid = create_individual(
                    name=name.strip(),
                    national_id=national_id.strip() or None,
                    employer_details=employer_details.strip() or None,
                    phone1=phone1.strip() or None,
                    phone2=phone2.strip() or None,
                    email1=email1.strip() or None,
                    email2=email2.strip() or None,
                    addresses=addresses,
                    sector_id=sector_id,
                    subsector_id=subsector_id,
                )
                st.success(f"Individual Customer Created. Customer ID: **{cid}**.")

                staged_ind_docs = st.session_state.get("ind_docs_staged") or []
                if documents_available and staged_ind_docs:
                    doc_count = 0
                    for row in staged_ind_docs:
                        cat_id = row["category_id"]
                        f = row["file"]
                        notes = row.get("notes") or ""
                        try:
                            upload_document(
                                "customer",
                                cid,
                                cat_id,
                                f.name,
                                f.type,
                                f.size,
                                f.getvalue(),
                                uploaded_by="System User",
                                notes=notes,
                            )
                            doc_count += 1
                        except Exception as e:
                            st.error(f"Failed to upload {f.name}: {e}")
                    if doc_count > 0:
                        st.success(f"Successfully Uploaded {doc_count} Documents.")
                st.session_state["ind_docs_staged"] = []

            except Exception as e:
                st.error(f"Could Not Create Customer: {e}")
        elif submitted and not name.strip():
            st.warning("Please Enter A Name.")


def render_add_corporate_tab(
    *,
    customers_available: bool,
    documents_available: bool,
    list_sectors,
    list_subsectors,
    create_corporate_with_entities,
    list_document_categories,
    upload_document,
) -> None:
    st.subheader("New Corporate Customer")
    with st.form("corporate_form", clear_on_submit=True):
        ct1, ct2, ct3, ct4 = st.columns(4)
        with ct1:
            legal_name = st.text_input("Legal Name *", placeholder="Company Ltd", key="corp_legal_name")
        with ct2:
            reg_number = st.text_input("Registration Number", placeholder="Optional", key="corp_reg_number")
        with ct3:
            trading_name = st.text_input("Trading Name", placeholder="Optional", key="corp_trading_name")
        with ct4:
            tin = st.text_input("TIN", placeholder="Optional", key="corp_tin")
        corp_sector_id, corp_subsector_id = None, None
        cs1, cs2, cs3, cs4 = st.columns(4)
        if customers_available:
            corp_sectors_list = list_sectors()
            corp_subsectors_list = list_subsectors()
            if corp_sectors_list:
                with cs1:
                    corp_sector_names = ["(None)"] + [s["name"] for s in corp_sectors_list]
                    corp_sel_sector = st.selectbox("Sector", corp_sector_names, key="corp_sector")
                corp_sector_id = (
                    next((s["id"] for s in corp_sectors_list if s["name"] == corp_sel_sector), None)
                    if corp_sel_sector != "(None)"
                    else None
                )
                corp_subs = [ss for ss in corp_subsectors_list if corp_sector_id and ss["sector_id"] == corp_sector_id]
                corp_sub_names = ["(None)"] + [s["name"] for s in corp_subs]
                with cs2:
                    corp_sel_subsector = st.selectbox("Subsector", corp_sub_names, key="corp_subsector")
                corp_subsector_id = (
                    next((s["id"] for s in corp_subs if s["name"] == corp_sel_subsector), None)
                    if corp_sel_subsector != "(None)"
                    else None
                )
        with st.expander("Addresses (Optional)"):
            ca1, ca2, ca3, ca4 = st.columns(4)
            with ca1:
                addr_type = st.text_input("Address Type", placeholder="e.g. registered, physical", key="corp_addr_type")
            with ca2:
                line1 = st.text_input("Address Line 1", key="corp_addr_line1")
            with ca3:
                line2 = st.text_input("Address Line 2", key="corp_addr_line2")
            with ca4:
                city = st.text_input("City", key="corp_addr_city")
            ca5, ca6, ca7, ca8 = st.columns(4)
            with ca5:
                region = st.text_input("Region", key="corp_addr_region")
            with ca6:
                postal_code = st.text_input("Postal Code", key="corp_addr_postal_code")
            with ca7:
                country = st.text_input("Country", key="corp_addr_country")
            with ca8:
                use_addr = st.checkbox("Include This Address", value=False, key="corp_use_addr")
        with st.expander("Contact Person (Optional)"):
            cp1, cp2, cp3, cp4 = st.columns(4)
            with cp1:
                cp_name = st.text_input("Full Name", key="corp_cp_name")
            with cp2:
                cp_national_id = st.text_input("National ID", key="corp_cp_national_id")
            with cp3:
                cp_designation = st.text_input("Designation", key="corp_cp_designation")
            with cp4:
                cp_phone1 = st.text_input("Phone 1", key="corp_cp_phone1")
            cp5, cp6, cp7, cp8 = st.columns(4)
            with cp5:
                cp_phone2 = st.text_input("Phone 2", key="corp_cp_phone2")
            with cp6:
                cp_email = st.text_input("Email", key="corp_cp_email")
            with cp7:
                cp_addr1 = st.text_input("Address Line 1", key="corp_cp_addr1")
            with cp8:
                cp_addr2 = st.text_input("Address Line 2", key="corp_cp_addr2")
            cp9, cp10, cp11, cp12 = st.columns(4)
            with cp9:
                cp_city = st.text_input("City", key="corp_cp_city")
            with cp10:
                cp_country = st.text_input("Country", key="corp_cp_country")
            with cp11:
                use_cp = st.checkbox("Include Contact Person", value=False, key="corp_use_cp")
            if "corp_contact_docs_staged" not in st.session_state:
                st.session_state["corp_contact_docs_staged"] = []
            staged_contact_docs = st.session_state["corp_contact_docs_staged"]
            if documents_available:
                st.caption("Contact Person Documents")
                doc_cats = list_document_categories(active_only=True)
                name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in INDIVIDUAL_DOC_TYPES}
                if not name_to_cat:
                    st.info("No Matching Document Categories (Contact Person KYC) Configured.")
                else:
                    cpa, cpb, cpc, cpd = st.columns(4)
                    with cpa:
                        cp_doc_type = st.selectbox(
                            "Document Type",
                            sorted(name_to_cat.keys()),
                            key="cp_doc_type",
                        )
                    cp_other_label = ""
                    if cp_doc_type == "Other":
                        with cpb:
                            cp_other_label = st.text_input(
                                "If Other, Describe The Document",
                                key="cp_doc_other_label",
                            )
                    with cpc:
                        cp_f = st.file_uploader(
                            "Choose File",
                            type=["pdf", "png", "jpg", "jpeg"],
                            key="cp_doc_file",
                        )
                    with cpd:
                        cp_notes = st.text_input("Notes (Optional)", key="cp_doc_notes")
                    cp_add = st.form_submit_button("Save Contact Person Document", key="cp_doc_add")
                    if cp_add and cp_f is not None:
                        cat = name_to_cat[cp_doc_type]
                        label = cp_other_label.strip() if cp_doc_type == "Other" else cp_notes.strip()
                        staged_contact_docs.append({"category_id": cat["id"], "file": cp_f, "notes": label or ""})
                        st.session_state["corp_contact_docs_staged"] = staged_contact_docs
                        st.success(f"Staged {cp_f.name} For Contact Person.")
                if staged_contact_docs:
                    for idx, row in enumerate(staged_contact_docs, start=1):
                        st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'No Notes'})")
        with st.expander("Directors (Optional)"):
            dz1, dz2, dz3, dz4 = st.columns(4)
            with dz1:
                dir_name = st.text_input("Director Full Name", key="corp_dir_name")
            with dz2:
                dir_national_id = st.text_input("Director National ID", key="corp_dir_national_id")
            with dz3:
                dir_designation = st.text_input("Director Designation", key="corp_dir_designation")
            with dz4:
                dir_phone1 = st.text_input("Director Phone 1", key="corp_dir_phone1")
            dz5, dz6, dz7, dz8 = st.columns(4)
            with dz5:
                dir_phone2 = st.text_input("Director Phone 2", key="corp_dir_phone2")
            with dz6:
                dir_email = st.text_input("Director Email", key="corp_dir_email")
            with dz7:
                use_dir = st.checkbox("Include This Director", value=False, key="corp_use_dir")
            if "corp_director_docs_staged" not in st.session_state:
                st.session_state["corp_director_docs_staged"] = []
            staged_director_docs = st.session_state["corp_director_docs_staged"]
            if documents_available:
                st.caption("Director Documents")
                doc_cats = list_document_categories(active_only=True)
                name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in INDIVIDUAL_DOC_TYPES}
                if not name_to_cat:
                    st.info("No Matching Document Categories (Director KYC) Configured.")
                else:
                    dza, dzb, dzc, dzd = st.columns(4)
                    with dza:
                        dir_doc_type = st.selectbox(
                            "Document Type",
                            sorted(name_to_cat.keys()),
                            key="dir_doc_type",
                        )
                    dir_other_label = ""
                    if dir_doc_type == "Other":
                        with dzb:
                            dir_other_label = st.text_input(
                                "If Other, Describe The Document",
                                key="dir_doc_other_label",
                            )
                    with dzc:
                        dir_f = st.file_uploader(
                            "Choose File",
                            type=["pdf", "png", "jpg", "jpeg"],
                            key="dir_doc_file",
                        )
                    with dzd:
                        dir_notes = st.text_input("Notes (Optional)", key="dir_doc_notes")
                    dir_add = st.form_submit_button("Save Director Document", key="dir_doc_add")
                    if dir_add and dir_f is not None:
                        cat = name_to_cat[dir_doc_type]
                        label = dir_other_label.strip() if dir_doc_type == "Other" else dir_notes.strip()
                        staged_director_docs.append({"category_id": cat["id"], "file": dir_f, "notes": label or ""})
                        st.session_state["corp_director_docs_staged"] = staged_director_docs
                        st.success(f"Staged {dir_f.name} For Director.")
                if staged_director_docs:
                    for idx, row in enumerate(staged_director_docs, start=1):
                        st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'No Notes'})")
        with st.expander("Shareholders (Optional)"):
            sz1, sz2, sz3, sz4 = st.columns(4)
            with sz1:
                sh_name = st.text_input("Shareholder Full Name", key="corp_sh_name")
            with sz2:
                sh_national_id = st.text_input("Shareholder National ID", key="corp_sh_national_id")
            with sz3:
                sh_designation = st.text_input("Shareholder Designation", key="corp_sh_designation")
            with sz4:
                sh_phone1 = st.text_input("Shareholder Phone 1", key="corp_sh_phone1")
            sz5, sz6, sz7, sz8 = st.columns(4)
            with sz5:
                sh_phone2 = st.text_input("Shareholder Phone 2", key="corp_sh_phone2")
            with sz6:
                sh_email = st.text_input("Shareholder Email", key="corp_sh_email")
            with sz7:
                sh_pct = st.number_input("Shareholding %", min_value=0.0, max_value=100.0, value=0.0, step=0.5, key="corp_sh_pct")
            with sz8:
                use_sh = st.checkbox("Include This Shareholder", value=False, key="corp_use_sh")

        # Corporate customer documents: single dropdown + uploader + staged list
        if "corp_docs_staged" not in st.session_state:
            st.session_state["corp_docs_staged"] = []
        with st.expander("Documents (Optional)"):
            staged_corp_docs = st.session_state["corp_docs_staged"]
            if documents_available:
                st.write("Upload Corporate Registration Documents Here. Max Size 200MB Per File.")
                doc_cats = list_document_categories(active_only=True)
                name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in CORPORATE_DOC_TYPES}
                if not name_to_cat:
                    st.info("No Matching Document Categories (Corporate KYC) Configured.")
                else:
                    cda, cdb, cdc, cdd = st.columns(4)
                    with cda:
                        doc_type = st.selectbox(
                            "Document Type",
                            sorted(name_to_cat.keys()),
                            key="corp_doc_type",
                        )
                    other_label = ""
                    if doc_type == "Other":
                        with cdb:
                            other_label = st.text_input(
                                "If Other, Describe The Document",
                                key="corp_doc_other_label",
                            )
                    with cdc:
                        f = st.file_uploader(
                            "Choose File",
                            type=["pdf", "png", "jpg", "jpeg"],
                            key="corp_doc_file",
                        )
                    with cdd:
                        notes = st.text_input("Notes (Optional)", key="corp_doc_notes")
                    doc_add_corp = st.form_submit_button("Save Document To List", key="corp_doc_add")
                    if doc_add_corp and f is not None:
                        cat = name_to_cat[doc_type]
                        label = other_label.strip() if doc_type == "Other" else notes.strip()
                        staged_corp_docs.append(
                            {
                                "category_id": cat["id"],
                                "file": f,
                                "notes": label or "",
                            }
                        )
                        st.session_state["corp_docs_staged"] = staged_corp_docs
                        st.success(f"Staged {f.name} as {doc_type}.")
                if staged_corp_docs:
                    st.markdown("**Staged Documents:**")
                    for idx, row in enumerate(staged_corp_docs, start=1):
                        st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'No Notes'})")
            else:
                st.info("Document Module Is Unavailable.")

        submitted = st.form_submit_button("Create Corporate")
        if submitted and legal_name.strip():
            addresses = [{"address_type": addr_type or None, "line1": line1 or None, "line2": line2 or None, "city": city or None, "region": region or None, "postal_code": postal_code or None, "country": country or None}] if use_addr and line1.strip() else None
            contact_person = None
            if use_cp and cp_name.strip():
                contact_person = {"full_name": cp_name.strip(), "national_id": cp_national_id.strip() or None, "designation": cp_designation.strip() or None, "phone1": cp_phone1.strip() or None, "phone2": cp_phone2.strip() or None, "email": cp_email.strip() or None, "address_line1": cp_addr1.strip() or None, "address_line2": cp_addr2.strip() or None, "city": cp_city.strip() or None, "country": cp_country.strip() or None}
            directors = [{"full_name": dir_name.strip(), "national_id": dir_national_id.strip() or None, "designation": dir_designation.strip() or None, "phone1": dir_phone1.strip() or None, "phone2": dir_phone2.strip() or None, "email": dir_email.strip() or None, "address_line1": None, "address_line2": None, "city": None, "country": None}] if use_dir and dir_name.strip() else None
            shareholders = [{"full_name": sh_name.strip(), "national_id": sh_national_id.strip() or None, "designation": sh_designation.strip() or None, "phone1": sh_phone1.strip() or None, "phone2": sh_phone2.strip() or None, "email": sh_email.strip() or None, "address_line1": None, "address_line2": None, "city": None, "country": None, "shareholding_pct": sh_pct}] if use_sh and sh_name.strip() else None
            try:
                created = create_corporate_with_entities(
                    legal_name=legal_name.strip(),
                    trading_name=trading_name.strip() or None,
                    reg_number=reg_number.strip() or None,
                    tin=tin.strip() or None,
                    addresses=addresses,
                    contact_person=contact_person,
                    directors=directors,
                    shareholders=shareholders,
                    sector_id=corp_sector_id,
                    subsector_id=corp_subsector_id,
                )
                cid = int(created["customer_id"])
                st.success(f"Corporate Customer Created. Customer ID: **{cid}**.")

                staged_corp_docs = st.session_state.get("corp_docs_staged") or []
                if documents_available and staged_corp_docs:
                    doc_count = 0
                    for row in staged_corp_docs:
                        cat_id = row["category_id"]
                        f = row["file"]
                        notes = row.get("notes") or ""
                        try:
                            upload_document(
                                "customer",
                                cid,
                                cat_id,
                                f.name,
                                f.type,
                                f.size,
                                f.getvalue(),
                                uploaded_by="System User",
                                notes=notes,
                            )
                            doc_count += 1
                        except Exception as e:
                            st.error(f"Failed to upload {f.name}: {e}")
                    if doc_count > 0:
                        st.success(f"Successfully Uploaded {doc_count} Documents.")
                staged_contact_docs = st.session_state.get("corp_contact_docs_staged") or []
                contact_ids = created.get("contact_person_ids") or []
                if documents_available and staged_contact_docs and contact_ids:
                    cp_id = int(contact_ids[0])
                    cp_count = 0
                    for row in staged_contact_docs:
                        try:
                            upload_document(
                                "contact_person",
                                cp_id,
                                row["category_id"],
                                row["file"].name,
                                row["file"].type,
                                row["file"].size,
                                row["file"].getvalue(),
                                uploaded_by="System User",
                                notes=row.get("notes") or "",
                            )
                            cp_count += 1
                        except Exception as e:
                            st.error(f"Failed to upload contact person doc {row['file'].name}: {e}")
                    if cp_count > 0:
                        st.success(f"Uploaded {cp_count} Contact Person Document(s).")
                staged_director_docs = st.session_state.get("corp_director_docs_staged") or []
                director_ids = created.get("director_ids") or []
                if documents_available and staged_director_docs and director_ids:
                    dir_id = int(director_ids[0])
                    dir_count = 0
                    for row in staged_director_docs:
                        try:
                            upload_document(
                                "director",
                                dir_id,
                                row["category_id"],
                                row["file"].name,
                                row["file"].type,
                                row["file"].size,
                                row["file"].getvalue(),
                                uploaded_by="System User",
                                notes=row.get("notes") or "",
                            )
                            dir_count += 1
                        except Exception as e:
                            st.error(f"Failed to upload director doc {row['file'].name}: {e}")
                    if dir_count > 0:
                        st.success(f"Uploaded {dir_count} Director Document(s).")
                st.session_state["corp_docs_staged"] = []
                st.session_state["corp_contact_docs_staged"] = []
                st.session_state["corp_director_docs_staged"] = []

            except Exception as e:
                st.error(f"Could Not Create Customer: {e}")
                st.exception(e)
        elif submitted and not legal_name.strip():
            st.warning("Please Enter A Legal Name.")


def render_view_manage_customers_tab(
    *,
    customers_available: bool,
    documents_available: bool,
    list_customers,
    list_agents,
    get_display_name,
    get_agent,
    get_customer,
    update_agent,
    update_individual,
    update_corporate,
    set_active,
    list_sectors,
    list_subsectors,
    list_document_categories,
    upload_document,
) -> None:

    st.subheader("View & Manage Customers & Agents")
    vm_r1a, vm_r1b, vm_r1c, vm_r1d = st.columns(4)
    with vm_r1a:
        status_filter = st.selectbox(
            "Status",
            ["all", "active", "inactive"],
            format_func=_fmt_status_filter,
            key="cust_status_filter",
        )
    with vm_r1b:
        type_filter = st.selectbox(
            "Type",
            ["all", "individual", "corporate", "agent"],
            format_func=_fmt_customer_type_filter,
            key="cust_type_filter",
        )
    status = None if status_filter == "all" else status_filter
    customer_type = None if type_filter == "all" else type_filter

    try:
        customers_list = []
        if type_filter in ["all", "individual", "corporate"]:
            customers_list.extend(list_customers(status=status, customer_type=customer_type))
        if type_filter in ["all", "agent"]:
            # Fetch agents and mock them to look like customers for the list view
            agents = list_agents(status=status)
            for a in agents:
                customers_list.append({
                    "id": f"A{a['id']}", # Prefix with A so it doesn't collide
                    "type": "agent",
                    "status": a.get("status", "active"),
                    "created_at": a.get("created_at"),
                    "name": a.get("name", ""),
                    "is_agent": True,
                    "original_id": a["id"]
                })
    except Exception as e:
        st.error(f"Could Not Load Entities: {e}")
        customers_list = []

    action_col1, action_col2, action_col3, action_col4 = st.columns(4)
    with action_col1:
        show_status_tools = st.checkbox(
            "Change Status",
            value=False,
            key="cust_show_status_tools_top",
        )
    with action_col2:
        show_contact_docs_tools = st.checkbox(
            "Contact Person Documents",
            value=False,
            key="cust_show_contact_docs_tools_top",
        )
    with action_col3:
        show_edit_customer = st.checkbox(
            "Edit Details",
            value=False,
            key="cust_show_edit_tools_top",
        )
    if not customers_list:
        st.info("No Entities Found.")
            
    def _get_display(item):
        is_agt = item.get("is_agent")
        if pd.notna(is_agt) and bool(is_agt):
            return f"(Agent) {item.get('name', '')}"
        try:
            # In case ID is not integer
            return get_display_name(int(item["id"])) or f"Customer #{item['id']}"
        except Exception:
            return str(item.get('name', item['id']))

    loaded_id = None
    is_loaded_agent = False
    if (show_status_tools or show_contact_docs_tools or show_edit_customer) and customers_list:
            
        cust_options = [(c["id"], _get_display(c), c.get("is_agent", False)) for c in customers_list]
        labels = [f"{name} (ID {cid})" for cid, name, _ in cust_options]
        sel_idx = 0
        if st.session_state.get("cust_loaded_id") is not None:
            try:
                prev_id = st.session_state["cust_loaded_id"]
                sel_idx = next(i for i, (cid, _n, _a) in enumerate(cust_options) if cid == prev_id)
            except Exception:
                sel_idx = 0
        st.divider()
        selected_label = st.selectbox(
            "Select Entity For Selected Action(s)",
            labels,
            index=sel_idx,
            key="cust_action_select",
        )
            
        if selected_label:
            idx = labels.index(selected_label)
            loaded_id = cust_options[idx][0]
            is_loaded_agent = cust_options[idx][2]
        st.session_state["cust_loaded_id"] = loaded_id
    elif not (show_status_tools or show_contact_docs_tools or show_edit_customer):
        st.caption("Enable An Action Above To Select And Manage An Entity.")
        st.session_state.pop("cust_loaded_id", None)

    if loaded_id is not None:
        if is_loaded_agent:
            # Agent edit flow
            real_agent_id = int(str(loaded_id)[1:])
            arec = get_agent(real_agent_id)
            if not arec:
                st.warning("Agent Not Found.")
            else:
                st.subheader(f"Agent #{real_agent_id}")
                st.markdown(f"**Name:** {arec.get('name')}")
                st.caption(f"Status: {arec.get('status')}")
                    
                if show_edit_customer:
                    st.divider()
                    from customers.approval import save_approval_draft
                    with st.form(f"edit_agent_manage_{real_agent_id}"):
                        eam1, eam2, eam3, eam4 = st.columns(4)
                        with eam1:
                            ename = st.text_input("Agent Name (Changing Requires Approval)", value=arec.get("name") or "", key="eam_name")
                        with eam2:
                            eid_number = st.text_input("ID Number", value=arec.get("id_number") or "", key="eam_id")
                        with eam3:
                            eaddr1 = st.text_input("Address Line 1", value=arec.get("address_line1") or "", key="eam_a1")
                        with eam4:
                            eaddr2 = st.text_input("Address Line 2", value=arec.get("address_line2") or "", key="eam_a2")
                        eam5, eam6, eam7, eam8 = st.columns(4)
                        with eam5:
                            ecity = st.text_input("City", value=arec.get("city") or "", key="eam_city")
                        with eam6:
                            ecountry = st.text_input("Country", value=arec.get("country") or "", key="eam_country")
                        with eam7:
                            ephone1 = st.text_input("Phone 1", value=arec.get("phone1") or "", key="eam_p1")
                        with eam8:
                            ephone2 = st.text_input("Phone 2", value=arec.get("phone2") or "", key="eam_p2")
                        eam9, eam10, eam11, eam12 = st.columns(4)
                        with eam9:
                            eemail = st.text_input("Email", value=arec.get("email") or "", key="eam_email")
                        with eam10:
                            ecommission = st.number_input("Commission Rate %", min_value=0.0, max_value=100.0, value=float(arec.get("commission_rate_pct") or 0), step=0.5, format="%.2f", key="eam_comm")
                        with eam11:
                            etin = st.text_input("TIN Number", value=arec.get("tin_number") or "", key="eam_tin")
                        with eam12:
                            etax_expiry = st.date_input("Tax Clearance Expiry", value=arec.get("tax_clearance_expiry"), key="eam_tax")
                        eam13, eam14, eam15, eam16 = st.columns(4)
                        with eam13:
                            estatus = st.selectbox(
                                "Status",
                                ["active", "inactive"],
                                index=0 if (arec.get("status") or "active") == "active" else 1,
                                format_func=_fmt_active_inactive,
                                key="eam_status",
                            )
                        with eam14:
                            e_agent_type_label = st.selectbox(
                                "Agent Type",
                                ["Individual", "Corporate"],
                                index=0 if (arec.get("agent_type") or "individual") == "individual" else 1,
                                key="eam_type",
                            )

                        st.caption("If You Change The Name, You MUST Provide A Supporting Document Reference.")
                        supp_doc = st.text_input(
                            "Supporting Document Link/Reference (Required If Name Changed)",
                            key="eam_doc",
                        )

                        if st.form_submit_button("Update Agent"):
                            try:
                                old_name = arec.get("name") or ""
                                if ename.strip() != old_name:
                                    if not supp_doc.strip():
                                        st.error("Supporting Document Is Required For Name Change.")
                                    else:
                                        save_approval_draft(
                                            entity_type="agent",
                                            entity_id=real_agent_id,
                                            action_type="NAME_CHANGE",
                                            old_details={"name": old_name},
                                            new_details={"name": ename.strip()},
                                            requested_by="System User",
                                            supporting_document=supp_doc.strip()
                                        )
                                        st.success("Name Change Draft Submitted For Approval.")
                                        ename = old_name
                                update_agent(
                                    real_agent_id,
                                    name=ename.strip(),
                                    id_number=eid_number.strip() or None,
                                    address_line1=eaddr1.strip() or None,
                                    address_line2=eaddr2.strip() or None,
                                    city=ecity.strip() or None,
                                    country=ecountry.strip() or None,
                                    phone1=ephone1.strip() or None,
                                    phone2=ephone2.strip() or None,
                                    email=eemail.strip() or None,
                                    commission_rate_pct=ecommission if ecommission else None,
                                    tin_number=etin.strip() or None,
                                    tax_clearance_expiry=etax_expiry,
                                    status=estatus,
                                    agent_type="individual" if e_agent_type_label.lower().startswith("individual") else "corporate",
                                )
                                st.success("Agent Details Updated Successfully.")
                                if (ename.strip() == old_name) or (ename.strip() != old_name and supp_doc.strip()):
                                    st.rerun()
                            except Exception as e:
                                st.error(f"Could Not Update Agent: {e}")

                if show_status_tools:
                    current_status = arec.get("status", "active")
                    new_active = st.radio(
                        "Set Status",
                        ["active", "inactive"],
                        index=0 if current_status == "active" else 1,
                        format_func=_fmt_active_inactive,
                        key="agt_set_status",
                    )
                    if st.button("Update Status", key="agt_update_status"):
                        update_agent(real_agent_id, name=arec.get("name"), status=new_active)
                        st.success(f"Status Set To **{new_active}**.")
                        st.session_state["cust_loaded_id"] = loaded_id
                        st.rerun()
        else:
            loaded_id = int(loaded_id)
            rec = get_customer(loaded_id)
            if not rec:
                st.warning("Customer Not Found.")
                st.session_state.pop("cust_loaded_id", None)
            else:
                st.subheader(f"Customer #{loaded_id}")
                # Human-readable profile view (avoid dumping raw JSON/object repr in UI).
                ctype = rec.get("type") or "—"
                cstatus = rec.get("status") or "—"
                sector_id = rec.get("sector_id")
                subsector_id = rec.get("subsector_id")
                if ctype == "individual":
                    ind = rec.get("individual") or {}
                    cname = ind.get("name") or "—"
                    st.markdown(f"**Name:** {cname}")
                    st.caption(
                        f"Type: {ctype} · Status: {cstatus} · "
                        f"Sector: {sector_id if sector_id is not None else '—'} · "
                        f"Subsector: {subsector_id if subsector_id is not None else '—'}"
                    )
                    cv1, cv2, cv3, cv4 = st.columns(4)
                    with cv1:
                        st.write(f"**National ID:** {ind.get('national_id') or '—'}")
                    with cv2:
                        st.write(f"**Phone 1:** {ind.get('phone1') or '—'}")
                    with cv3:
                        st.write(f"**Email 1:** {ind.get('email1') or '—'}")
                    with cv4:
                        st.write(f"**Employer Details:** {ind.get('employer_details') or '—'}")
                    cv5, cv6, cv7, cv8 = st.columns(4)
                    with cv5:
                        st.write(f"**Phone 2:** {ind.get('phone2') or '—'}")
                    with cv6:
                        st.write(f"**Email 2:** {ind.get('email2') or '—'}")
                else:
                    corp = rec.get("corporate") or {}
                    cname = corp.get("trading_name") or corp.get("legal_name") or "—"
                    st.markdown(f"**Corporate Name:** {cname}")
                    st.caption(
                        f"Type: {ctype} · Status: {cstatus} · "
                        f"Sector: {sector_id if sector_id is not None else '—'} · "
                        f"Subsector: {subsector_id if subsector_id is not None else '—'}"
                    )
                    cv9, cv10, cv11, cv12 = st.columns(4)
                    with cv9:
                        st.write(f"**Legal Name:** {corp.get('legal_name') or '—'}")
                    with cv10:
                        st.write(f"**Registration Number:** {corp.get('reg_number') or '—'}")
                    with cv11:
                        st.write(f"**Trading Name:** {corp.get('trading_name') or '—'}")
                    with cv12:
                        st.write(f"**TIN:** {corp.get('tin') or '—'}")

                addrs = rec.get("addresses") or []
                if addrs:
                    st.markdown("**Addresses**")
                    for i, a in enumerate(addrs, start=1):
                        line = ", ".join(
                            str(x).strip()
                            for x in [
                                a.get("line1"),
                                a.get("line2"),
                                a.get("city"),
                                a.get("region"),
                                a.get("postal_code"),
                                a.get("country"),
                            ]
                            if x
                        )
                        atype = a.get("address_type") or f"Address {i}"
                        st.write(f"- {atype}: {line or '—'}")

                if show_edit_customer:
                    from customers.approval import save_approval_draft
                    st.divider()
                    st.subheader("Edit Customer Details")
                    with st.form(f"edit_customer_form_{loaded_id}"):
                        if ctype == "individual":
                            ecu1, ecu2, ecu3, ecu4 = st.columns(4)
                            with ecu1:
                                ename = st.text_input("Full Name (Changing This Requires Approval)", value=ind.get("name") or "", key="edit_ind_name")
                            with ecu2:
                                enational_id = st.text_input("National ID", value=ind.get("national_id") or "", key="edit_ind_national_id")
                            with ecu3:
                                ephone1 = st.text_input("Phone 1", value=ind.get("phone1") or "", key="edit_ind_phone1")
                            with ecu4:
                                ephone2 = st.text_input("Phone 2", value=ind.get("phone2") or "", key="edit_ind_phone2")
                            ecu5, ecu6, ecu7, ecu8 = st.columns(4)
                            with ecu5:
                                eemail1 = st.text_input("Email 1", value=ind.get("email1") or "", key="edit_ind_email1")
                            with ecu6:
                                eemail2 = st.text_input("Email 2", value=ind.get("email2") or "", key="edit_ind_email2")
                            with ecu7:
                                eemp = st.text_input(
                                    "Employer Details",
                                    value=ind.get("employer_details") or "",
                                    key="edit_ind_emp",
                                )
                        else:
                            ecc1, ecc2, ecc3, ecc4 = st.columns(4)
                            with ecc1:
                                ename = st.text_input("Legal Name (Changing This Requires Approval)", value=corp.get("legal_name") or "", key="edit_corp_name")
                            with ecc2:
                                etrading = st.text_input("Trading Name", value=corp.get("trading_name") or "", key="edit_corp_trading")
                            with ecc3:
                                ereg = st.text_input("Registration Number", value=corp.get("reg_number") or "", key="edit_corp_reg")
                            with ecc4:
                                etin = st.text_input("TIN", value=corp.get("tin") or "", key="edit_corp_tin")
                    
                        esector_id, esubsector_id = sector_id, subsector_id
                        if customers_available:
                            sectors_list = list_sectors()
                            subsectors_list = list_subsectors()
                            if sectors_list:
                                sector_names = ["(None)"] + [s["name"] for s in sectors_list]
                                curr_sec_name = next((s["name"] for s in sectors_list if s["id"] == sector_id), "(None)")
                                esu_a, esu_b, esu_c, esu_d = st.columns(4)
                                with esu_a:
                                    sel_sector_name = st.selectbox(
                                        "Sector",
                                        sector_names,
                                        index=sector_names.index(curr_sec_name) if curr_sec_name in sector_names else 0,
                                        key="edit_sector",
                                    )
                                esector_id = next((s["id"] for s in sectors_list if s["name"] == sel_sector_name), None) if sel_sector_name != "(None)" else None
                                subs = [ss for ss in subsectors_list if esector_id and ss["sector_id"] == esector_id]
                                sub_names = ["(None)"] + [s["name"] for s in subs]
                                curr_sub_name = next((s["name"] for s in subs if s["id"] == subsector_id), "(None)")
                                s_idx = sub_names.index(curr_sub_name) if curr_sub_name in sub_names else 0
                                with esu_b:
                                    sel_subsector_name = st.selectbox(
                                        "Subsector",
                                        sub_names,
                                        index=s_idx,
                                        key="edit_subsector",
                                    )
                                esubsector_id = next((s["id"] for s in subs if s["name"] == sel_subsector_name), None) if sel_subsector_name != "(None)" else None

                        st.caption("If You Change The Name, You MUST Provide A Supporting Document Reference.")
                        supp_doc = st.text_input(
                            "Supporting Document Link/Reference (Required If Name Changed)",
                            key="edit_supp_doc",
                        )

                        if st.form_submit_button("Save Changes"):
                            try:
                                if ctype == "individual":
                                    old_name = ind.get("name") or ""
                                    if ename.strip() != old_name:
                                        if not supp_doc.strip():
                                            st.error("Supporting Document Is Required For Name Change.")
                                        else:
                                            save_approval_draft(
                                                entity_type="customer",
                                                entity_id=loaded_id,
                                                action_type="NAME_CHANGE",
                                                old_details={"name": old_name},
                                                new_details={"name": ename.strip()},
                                                requested_by="System User",
                                                supporting_document=supp_doc.strip()
                                            )
                                            st.success("Name Change Draft Submitted For Approval.")
                                            ename = old_name
                                    update_individual(
                                        loaded_id,
                                        name=ename.strip(),
                                        national_id=enational_id.strip() or None,
                                        employer_details=eemp.strip() or None,
                                        phone1=ephone1.strip() or None,
                                        phone2=ephone2.strip() or None,
                                        email1=eemail1.strip() or None,
                                        email2=eemail2.strip() or None,
                                        sector_id=esector_id,
                                        subsector_id=esubsector_id,
                                    )
                                else:
                                    old_name = corp.get("legal_name") or ""
                                    if ename.strip() != old_name:
                                        if not supp_doc.strip():
                                            st.error("Supporting Document Is Required For Name Change.")
                                        else:
                                            save_approval_draft(
                                                entity_type="customer",
                                                entity_id=loaded_id,
                                                action_type="NAME_CHANGE",
                                                old_details={"name": old_name},
                                                new_details={"name": ename.strip()},
                                                requested_by="System User",
                                                supporting_document=supp_doc.strip()
                                            )
                                            st.success("Name Change Draft Submitted For Approval.")
                                            ename = old_name
                                    update_corporate(
                                        loaded_id,
                                        legal_name=ename.strip(),
                                        trading_name=etrading.strip() or None,
                                        reg_number=ereg.strip() or None,
                                        tin=etin.strip() or None,
                                        sector_id=esector_id,
                                        subsector_id=esubsector_id,
                                    )
                                st.success("Customer Details Updated Successfully.")
                                # Don't rerun immediately if there was an error with name change validation
                                if (ename.strip() == old_name) or (ename.strip() != old_name and supp_doc.strip()):
                                    st.rerun()
                            except Exception as e:
                                st.error(f"Error Updating Customer: {e}")

                if show_status_tools:
                    current_status = rec.get("status", "active")
                    new_active = st.radio(
                        "Set Status",
                        ["active", "inactive"],
                        index=0 if current_status == "active" else 1,
                        format_func=_fmt_active_inactive,
                        key="cust_set_status",
                    )
                    if st.button("Update Status", key="cust_update_status"):
                        set_active(loaded_id, new_active == "active")
                        st.success(f"Status Set To **{new_active}**.")
                        st.session_state["cust_loaded_id"] = loaded_id
                        st.rerun()

                # Direct document upload to corporate sub-entities (separate buckets/IDs).
                if show_contact_docs_tools and rec.get("type") == "corporate" and documents_available:
                    doc_cats = list_document_categories(active_only=True) or []
                    # Contact person + directors share Individual KYC types plus Other.
                    name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in INDIVIDUAL_DOC_TYPES}

                    if not name_to_cat:
                        st.info("No Matching Document Categories Configured For Contact/Director KYC.")
                    else:
                        cp_list = rec.get("contact_persons") or []
                        dir_list = rec.get("directors") or []

                        if cp_list:
                            st.divider()
                            st.subheader("Contact Person Documents")
                            cp_options = [(cp["id"], cp.get("full_name") or f"Contact #{cp['id']}") for cp in cp_list]
                            cdp_a, cdp_b, cdp_c, cdp_d = st.columns(4)
                            with cdp_a:
                                cp_id = st.selectbox(
                                    "Select Contact Person",
                                    options=[x[0] for x in cp_options],
                                    format_func=lambda i: next((n for (cid, n) in cp_options if cid == i), str(i)),
                                    key=f"cp_doc_pick_{loaded_id}",
                                )
                            with cdp_b:
                                cp_doc_type = st.selectbox(
                                    "Document Type",
                                    sorted(name_to_cat.keys()),
                                    key=f"cp_doc_type_{loaded_id}",
                                )
                            with cdp_c:
                                cp_notes = st.text_input(
                                    "Notes (Optional)",
                                    key=f"cp_doc_notes_{loaded_id}",
                                )
                            with cdp_d:
                                cp_file = st.file_uploader(
                                    "Choose File",
                                    type=["pdf", "png", "jpg", "jpeg"],
                                    key=f"cp_doc_file_{loaded_id}",
                                )
                            cp_other_desc = ""
                            if cp_doc_type == "Other":
                                cp_other_desc = st.text_input(
                                    "Other Document Name",
                                    key=f"cp_doc_other_{loaded_id}",
                                )
                            if st.button("Upload Contact Document", key=f"cp_doc_upload_{loaded_id}") and cp_file is not None:
                                cat = name_to_cat[cp_doc_type]
                                stored_notes = cp_other_desc.strip() if cp_doc_type == "Other" else cp_notes.strip()
                                upload_document(
                                    "contact_person",
                                    int(cp_id),
                                    cat["id"],
                                    cp_file.name,
                                    cp_file.type,
                                    cp_file.size,
                                    cp_file.getvalue(),
                                    uploaded_by="System User",
                                    notes=stored_notes or "",
                                )
                                st.success("Contact Person Document Uploaded.")

                        if dir_list:
                            st.divider()
                            st.subheader("Director Documents")
                            dir_options = [(d["id"], d.get("full_name") or f"Director #{d['id']}") for d in dir_list]
                            ddp_a, ddp_b, ddp_c, ddp_d = st.columns(4)
                            with ddp_a:
                                dir_id = st.selectbox(
                                    "Select Director",
                                    options=[x[0] for x in dir_options],
                                    format_func=lambda i: next((n for (did, n) in dir_options if did == i), str(i)),
                                    key=f"dir_doc_pick_{loaded_id}",
                                )
                            with ddp_b:
                                dir_doc_type = st.selectbox(
                                    "Document Type",
                                    sorted(name_to_cat.keys()),
                                    key=f"dir_doc_type_{loaded_id}",
                                )
                            with ddp_c:
                                dir_notes = st.text_input(
                                    "Notes (Optional)",
                                    key=f"dir_doc_notes_{loaded_id}",
                                )
                            with ddp_d:
                                dir_file = st.file_uploader(
                                    "Choose File",
                                    type=["pdf", "png", "jpg", "jpeg"],
                                    key=f"dir_doc_file_{loaded_id}",
                                )
                            dir_other_desc = ""
                            if dir_doc_type == "Other":
                                dir_other_desc = st.text_input(
                                    "Other Document Name",
                                    key=f"dir_doc_other_{loaded_id}",
                                )
                            if st.button("Upload Director Document", key=f"dir_doc_upload_{loaded_id}") and dir_file is not None:
                                cat = name_to_cat[dir_doc_type]
                                stored_notes = dir_other_desc.strip() if dir_doc_type == "Other" else dir_notes.strip()
                                upload_document(
                                    "director",
                                    int(dir_id),
                                    cat["id"],
                                    dir_file.name,
                                    dir_file.type,
                                    dir_file.size,
                                    dir_file.getvalue(),
                                    uploaded_by="System User",
                                    notes=stored_notes or "",
                                )
                                st.success("Director Document Uploaded.")
    st.divider()
    if customers_list:
        df = pd.DataFrame(customers_list)
        df["display_name"] = df.apply(_get_display, axis=1)
        st.dataframe(
            df[["id", "type", "status", "display_name", "created_at"]],
            width="stretch",
            hide_index=True,
        )


def render_agents_tab(
    *,
    agents_available: bool,
    agents_error: str,
    documents_available: bool,
    list_agents,
    create_agent,
    get_agent,
    update_agent,
    list_document_categories,
    upload_document,
) -> None:

    st.subheader("Agents")
    if not agents_available:
        st.error(f"Agents Module Is Not Available. ({agents_error})")
    else:
        status_agent = st.selectbox(
            "Filter By Status",
            ["active", "inactive", "all"],
            format_func=_fmt_status_filter,
            key="agent_status_filter",
        )
        status_val = None if status_agent == "all" else status_agent
        try:
            agents_list = list_agents(status=status_val)
        except Exception as e:
            st.error(f"Could Not Load Agents: {e}")
            agents_list = []
        if agents_list:
            df_agents = pd.DataFrame(agents_list)
            cols_show = ["id", "name", "id_number", "phone1", "email", "commission_rate_pct", "tax_clearance_expiry", "status"]
            cols_show = [c for c in cols_show if c in df_agents.columns]
            df_show = df_agents[cols_show].copy()
            col_cfg = {}
            if "id" in df_show.columns:
                df_show["id"] = df_show["id"].map(_agent_table_id_cell)
                col_cfg["id"] = st.column_config.TextColumn("ID", width="small")
            st.dataframe(
                df_show,
                width="stretch",
                hide_index=True,
                column_config=col_cfg if col_cfg else None,
            )
        else:
            st.info("No Agents Found.")
        st.divider()
        ag_col1, ag_col2 = st.columns(2)
        with ag_col1:
            show_add_agent = st.checkbox(
                "Add Agent",
                value=False,
                key="agent_show_add_toggle",
            )
        with ag_col2:
            show_edit_agent = st.checkbox(
                "Edit Agent",
                value=False,
                key="agent_show_edit_toggle",
            )

        if not show_add_agent and not show_edit_agent:
            st.caption("Enable An Action Above To Add Or Edit An Agent.")

        if show_add_agent:
            st.subheader("Add Agent")
            with st.form("add_agent_form", clear_on_submit=True):
                col_a1, col_a2 = st.columns(2)
                with col_a1:
                    aname = st.text_input("Agent Name *", key="agent_name")
                    atype_label = st.selectbox("Agent Type", ["Individual", "Corporate"], key="agent_type")
                    aid_number = st.text_input("ID Number", placeholder="e.g. 111111111x11", key="agent_id_number")
                    aaddr1 = st.text_input("Address Line 1", key="agent_addr1")
                    acity = st.text_input("City", key="agent_city")
                    aphone1 = st.text_input("Phone 1", key="agent_phone1")
                    aemail = st.text_input("Email", key="agent_email")
                with col_a2:
                    aaddr2 = st.text_input("Address Line 2", key="agent_addr2")
                    acountry = st.text_input("Country", key="agent_country")
                    aphone2 = st.text_input("Phone 2", key="agent_phone2")
                acommission = st.number_input("Commission Rate %", min_value=0.0, max_value=100.0, value=0.0, step=0.5, format="%.2f", key="agent_commission")
                atin = st.text_input("TIN Number", key="agent_tin")
                atax_expiry = st.date_input("Tax Clearance Expiry", value=None, key="agent_tax_expiry")
                # Agent documents (optional)
                if "agent_docs_staged" not in st.session_state:
                    st.session_state["agent_docs_staged"] = []
                staged_agent_docs = st.session_state["agent_docs_staged"]
                with st.expander("Agent Documents (Optional)"):
                    if documents_available:
                        atype_internal = "individual" if atype_label.lower().startswith("individual") else "corporate"
                        doc_cats = list_document_categories(active_only=True)
                        allowed = AGENT_INDIVIDUAL_DOC_TYPES if atype_internal == "individual" else AGENT_CORPORATE_DOC_TYPES
                        name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in allowed}
                        if not name_to_cat:
                            st.info("No Matching Document Categories Configured For Agents.")
                        else:
                            doc_type = st.selectbox(
                                "Document Type",
                                sorted(name_to_cat.keys()),
                                key="agent_doc_type",
                            )
                            other_label = ""
                            if doc_type == "Other":
                                other_label = st.text_input(
                                    "If Other, Describe The Document",
                                    key="agent_doc_other_label",
                                )
                            f = st.file_uploader(
                                "Choose File",
                                type=["pdf", "png", "jpg", "jpeg"],
                                key="agent_doc_file",
                            )
                            notes = st.text_input("Notes (Optional)", key="agent_doc_notes")
                            add_agent_doc = st.form_submit_button("Save Document To List", key="agent_doc_add")
                            if add_agent_doc and f is not None:
                                cat = name_to_cat[doc_type]
                                label = other_label.strip() if doc_type == "Other" else notes.strip()
                                staged_agent_docs.append(
                                    {
                                        "category_id": cat["id"],
                                        "file": f,
                                        "notes": label or "",
                                    }
                                )
                                st.session_state["agent_docs_staged"] = staged_agent_docs
                                st.success(f"Staged {f.name} as {doc_type}.")
                        if staged_agent_docs:
                            st.markdown("**Staged Documents:**")
                            for idx, row in enumerate(staged_agent_docs, start=1):
                                st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'No Notes'})")
                    else:
                        st.info("Document Module Is Unavailable.")

                submitted_create_agent = st.form_submit_button("Create Agent")
                if submitted_create_agent and aname.strip():
                    try:
                        atype_internal = "individual" if atype_label.lower().startswith("individual") else "corporate"
                        aid = create_agent(
                            name=aname.strip(),
                            agent_type=atype_internal,
                            id_number=aid_number.strip() or None,
                            address_line1=aaddr1.strip() or None,
                            address_line2=aaddr2.strip() or None,
                            city=acity.strip() or None,
                            country=acountry.strip() or None,
                            phone1=aphone1.strip() or None,
                            phone2=aphone2.strip() or None,
                            email=aemail.strip() or None,
                            commission_rate_pct=acommission if acommission else None,
                            tin_number=atin.strip() or None,
                            tax_clearance_expiry=atax_expiry,
                        )
                        # Upload any staged agent documents
                        staged_agent_docs = st.session_state.get("agent_docs_staged") or []
                        if documents_available and staged_agent_docs:
                            doc_count = 0
                            for row in staged_agent_docs:
                                cat_id = row["category_id"]
                                f = row["file"]
                                notes = row.get("notes") or ""
                                try:
                                    upload_document(
                                        "agent",
                                        aid,
                                        cat_id,
                                        f.name,
                                        f.type,
                                        f.size,
                                        f.getvalue(),
                                        uploaded_by="System User",
                                        notes=notes,
                                    )
                                    doc_count += 1
                                except Exception as e:
                                    st.error(f"Failed to upload {f.name}: {e}")
                            if doc_count > 0:
                                st.success(f"Successfully Uploaded {doc_count} Agent Document(s).")
                        st.session_state["agent_docs_staged"] = []
                        st.success(f"Agent Created. Agent ID: **{aid}**.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Could Not Create Agent: {e}")
                elif submitted_create_agent and not aname.strip():
                    st.warning("Please Enter Agent Name.")

        if show_edit_agent:
            st.divider()
            st.subheader("Edit Agent")
            edit_agent_id = st.number_input("Agent ID To Edit", min_value=1, value=1, step=1, key="edit_agent_id")
            if st.button("Load Agent", key="agent_load_btn"):
                st.session_state["agent_edit_loaded_id"] = edit_agent_id
            loaded_agent_id = st.session_state.get("agent_edit_loaded_id")
            if loaded_agent_id is not None:
                arec = get_agent(loaded_agent_id)
                if not arec:
                    st.warning("Agent Not Found.")
                    st.session_state.pop("agent_edit_loaded_id", None)
                else:
                    with st.form("edit_agent_form"):
                        ename = st.text_input("Agent Name *", value=arec.get("name") or "", key="edit_agent_name")
                        eid_number = st.text_input("ID Number", value=arec.get("id_number") or "", key="edit_agent_id_number")
                        eaddr1 = st.text_input("Address Line 1", value=arec.get("address_line1") or "", key="edit_agent_addr1")
                        eaddr2 = st.text_input("Address Line 2", value=arec.get("address_line2") or "", key="edit_agent_addr2")
                        ecity = st.text_input("City", value=arec.get("city") or "", key="edit_agent_city")
                        ecountry = st.text_input("Country", value=arec.get("country") or "", key="edit_agent_country")
                        ephone1 = st.text_input("Phone 1", value=arec.get("phone1") or "", key="edit_agent_phone1")
                        ephone2 = st.text_input("Phone 2", value=arec.get("phone2") or "", key="edit_agent_phone2")
                        eemail = st.text_input("Email", value=arec.get("email") or "", key="edit_agent_email")
                        ecommission = st.number_input("Commission Rate %", min_value=0.0, max_value=100.0, value=float(arec.get("commission_rate_pct") or 0), step=0.5, format="%.2f", key="edit_agent_commission")
                        etin = st.text_input("TIN Number", value=arec.get("tin_number") or "", key="edit_agent_tin")
                        etax_expiry = st.date_input("Tax Clearance Expiry", value=arec.get("tax_clearance_expiry"), key="edit_agent_tax_expiry")
                        estatus = st.selectbox(
                            "Status",
                            ["active", "inactive"],
                            index=0 if (arec.get("status") or "active") == "active" else 1,
                            format_func=_fmt_active_inactive,
                            key="edit_agent_status",
                        )
                        e_agent_type_label = st.selectbox(
                            "Agent Type",
                            ["Individual", "Corporate"],
                            index=0 if (arec.get("agent_type") or "individual") == "individual" else 1,
                            key="edit_agent_type",
                        )
                        submitted_update_agent = st.form_submit_button("Update Agent")
                        st.caption("If You Change The Name, You MUST Provide A Supporting Document Reference.")
                        supp_doc = st.text_input(
                            "Supporting Document Link/Reference (Required If Name Changed)",
                            key="edit_agent_supp_doc",
                        )

                        if submitted_update_agent and ename.strip():
                            try:
                                old_name = arec.get("name") or ""
                                if ename.strip() != old_name:
                                    if not supp_doc.strip():
                                        st.error("Supporting Document Is Required For Name Change.")
                                    else:
                                        from customers.approval import save_approval_draft
                                        save_approval_draft(
                                            entity_type="agent",
                                            entity_id=loaded_agent_id,
                                            action_type="NAME_CHANGE",
                                            old_details={"name": old_name},
                                            new_details={"name": ename.strip()},
                                            requested_by="System User",
                                            supporting_document=supp_doc.strip()
                                        )
                                        st.success("Name Change Draft Submitted For Approval.")
                                        ename = old_name # Revert to save other fields without name
                                update_agent(
                                    loaded_agent_id,
                                    name=ename.strip(),
                                    id_number=eid_number.strip() or None,
                                    address_line1=eaddr1.strip() or None,
                                    address_line2=eaddr2.strip() or None,
                                    city=ecity.strip() or None,
                                    country=ecountry.strip() or None,
                                    phone1=ephone1.strip() or None,
                                    phone2=ephone2.strip() or None,
                                    email=eemail.strip() or None,
                                    commission_rate_pct=ecommission if ecommission else None,
                                    tin_number=etin.strip() or None,
                                    tax_clearance_expiry=etax_expiry,
                                    status=estatus,
                                    agent_type="individual" if e_agent_type_label.lower().startswith("individual") else "corporate",
                                )
                                st.success("Agent Details Updated Successfully.")
                                if (ename.strip() == old_name) or (ename.strip() != old_name and supp_doc.strip()):
                                    st.rerun()
                            except Exception as e:
                                st.error(f"Could Not Update Agent: {e}")
                        elif submitted_update_agent and not ename.strip():
                            st.warning("Please Enter Agent Name.")


def render_customers_ui(
    *,
    customers_available: bool,
    customers_error: str,
    documents_available: bool,
    agents_available: bool,
    agents_error: str,
    list_sectors,
    list_subsectors,
    create_individual,
    create_corporate_with_entities,
    list_customers,
    list_agents,
    get_display_name,
    get_agent,
    get_customer,
    update_agent,
    update_individual,
    update_corporate,
    set_active,
    create_agent,
    list_document_categories,
    upload_document,
) -> None:
    """Customers page (all tabs). Widget/session keys unchanged; see docs/SESSION_STATE_CONTRACT.md."""
    if not customers_available:
        st.error(
            "Customer Module Is Not Available. Check Database Connection And Install: psycopg2-binary. "
            f"({customers_error})"
        )
        return

    render_green_page_title("Customers")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["Add Individual", "Add Corporate", "View & Manage", "Agents", "Approvals"]
    )

    with tab1:
        render_add_individual_tab(
            customers_available=customers_available,
            documents_available=documents_available,
            list_sectors=list_sectors,
            list_subsectors=list_subsectors,
            create_individual=create_individual,
            list_document_categories=list_document_categories,
            upload_document=upload_document,
        )

    with tab2:
        render_add_corporate_tab(
            customers_available=customers_available,
            documents_available=documents_available,
            list_sectors=list_sectors,
            list_subsectors=list_subsectors,
            create_corporate_with_entities=create_corporate_with_entities,
            list_document_categories=list_document_categories,
            upload_document=upload_document,
        )

    with tab3:
        render_view_manage_customers_tab(
            customers_available=customers_available,
            documents_available=documents_available,
            list_customers=list_customers,
            list_agents=list_agents,
            get_display_name=get_display_name,
            get_agent=get_agent,
            get_customer=get_customer,
            update_agent=update_agent,
            update_individual=update_individual,
            update_corporate=update_corporate,
            set_active=set_active,
            list_sectors=list_sectors,
            list_subsectors=list_subsectors,
            list_document_categories=list_document_categories,
            upload_document=upload_document,
        )

    with tab4:
        render_agents_tab(
            agents_available=agents_available,
            agents_error=agents_error,
            documents_available=documents_available,
            list_agents=list_agents,
            create_agent=create_agent,
            get_agent=get_agent,
            update_agent=update_agent,
            list_document_categories=list_document_categories,
            upload_document=upload_document,
        )

    with tab5:
        customer_approvals_ui(is_tab=True)


def customer_approvals_ui(is_tab=False):
    """Web UI to manage customer & agent approval drafts (e.g. name changes)."""
    import json as _json

    from customers.approval import approve_draft, dismiss_draft, list_pending_drafts, rework_draft

    if not st.session_state.get("_cust_appr_panel_css"):
        st.session_state["_cust_appr_panel_css"] = True
        st.markdown(
            """
<style>
.cust-appr-stage-lbl {
  font-size: 0.9rem;
  font-weight: 800;
  letter-spacing: 0.055em;
  text-transform: none;
  color: #1d4ed8;
  margin: 0.1rem 0 0.32rem 0;
  line-height: 1.25;
}
.cust-appr-blue-rule {
  border: 0;
  border-top: 2px solid #2563eb;
  margin: 0.48rem 0 0.52rem 0;
  opacity: 0.92;
}
</style>
            """,
            unsafe_allow_html=True,
        )

    if not is_tab:
        st.markdown(
            "<div style='color:#16A34A; font-weight:700; font-size:1.725rem; margin:0.08rem 0 0.4rem 0;'>"
            "Customer & Agent Approvals</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            "<div style='font-weight:700; font-size:1.3125rem; margin:0.02rem 0 0.32rem 0; color:#334155;'>"
            "Customer & Agent Approvals</div>",
            unsafe_allow_html=True,
        )

    drafts: list = []
    try:
        drafts = list_pending_drafts() or []
    except Exception as e:
        st.error(f"Could Not Load Drafts: {e}")

    with st.container(border=True):
        st.caption(
            "One Compact Panel: **Stage 1** Lists Pending Items; The **Blue Rule** Separates **Stage 2** "
            "(Pick A Draft, Compare Old Vs New, Apply A Decision)."
        )
        st.markdown('<p class="cust-appr-stage-lbl">Stage 1 — Pending Queue</p>', unsafe_allow_html=True)
        if not drafts:
            st.info("No Pending Approval Drafts.")
        else:
            disp_rows = []
            for d in drafts:
                ts = d.get("submitted_at")
                disp_rows.append(
                    {
                        "id": int(d["id"]),
                        "entity_type": d.get("entity_type") or "—",
                        "entity_id": d.get("entity_id"),
                        "action_type": d.get("action_type") or "—",
                        "status": d.get("status") or "—",
                        "requested_by": (d.get("requested_by") or "").strip() or "—",
                        "submitted_at": str(ts)[:19] if ts else "—",
                    }
                )
            df_q = pd.DataFrame(disp_rows)
            st.dataframe(
                df_q,
                hide_index=True,
                height=min(200, 52 + len(drafts) * 30),
                width="stretch",
                column_config={
                    "id": st.column_config.NumberColumn("ID", width="small"),
                    "entity_type": st.column_config.TextColumn("Entity", width="small"),
                    "entity_id": st.column_config.NumberColumn("Ent. ID", width="small"),
                    "action_type": st.column_config.TextColumn("Action", width="medium"),
                    "status": st.column_config.TextColumn("Status", width="small"),
                    "requested_by": st.column_config.TextColumn("Requested By", width="medium"),
                    "submitted_at": st.column_config.TextColumn("Submitted", width="medium"),
                },
            )

        st.markdown('<hr class="cust-appr-blue-rule"/>', unsafe_allow_html=True)
        st.markdown('<p class="cust-appr-stage-lbl">Stage 2 — Review & Decision</p>', unsafe_allow_html=True)

        if not drafts:
            st.caption("When Items Appear Above, Select A Draft And Choose Approve, Rework, Or Dismiss.")
        else:
            ap1, ap2, ap3, ap4 = st.columns(4)
            with ap1:
                selected_id = st.selectbox(
                    "Draft",
                    [d["id"] for d in drafts],
                    format_func=lambda i: f"#{int(i)}",
                    key="cust_appr_draft_id",
                )
            with ap2:
                action = st.radio(
                    "Decision",
                    ["Approve", "Rework", "Dismiss"],
                    horizontal=True,
                    key="cust_appr_action",
                )
            with ap3:
                note = st.text_input(
                    "Reviewer Note",
                    key="cust_appr_note",
                    placeholder="Required For Rework / Dismiss",
                )

            draft = next((d for d in drafts if d["id"] == selected_id), None)
            if draft:
                old_details = draft.get("old_details") or {}
                new_details = draft.get("new_details") or {}
                supp = (draft.get("supporting_document") or "").strip() or "—"
                st.caption(
                    f"**{draft.get('entity_type', '')}** #{draft.get('entity_id')} · "
                    f"**{draft.get('action_type', '')}** · Supporting Doc: {supp}"
                )
                hdr_l = (
                    "<div style='text-align:center;font-size:0.975rem;font-weight:600;color:#64748b;"
                    "margin:0.1rem 0 0.15rem 0;'>Previous (Old)</div>"
                )
                hdr_r = (
                    "<div style='text-align:center;font-size:0.975rem;font-weight:600;color:#64748b;"
                    "margin:0.1rem 0 0.15rem 0;'>Proposed (New)</div>"
                )
                ocol, ncol = st.columns(2)
                with ocol:
                    st.markdown(hdr_l, unsafe_allow_html=True)
                    st.text_area(
                        "old_json",
                        value=_json.dumps(old_details, indent=2, ensure_ascii=False),
                        height=130,
                        disabled=True,
                        key=f"cust_appr_old_{selected_id}",
                        label_visibility="collapsed",
                    )
                with ncol:
                    st.markdown(hdr_r, unsafe_allow_html=True)
                    st.text_area(
                        "new_json",
                        value=_json.dumps(new_details, indent=2, ensure_ascii=False),
                        height=130,
                        disabled=True,
                        key=f"cust_appr_new_{selected_id}",
                        label_visibility="collapsed",
                    )

            if st.button("Apply Decision", key="cust_appr_submit", type="primary"):
                try:
                    did_mutate = False
                    if action == "Approve":
                        approve_draft(int(selected_id), approved_by="System User")
                        st.success(f"Draft #{int(selected_id)} Approved.")
                        did_mutate = True
                    elif action == "Rework":
                        if not str(note or "").strip():
                            st.warning("Note Is Required For Rework.")
                        else:
                            rework_draft(int(selected_id), str(note).strip(), reworked_by="System User")
                            st.success(f"Draft #{int(selected_id)} Sent For Rework.")
                            did_mutate = True
                    elif action == "Dismiss":
                        if not str(note or "").strip():
                            st.warning("Note Is Required For Dismiss.")
                        else:
                            dismiss_draft(int(selected_id), str(note).strip(), dismissed_by="System User")
                            st.success(f"Draft #{int(selected_id)} Dismissed.")
                            did_mutate = True
                    if did_mutate:
                        st.rerun()
                except Exception as e:
                    st.error(f"Error Applying Action: {e}")
