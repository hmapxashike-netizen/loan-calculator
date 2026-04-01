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
    st.subheader("New individual customer")
    col_main, _ = st.columns([1, 1])
    with col_main:
        with st.form("individual_form", clear_on_submit=True):
            col_id1, col_id2 = st.columns(2)
            with col_id1:
                name = st.text_input("Full name *", placeholder="e.g. John Doe", key="ind_full_name")
            with col_id2:
                national_id = st.text_input("National ID", placeholder="Optional", key="ind_national_id")
            sector_id, subsector_id = None, None
            if customers_available:
                sectors_list = list_sectors()
                subsectors_list = list_subsectors()
                if sectors_list:
                    sector_names = ["(None)"] + [s["name"] for s in sectors_list]
                    sel_sector_name = st.selectbox("Sector", sector_names, key="ind_sector")
                    sector_id = next((s["id"] for s in sectors_list if s["name"] == sel_sector_name), None) if sel_sector_name != "(None)" else None
                    subs_by_sector = [ss for ss in subsectors_list if sector_id and ss["sector_id"] == sector_id]
                    sub_names = ["(None)"] + [s["name"] for s in subs_by_sector]
                    sel_subsector_name = st.selectbox("Subsector", sub_names, key="ind_subsector")
                    subsector_id = next((s["id"] for s in subs_by_sector if s["name"] == sel_subsector_name), None) if sel_subsector_name != "(None)" else None
            col1, col2 = st.columns(2)
            with col1:
                phone1 = st.text_input("Phone 1", placeholder="Optional", key="ind_phone1")
                email1 = st.text_input("Email 1", placeholder="Optional", key="ind_email1")
            with col2:
                phone2 = st.text_input("Phone 2", placeholder="Optional", key="ind_phone2")
                email2 = st.text_input("Email 2", placeholder="Optional", key="ind_email2")
            employer_details = st.text_area("Employer details", placeholder="Optional", key="ind_employer_details", height=80)
            with st.expander("Addresses (optional)"):
                addr_type = st.text_input("Address type", placeholder="e.g. physical, postal", key="ind_addr_type")
                line1 = st.text_input("Address line 1", key="ind_addr_line1")
                line2 = st.text_input("Address line 2", key="ind_addr_line2")
                city = st.text_input("City", key="ind_addr_city")
                region = st.text_input("Region", key="ind_addr_region")
                postal_code = st.text_input("Postal code", key="ind_addr_postal_code")
                country = st.text_input("Country", key="ind_addr_country")
                use_addr = st.checkbox("Include this address", value=False, key="ind_use_addr")
    
            # Individual customer documents: single dropdown + uploader + staged list
            if "ind_docs_staged" not in st.session_state:
                st.session_state["ind_docs_staged"] = []
            with st.expander("Documents (optional)"):
                staged_ind_docs = st.session_state["ind_docs_staged"]
                if documents_available:
                    st.write("Upload individual KYC documents here. Max size 200MB per file.")
                    doc_cats = list_document_categories(active_only=True)
                    name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in INDIVIDUAL_DOC_TYPES}
                    if not name_to_cat:
                        st.info("No matching document categories (Individual KYC) configured.")
                    else:
                        doc_type = st.selectbox(
                            "Document type",
                            sorted(name_to_cat.keys()),
                            key="ind_doc_type",
                        )
                        other_label = ""
                        if doc_type == "Other":
                            other_label = st.text_input(
                                "If Other, describe the document",
                                key="ind_doc_other_label",
                            )
                        f = st.file_uploader(
                            "Choose file",
                            type=["pdf", "png", "jpg", "jpeg"],
                            key="ind_doc_file",
                        )
                        notes = st.text_input("Notes (optional)", key="ind_doc_notes")
                        doc_add = st.form_submit_button("Save document to list", key="ind_doc_add")
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
                        st.markdown("**Staged documents:**")
                        for idx, row in enumerate(staged_ind_docs, start=1):
                            st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'no notes'})")
                else:
                    st.info("Document module is unavailable.")
    
            submitted = st.form_submit_button("Create individual")
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
                    st.success(f"Individual customer created. Customer ID: **{cid}**.")
    
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
                            st.success(f"Successfully uploaded {doc_count} documents.")
                    st.session_state["ind_docs_staged"] = []
    
                except Exception as e:
                    st.error(f"Could not create customer: {e}")
            elif submitted and not name.strip():
                st.warning("Please enter a name.")


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
    st.subheader("New corporate customer")
    col_main2, _ = st.columns([1, 1])
    with col_main2:
        with st.form("corporate_form", clear_on_submit=True):
            corp_top1, corp_top2 = st.columns(2)
            with corp_top1:
                legal_name = st.text_input("Legal name *", placeholder="Company Ltd", key="corp_legal_name")
                reg_number = st.text_input("Registration number", placeholder="Optional", key="corp_reg_number")
            with corp_top2:
                trading_name = st.text_input("Trading name", placeholder="Optional", key="corp_trading_name")
            tin = st.text_input("TIN", placeholder="Optional", key="corp_tin")
            corp_sector_id, corp_subsector_id = None, None
            if customers_available:
                corp_sectors_list = list_sectors()
                corp_subsectors_list = list_subsectors()
                if corp_sectors_list:
                    corp_sector_names = ["(None)"] + [s["name"] for s in corp_sectors_list]
                    corp_sel_sector = st.selectbox("Sector", corp_sector_names, key="corp_sector")
                    corp_sector_id = next((s["id"] for s in corp_sectors_list if s["name"] == corp_sel_sector), None) if corp_sel_sector != "(None)" else None
                    corp_subs = [ss for ss in corp_subsectors_list if corp_sector_id and ss["sector_id"] == corp_sector_id]
                    corp_sub_names = ["(None)"] + [s["name"] for s in corp_subs]
                    corp_sel_subsector = st.selectbox("Subsector", corp_sub_names, key="corp_subsector")
                    corp_subsector_id = next((s["id"] for s in corp_subs if s["name"] == corp_sel_subsector), None) if corp_sel_subsector != "(None)" else None
            with st.expander("Addresses (optional)"):
                addr_type = st.text_input("Address type", placeholder="e.g. registered, physical", key="corp_addr_type")
                line1 = st.text_input("Address line 1", key="corp_addr_line1")
                line2 = st.text_input("Address line 2", key="corp_addr_line2")
                city = st.text_input("City", key="corp_addr_city")
                region = st.text_input("Region", key="corp_addr_region")
                postal_code = st.text_input("Postal code", key="corp_addr_postal_code")
                country = st.text_input("Country", key="corp_addr_country")
                use_addr = st.checkbox("Include this address", value=False, key="corp_use_addr")
            with st.expander("Contact person (optional)"):
                cp_name = st.text_input("Full name", key="corp_cp_name")
                cp_national_id = st.text_input("National ID", key="corp_cp_national_id")
                cp_designation = st.text_input("Designation", key="corp_cp_designation")
                cp_phone1 = st.text_input("Phone 1", key="corp_cp_phone1")
                cp_phone2 = st.text_input("Phone 2", key="corp_cp_phone2")
                cp_email = st.text_input("Email", key="corp_cp_email")
                cp_addr1 = st.text_input("Address line 1", key="corp_cp_addr1")
                cp_addr2 = st.text_input("Address line 2", key="corp_cp_addr2")
                cp_city = st.text_input("City", key="corp_cp_city")
                cp_country = st.text_input("Country", key="corp_cp_country")
                use_cp = st.checkbox("Include contact person", value=False, key="corp_use_cp")
                if "corp_contact_docs_staged" not in st.session_state:
                    st.session_state["corp_contact_docs_staged"] = []
                staged_contact_docs = st.session_state["corp_contact_docs_staged"]
                if documents_available:
                    st.caption("Contact person documents")
                    doc_cats = list_document_categories(active_only=True)
                    name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in INDIVIDUAL_DOC_TYPES}
                    if not name_to_cat:
                        st.info("No matching document categories (Contact person KYC) configured.")
                    else:
                        cp_doc_type = st.selectbox(
                            "Document type",
                            sorted(name_to_cat.keys()),
                            key="cp_doc_type",
                        )
                        cp_other_label = ""
                        if cp_doc_type == "Other":
                            cp_other_label = st.text_input(
                                "If Other, describe the document",
                                key="cp_doc_other_label",
                            )
                        cp_f = st.file_uploader(
                            "Choose file",
                            type=["pdf", "png", "jpg", "jpeg"],
                            key="cp_doc_file",
                        )
                        cp_notes = st.text_input("Notes (optional)", key="cp_doc_notes")
                        cp_add = st.form_submit_button("Save contact person document", key="cp_doc_add")
                        if cp_add and cp_f is not None:
                            cat = name_to_cat[cp_doc_type]
                            label = cp_other_label.strip() if cp_doc_type == "Other" else cp_notes.strip()
                            staged_contact_docs.append({"category_id": cat["id"], "file": cp_f, "notes": label or ""})
                            st.session_state["corp_contact_docs_staged"] = staged_contact_docs
                            st.success(f"Staged {cp_f.name} for contact person.")
                    if staged_contact_docs:
                        for idx, row in enumerate(staged_contact_docs, start=1):
                            st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'no notes'})")
            with st.expander("Directors (optional)"):
                dir_name = st.text_input("Director full name", key="corp_dir_name")
                dir_national_id = st.text_input("Director national ID", key="corp_dir_national_id")
                dir_designation = st.text_input("Director designation", key="corp_dir_designation")
                dir_phone1 = st.text_input("Director phone 1", key="corp_dir_phone1")
                dir_phone2 = st.text_input("Director phone 2", key="corp_dir_phone2")
                dir_email = st.text_input("Director email", key="corp_dir_email")
                use_dir = st.checkbox("Include this director", value=False, key="corp_use_dir")
                if "corp_director_docs_staged" not in st.session_state:
                    st.session_state["corp_director_docs_staged"] = []
                staged_director_docs = st.session_state["corp_director_docs_staged"]
                if documents_available:
                    st.caption("Director documents")
                    doc_cats = list_document_categories(active_only=True)
                    name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in INDIVIDUAL_DOC_TYPES}
                    if not name_to_cat:
                        st.info("No matching document categories (Director KYC) configured.")
                    else:
                        dir_doc_type = st.selectbox(
                            "Document type",
                            sorted(name_to_cat.keys()),
                            key="dir_doc_type",
                        )
                        dir_other_label = ""
                        if dir_doc_type == "Other":
                            dir_other_label = st.text_input(
                                "If Other, describe the document",
                                key="dir_doc_other_label",
                            )
                        dir_f = st.file_uploader(
                            "Choose file",
                            type=["pdf", "png", "jpg", "jpeg"],
                            key="dir_doc_file",
                        )
                        dir_notes = st.text_input("Notes (optional)", key="dir_doc_notes")
                        dir_add = st.form_submit_button("Save director document", key="dir_doc_add")
                        if dir_add and dir_f is not None:
                            cat = name_to_cat[dir_doc_type]
                            label = dir_other_label.strip() if dir_doc_type == "Other" else dir_notes.strip()
                            staged_director_docs.append({"category_id": cat["id"], "file": dir_f, "notes": label or ""})
                            st.session_state["corp_director_docs_staged"] = staged_director_docs
                            st.success(f"Staged {dir_f.name} for director.")
                    if staged_director_docs:
                        for idx, row in enumerate(staged_director_docs, start=1):
                            st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'no notes'})")
            with st.expander("Shareholders (optional)"):
                sh_name = st.text_input("Shareholder full name", key="corp_sh_name")
                sh_national_id = st.text_input("Shareholder national ID", key="corp_sh_national_id")
                sh_designation = st.text_input("Shareholder designation", key="corp_sh_designation")
                sh_phone1 = st.text_input("Shareholder phone 1", key="corp_sh_phone1")
                sh_phone2 = st.text_input("Shareholder phone 2", key="corp_sh_phone2")
                sh_email = st.text_input("Shareholder email", key="corp_sh_email")
                sh_pct = st.number_input("Shareholding %", min_value=0.0, max_value=100.0, value=0.0, step=0.5, key="corp_sh_pct")
                use_sh = st.checkbox("Include this shareholder", value=False, key="corp_use_sh")
    
            # Corporate customer documents: single dropdown + uploader + staged list
            if "corp_docs_staged" not in st.session_state:
                st.session_state["corp_docs_staged"] = []
            with st.expander("Documents (optional)"):
                staged_corp_docs = st.session_state["corp_docs_staged"]
                if documents_available:
                    st.write("Upload corporate registration documents here. Max size 200MB per file.")
                    doc_cats = list_document_categories(active_only=True)
                    name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in CORPORATE_DOC_TYPES}
                    if not name_to_cat:
                        st.info("No matching document categories (Corporate KYC) configured.")
                    else:
                        doc_type = st.selectbox(
                            "Document type",
                            sorted(name_to_cat.keys()),
                            key="corp_doc_type",
                        )
                        other_label = ""
                        if doc_type == "Other":
                            other_label = st.text_input(
                                "If Other, describe the document",
                                key="corp_doc_other_label",
                            )
                        f = st.file_uploader(
                            "Choose file",
                            type=["pdf", "png", "jpg", "jpeg"],
                            key="corp_doc_file",
                        )
                        notes = st.text_input("Notes (optional)", key="corp_doc_notes")
                        doc_add_corp = st.form_submit_button("Save document to list", key="corp_doc_add")
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
                        st.markdown("**Staged documents:**")
                        for idx, row in enumerate(staged_corp_docs, start=1):
                            st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'no notes'})")
                else:
                    st.info("Document module is unavailable.")
    
            submitted = st.form_submit_button("Create corporate")
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
                    st.success(f"Corporate customer created. Customer ID: **{cid}**.")
    
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
                            st.success(f"Successfully uploaded {doc_count} documents.")
                    # Upload contact person docs to their own bucket/entity id.
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
                            st.success(f"Uploaded {cp_count} contact person document(s).")
                    # Upload director docs to their own bucket/entity id (first director from this form).
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
                            st.success(f"Uploaded {dir_count} director document(s).")
                    st.session_state["corp_docs_staged"] = []
                    st.session_state["corp_contact_docs_staged"] = []
                    st.session_state["corp_director_docs_staged"] = []
    
                except Exception as e:
                    st.error(f"Could not create customer: {e}")
                    st.exception(e)
            elif submitted and not legal_name.strip():
                st.warning("Please enter a legal name.")


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

    st.subheader("View & manage customers and agents")
    col_main3, _ = st.columns([1, 1])
    with col_main3:
        status_filter = st.selectbox("Status", ["all", "active", "inactive"], key="cust_status_filter")
        type_filter = st.selectbox("Type", ["all", "individual", "corporate", "agent"], key="cust_type_filter")
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
            st.error(f"Could not load entities: {e}")
            customers_list = []

        action_col1, action_col2, action_col3 = st.columns(3)
        with action_col1:
            show_status_tools = st.checkbox(
                "Change status",
                value=False,
                key="cust_show_status_tools_top",
            )
        with action_col2:
            show_contact_docs_tools = st.checkbox(
                "Contact person documents",
                value=False,
                key="cust_show_contact_docs_tools_top",
            )
        with action_col3:
            show_edit_customer = st.checkbox(
                "Edit details",
                value=False,
                key="cust_show_edit_tools_top",
            )
        if not customers_list:
            st.info("No entities found.")
            
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
                "Select entity for selected action(s)",
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
            st.caption("Enable an action above to select and manage an entity.")
            st.session_state.pop("cust_loaded_id", None)

        if loaded_id is not None:
            if is_loaded_agent:
                # Agent edit flow
                real_agent_id = int(str(loaded_id)[1:])
                arec = get_agent(real_agent_id)
                if not arec:
                    st.warning("Agent not found.")
                else:
                    st.subheader(f"Agent #{real_agent_id}")
                    st.markdown(f"**Name:** {arec.get('name')}")
                    st.caption(f"Status: {arec.get('status')}")
                    
                    if show_edit_customer:
                        st.divider()
                        from customer_approval import save_approval_draft
                        with st.form(f"edit_agent_manage_{real_agent_id}"):
                            ename = st.text_input("Agent name (changing requires approval)", value=arec.get("name") or "", key="eam_name")
                            eid_number = st.text_input("ID number", value=arec.get("id_number") or "", key="eam_id")
                            eaddr1 = st.text_input("Address line 1", value=arec.get("address_line1") or "", key="eam_a1")
                            eaddr2 = st.text_input("Address line 2", value=arec.get("address_line2") or "", key="eam_a2")
                            ecity = st.text_input("City", value=arec.get("city") or "", key="eam_city")
                            ecountry = st.text_input("Country", value=arec.get("country") or "", key="eam_country")
                            ephone1 = st.text_input("Phone 1", value=arec.get("phone1") or "", key="eam_p1")
                            ephone2 = st.text_input("Phone 2", value=arec.get("phone2") or "", key="eam_p2")
                            eemail = st.text_input("Email", value=arec.get("email") or "", key="eam_email")
                            ecommission = st.number_input("Commission rate %", min_value=0.0, max_value=100.0, value=float(arec.get("commission_rate_pct") or 0), step=0.5, format="%.2f", key="eam_comm")
                            etin = st.text_input("TIN number", value=arec.get("tin_number") or "", key="eam_tin")
                            etax_expiry = st.date_input("Tax clearance expiry", value=arec.get("tax_clearance_expiry"), key="eam_tax")
                            estatus = st.selectbox("Status", ["active", "inactive"], index=0 if (arec.get("status") or "active") == "active" else 1, key="eam_status")
                            e_agent_type_label = st.selectbox("Agent type", ["Individual", "Corporate"], index=0 if (arec.get("agent_type") or "individual") == "individual" else 1, key="eam_type")
                            
                            st.caption("If you change the name, you MUST provide a supporting document reference.")
                            supp_doc = st.text_input("Supporting document link/reference (required if name changed)", key="eam_doc")

                            if st.form_submit_button("Update agent"):
                                try:
                                    old_name = arec.get("name") or ""
                                    if ename.strip() != old_name:
                                        if not supp_doc.strip():
                                            st.error("Supporting document is required for name change.")
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
                                            st.success("Name change draft submitted for approval.")
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
                                    st.success("Agent details updated successfully.")
                                    if (ename.strip() == old_name) or (ename.strip() != old_name and supp_doc.strip()):
                                        st.rerun()
                                except Exception as e:
                                    st.error(f"Could not update agent: {e}")
                                    
                    if show_status_tools:
                        current_status = arec.get("status", "active")
                        new_active = st.radio("Set status", ["active", "inactive"], index=0 if current_status == "active" else 1, key="agt_set_status")
                        if st.button("Update status", key="agt_update_status"):
                            update_agent(real_agent_id, name=arec.get("name"), status=new_active)
                            st.success(f"Status set to **{new_active}**.")
                            st.session_state["cust_loaded_id"] = loaded_id
                            st.rerun()
            else:
                loaded_id = int(loaded_id)
                rec = get_customer(loaded_id)
                if not rec:
                    st.warning("Customer not found.")
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
                        c1, c2 = st.columns(2)
                        with c1:
                            st.write(f"**National ID:** {ind.get('national_id') or '—'}")
                            st.write(f"**Phone 1:** {ind.get('phone1') or '—'}")
                            st.write(f"**Email 1:** {ind.get('email1') or '—'}")
                        with c2:
                            st.write(f"**Employer details:** {ind.get('employer_details') or '—'}")
                            st.write(f"**Phone 2:** {ind.get('phone2') or '—'}")
                            st.write(f"**Email 2:** {ind.get('email2') or '—'}")
                    else:
                        corp = rec.get("corporate") or {}
                        cname = corp.get("trading_name") or corp.get("legal_name") or "—"
                        st.markdown(f"**Corporate name:** {cname}")
                        st.caption(
                            f"Type: {ctype} · Status: {cstatus} · "
                            f"Sector: {sector_id if sector_id is not None else '—'} · "
                            f"Subsector: {subsector_id if subsector_id is not None else '—'}"
                        )
                        c1, c2 = st.columns(2)
                        with c1:
                            st.write(f"**Legal name:** {corp.get('legal_name') or '—'}")
                            st.write(f"**Registration number:** {corp.get('reg_number') or '—'}")
                        with c2:
                            st.write(f"**Trading name:** {corp.get('trading_name') or '—'}")
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
                        from customer_approval import save_approval_draft
                        st.divider()
                        st.subheader("Edit customer details")
                        with st.form(f"edit_customer_form_{loaded_id}"):
                            if ctype == "individual":
                                ename = st.text_input("Full name (changing this requires approval)", value=ind.get("name") or "", key="edit_ind_name")
                                enational_id = st.text_input("National ID", value=ind.get("national_id") or "", key="edit_ind_national_id")
                                ephone1 = st.text_input("Phone 1", value=ind.get("phone1") or "", key="edit_ind_phone1")
                                ephone2 = st.text_input("Phone 2", value=ind.get("phone2") or "", key="edit_ind_phone2")
                                eemail1 = st.text_input("Email 1", value=ind.get("email1") or "", key="edit_ind_email1")
                                eemail2 = st.text_input("Email 2", value=ind.get("email2") or "", key="edit_ind_email2")
                                eemp = st.text_area("Employer details", value=ind.get("employer_details") or "", key="edit_ind_emp")
                            else:
                                ename = st.text_input("Legal name (changing this requires approval)", value=corp.get("legal_name") or "", key="edit_corp_name")
                                etrading = st.text_input("Trading name", value=corp.get("trading_name") or "", key="edit_corp_trading")
                                ereg = st.text_input("Registration number", value=corp.get("reg_number") or "", key="edit_corp_reg")
                                etin = st.text_input("TIN", value=corp.get("tin") or "", key="edit_corp_tin")
                        
                            esector_id, esubsector_id = sector_id, subsector_id
                            if customers_available:
                                sectors_list = list_sectors()
                                subsectors_list = list_subsectors()
                                if sectors_list:
                                    sector_names = ["(None)"] + [s["name"] for s in sectors_list]
                                    curr_sec_name = next((s["name"] for s in sectors_list if s["id"] == sector_id), "(None)")
                                    sel_sector_name = st.selectbox("Sector", sector_names, index=sector_names.index(curr_sec_name) if curr_sec_name in sector_names else 0, key="edit_sector")
                                    esector_id = next((s["id"] for s in sectors_list if s["name"] == sel_sector_name), None) if sel_sector_name != "(None)" else None
                                
                                    subs = [ss for ss in subsectors_list if esector_id and ss["sector_id"] == esector_id]
                                    sub_names = ["(None)"] + [s["name"] for s in subs]
                                    curr_sub_name = next((s["name"] for s in subs if s["id"] == subsector_id), "(None)")
                                    # Fallback index to 0 if the subsector name isn't in the list (e.g. mismatched sector)
                                    s_idx = sub_names.index(curr_sub_name) if curr_sub_name in sub_names else 0
                                    sel_subsector_name = st.selectbox("Subsector", sub_names, index=s_idx, key="edit_subsector")
                                    esubsector_id = next((s["id"] for s in subs if s["name"] == sel_subsector_name), None) if sel_subsector_name != "(None)" else None

                            st.caption("If you change the name, you MUST provide a supporting document reference.")
                            supp_doc = st.text_input("Supporting document link/reference (required if name changed)", key="edit_supp_doc")

                            if st.form_submit_button("Save changes"):
                                try:
                                    if ctype == "individual":
                                        old_name = ind.get("name") or ""
                                        if ename.strip() != old_name:
                                            if not supp_doc.strip():
                                                st.error("Supporting document is required for name change.")
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
                                                st.success("Name change draft submitted for approval.")
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
                                                st.error("Supporting document is required for name change.")
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
                                                st.success("Name change draft submitted for approval.")
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
                                    st.success("Customer details updated successfully.")
                                    # Don't rerun immediately if there was an error with name change validation
                                    if (ename.strip() == old_name) or (ename.strip() != old_name and supp_doc.strip()):
                                        st.rerun()
                                except Exception as e:
                                    st.error(f"Error updating customer: {e}")

                    if show_status_tools:
                        current_status = rec.get("status", "active")
                        new_active = st.radio(
                            "Set status",
                            ["active", "inactive"],
                            index=0 if current_status == "active" else 1,
                            key="cust_set_status",
                        )
                        if st.button("Update status", key="cust_update_status"):
                            set_active(loaded_id, new_active == "active")
                            st.success(f"Status set to **{new_active}**.")
                            st.session_state["cust_loaded_id"] = loaded_id
                            st.rerun()

                    # Direct document upload to corporate sub-entities (separate buckets/IDs).
                    if show_contact_docs_tools and rec.get("type") == "corporate" and documents_available:
                        doc_cats = list_document_categories(active_only=True) or []
                        # Contact person + directors share Individual KYC types plus Other.
                        name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in INDIVIDUAL_DOC_TYPES}

                        if not name_to_cat:
                            st.info("No matching document categories configured for contact/director KYC.")
                        else:
                            cp_list = rec.get("contact_persons") or []
                            dir_list = rec.get("directors") or []

                            if cp_list:
                                st.divider()
                                st.subheader("Contact person documents")
                                cp_options = [(cp["id"], cp.get("full_name") or f"Contact #{cp['id']}") for cp in cp_list]
                                cp_id = st.selectbox(
                                    "Select contact person",
                                    options=[x[0] for x in cp_options],
                                    format_func=lambda i: next((n for (cid, n) in cp_options if cid == i), str(i)),
                                    key=f"cp_doc_pick_{loaded_id}",
                                )

                                cp_doc_type = st.selectbox(
                                    "Document type",
                                    sorted(name_to_cat.keys()),
                                    key=f"cp_doc_type_{loaded_id}",
                                )
                                cp_other_desc = ""
                                if cp_doc_type == "Other":
                                    cp_other_desc = st.text_input(
                                        "Other document name",
                                        key=f"cp_doc_other_{loaded_id}",
                                    )
                                cp_notes = st.text_input(
                                    "Notes (optional)",
                                    key=f"cp_doc_notes_{loaded_id}",
                                )
                                cp_file = st.file_uploader(
                                    "Choose file",
                                    type=["pdf", "png", "jpg", "jpeg"],
                                    key=f"cp_doc_file_{loaded_id}",
                                )
                                if st.button("Upload contact document", key=f"cp_doc_upload_{loaded_id}") and cp_file is not None:
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
                                    st.success("Contact person document uploaded.")

                            if dir_list:
                                st.divider()
                                st.subheader("Director documents")
                                dir_options = [(d["id"], d.get("full_name") or f"Director #{d['id']}") for d in dir_list]
                                dir_id = st.selectbox(
                                    "Select director",
                                    options=[x[0] for x in dir_options],
                                    format_func=lambda i: next((n for (did, n) in dir_options if did == i), str(i)),
                                    key=f"dir_doc_pick_{loaded_id}",
                                )

                                dir_doc_type = st.selectbox(
                                    "Document type",
                                    sorted(name_to_cat.keys()),
                                    key=f"dir_doc_type_{loaded_id}",
                                )
                                dir_other_desc = ""
                                if dir_doc_type == "Other":
                                    dir_other_desc = st.text_input(
                                        "Other document name",
                                        key=f"dir_doc_other_{loaded_id}",
                                    )
                                dir_notes = st.text_input(
                                    "Notes (optional)",
                                    key=f"dir_doc_notes_{loaded_id}",
                                )
                                dir_file = st.file_uploader(
                                    "Choose file",
                                    type=["pdf", "png", "jpg", "jpeg"],
                                    key=f"dir_doc_file_{loaded_id}",
                                )
                                if st.button("Upload director document", key=f"dir_doc_upload_{loaded_id}") and dir_file is not None:
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
                                    st.success("Director document uploaded.")
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
    col_main4, _ = st.columns([1, 1])
    with col_main4:
        if not agents_available:
            st.error(f"Agents module is not available. ({agents_error})")
        else:
            status_agent = st.selectbox("Filter by status", ["active", "inactive", "all"], key="agent_status_filter")
            status_val = None if status_agent == "all" else status_agent
            try:
                agents_list = list_agents(status=status_val)
            except Exception as e:
                st.error(f"Could not load agents: {e}")
                agents_list = []
            if agents_list:
                df_agents = pd.DataFrame(agents_list)
                cols_show = ["id", "name", "id_number", "phone1", "email", "commission_rate_pct", "tax_clearance_expiry", "status"]
                cols_show = [c for c in cols_show if c in df_agents.columns]
                st.dataframe(df_agents[cols_show], width="stretch", hide_index=True)
            else:
                st.info("No agents found.")
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
                st.caption("Enable an action above to add or edit an agent.")

            if show_add_agent:
                st.subheader("Add agent")
                with st.form("add_agent_form", clear_on_submit=True):
                    col_a1, col_a2 = st.columns(2)
                    with col_a1:
                        aname = st.text_input("Agent name *", key="agent_name")
                        atype_label = st.selectbox("Agent type", ["Individual", "Corporate"], key="agent_type")
                        aid_number = st.text_input("ID number", placeholder="e.g. 111111111x11", key="agent_id_number")
                        aaddr1 = st.text_input("Address line 1", key="agent_addr1")
                        acity = st.text_input("City", key="agent_city")
                        aphone1 = st.text_input("Phone 1", key="agent_phone1")
                        aemail = st.text_input("Email", key="agent_email")
                    with col_a2:
                        aaddr2 = st.text_input("Address line 2", key="agent_addr2")
                        acountry = st.text_input("Country", key="agent_country")
                        aphone2 = st.text_input("Phone 2", key="agent_phone2")
                    acommission = st.number_input("Commission rate %", min_value=0.0, max_value=100.0, value=0.0, step=0.5, format="%.2f", key="agent_commission")
                    atin = st.text_input("TIN number", key="agent_tin")
                    atax_expiry = st.date_input("Tax clearance expiry", value=None, key="agent_tax_expiry")
                    # Agent documents (optional)
                    if "agent_docs_staged" not in st.session_state:
                        st.session_state["agent_docs_staged"] = []
                    staged_agent_docs = st.session_state["agent_docs_staged"]
                    with st.expander("Agent documents (optional)"):
                        if documents_available:
                            atype_internal = "individual" if atype_label.lower().startswith("individual") else "corporate"
                            doc_cats = list_document_categories(active_only=True)
                            allowed = AGENT_INDIVIDUAL_DOC_TYPES if atype_internal == "individual" else AGENT_CORPORATE_DOC_TYPES
                            name_to_cat = {c["name"]: c for c in doc_cats if c.get("name") in allowed}
                            if not name_to_cat:
                                st.info("No matching document categories configured for agents.")
                            else:
                                doc_type = st.selectbox(
                                    "Document type",
                                    sorted(name_to_cat.keys()),
                                    key="agent_doc_type",
                                )
                                other_label = ""
                                if doc_type == "Other":
                                    other_label = st.text_input(
                                        "If Other, describe the document",
                                        key="agent_doc_other_label",
                                    )
                                f = st.file_uploader(
                                    "Choose file",
                                    type=["pdf", "png", "jpg", "jpeg"],
                                    key="agent_doc_file",
                                )
                                notes = st.text_input("Notes (optional)", key="agent_doc_notes")
                                add_agent_doc = st.form_submit_button("Save document to list", key="agent_doc_add")
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
                                st.markdown("**Staged documents:**")
                                for idx, row in enumerate(staged_agent_docs, start=1):
                                    st.write(f"{idx}. {row['file'].name} ({row.get('notes') or 'no notes'})")
                        else:
                            st.info("Document module is unavailable.")

                    submitted_create_agent = st.form_submit_button("Create agent")
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
                                    st.success(f"Successfully uploaded {doc_count} agent document(s).")
                            st.session_state["agent_docs_staged"] = []
                            st.success(f"Agent created. Agent ID: **{aid}**.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Could not create agent: {e}")
                    elif submitted_create_agent and not aname.strip():
                        st.warning("Please enter agent name.")

            if show_edit_agent:
                st.divider()
                st.subheader("Edit agent")
                edit_agent_id = st.number_input("Agent ID to edit", min_value=1, value=1, step=1, key="edit_agent_id")
                if st.button("Load agent", key="agent_load_btn"):
                    st.session_state["agent_edit_loaded_id"] = edit_agent_id
                loaded_agent_id = st.session_state.get("agent_edit_loaded_id")
                if loaded_agent_id is not None:
                    arec = get_agent(loaded_agent_id)
                    if not arec:
                        st.warning("Agent not found.")
                        st.session_state.pop("agent_edit_loaded_id", None)
                    else:
                        with st.form("edit_agent_form"):
                            ename = st.text_input("Agent name *", value=arec.get("name") or "", key="edit_agent_name")
                            eid_number = st.text_input("ID number", value=arec.get("id_number") or "", key="edit_agent_id_number")
                            eaddr1 = st.text_input("Address line 1", value=arec.get("address_line1") or "", key="edit_agent_addr1")
                            eaddr2 = st.text_input("Address line 2", value=arec.get("address_line2") or "", key="edit_agent_addr2")
                            ecity = st.text_input("City", value=arec.get("city") or "", key="edit_agent_city")
                            ecountry = st.text_input("Country", value=arec.get("country") or "", key="edit_agent_country")
                            ephone1 = st.text_input("Phone 1", value=arec.get("phone1") or "", key="edit_agent_phone1")
                            ephone2 = st.text_input("Phone 2", value=arec.get("phone2") or "", key="edit_agent_phone2")
                            eemail = st.text_input("Email", value=arec.get("email") or "", key="edit_agent_email")
                            ecommission = st.number_input("Commission rate %", min_value=0.0, max_value=100.0, value=float(arec.get("commission_rate_pct") or 0), step=0.5, format="%.2f", key="edit_agent_commission")
                            etin = st.text_input("TIN number", value=arec.get("tin_number") or "", key="edit_agent_tin")
                            etax_expiry = st.date_input("Tax clearance expiry", value=arec.get("tax_clearance_expiry"), key="edit_agent_tax_expiry")
                            estatus = st.selectbox("Status", ["active", "inactive"], index=0 if (arec.get("status") or "active") == "active" else 1, key="edit_agent_status")
                            e_agent_type_label = st.selectbox(
                                "Agent type",
                                ["Individual", "Corporate"],
                                index=0 if (arec.get("agent_type") or "individual") == "individual" else 1,
                                key="edit_agent_type",
                            )
                            submitted_update_agent = st.form_submit_button("Update agent")
                            st.caption("If you change the name, you MUST provide a supporting document reference.")
                            supp_doc = st.text_input("Supporting document link/reference (required if name changed)", key="edit_agent_supp_doc")

                            if submitted_update_agent and ename.strip():
                                try:
                                    old_name = arec.get("name") or ""
                                    if ename.strip() != old_name:
                                        if not supp_doc.strip():
                                            st.error("Supporting document is required for name change.")
                                        else:
                                            from customer_approval import save_approval_draft
                                            save_approval_draft(
                                                entity_type="agent",
                                                entity_id=loaded_agent_id,
                                                action_type="NAME_CHANGE",
                                                old_details={"name": old_name},
                                                new_details={"name": ename.strip()},
                                                requested_by="System User",
                                                supporting_document=supp_doc.strip()
                                            )
                                            st.success("Name change draft submitted for approval.")
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
                                    st.success("Agent details updated successfully.")
                                    if (ename.strip() == old_name) or (ename.strip() != old_name and supp_doc.strip()):
                                        st.rerun()
                                except Exception as e:
                                    st.error(f"Could not update agent: {e}")
                            elif submitted_update_agent and not ename.strip():
                                st.warning("Please enter agent name.")


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
            "Customer module is not available. Check database connection and install: psycopg2-binary. "
            f"({customers_error})"
        )
        return

    render_green_page_title("Customers")

    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["Add Individual", "Add Corporate", "View & manage", "Agents", "Approvals"]
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

    from customer_approval import approve_draft, dismiss_draft, list_pending_drafts, rework_draft

    if not st.session_state.get("_cust_appr_panel_css"):
        st.session_state["_cust_appr_panel_css"] = True
        st.markdown(
            """
<style>
.cust-appr-stage-lbl {
  font-size: 0.72rem;
  font-weight: 800;
  letter-spacing: 0.055em;
  text-transform: uppercase;
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
            "<div style='color:#16A34A; font-weight:700; font-size:1.38rem; margin:0.08rem 0 0.4rem 0;'>"
            "Customer & Agent Approvals</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            "<div style='font-weight:700; font-size:1.05rem; margin:0.02rem 0 0.32rem 0; color:#334155;'>"
            "Customer & Agent Approvals</div>",
            unsafe_allow_html=True,
        )

    drafts: list = []
    try:
        drafts = list_pending_drafts() or []
    except Exception as e:
        st.error(f"Could not load drafts: {e}")

    with st.container(border=True):
        st.caption(
            "One compact panel: **Stage 1** lists pending items; the **blue rule** separates **Stage 2** "
            "(pick a draft, compare old vs new, apply a decision)."
        )
        st.markdown('<p class="cust-appr-stage-lbl">Stage 1 — Pending queue</p>', unsafe_allow_html=True)
        if not drafts:
            st.info("No pending approval drafts.")
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
                    "requested_by": st.column_config.TextColumn("Requested by", width="medium"),
                    "submitted_at": st.column_config.TextColumn("Submitted", width="medium"),
                },
            )

        st.markdown('<hr class="cust-appr-blue-rule"/>', unsafe_allow_html=True)
        st.markdown('<p class="cust-appr-stage-lbl">Stage 2 — Review & decision</p>', unsafe_allow_html=True)

        if not drafts:
            st.caption("When items appear above, select a draft and choose Approve, Rework, or Dismiss.")
        else:
            dc1, dc2, dc3 = st.columns(3)
            with dc1:
                selected_id = st.selectbox(
                    "Draft",
                    [d["id"] for d in drafts],
                    format_func=lambda i: f"#{int(i)}",
                    key="cust_appr_draft_id",
                )
            with dc2:
                action = st.radio(
                    "Decision",
                    ["Approve", "Rework", "Dismiss"],
                    horizontal=True,
                    key="cust_appr_action",
                )
            with dc3:
                note = st.text_input(
                    "Reviewer note",
                    key="cust_appr_note",
                    placeholder="Required for Rework / Dismiss",
                )

            draft = next((d for d in drafts if d["id"] == selected_id), None)
            if draft:
                old_details = draft.get("old_details") or {}
                new_details = draft.get("new_details") or {}
                supp = (draft.get("supporting_document") or "").strip() or "—"
                st.caption(
                    f"**{draft.get('entity_type', '')}** #{draft.get('entity_id')} · "
                    f"**{draft.get('action_type', '')}** · Supporting doc: {supp}"
                )
                hdr_l = (
                    "<div style='text-align:center;font-size:0.78rem;font-weight:600;color:#64748b;"
                    "margin:0.1rem 0 0.15rem 0;'>Previous (old)</div>"
                )
                hdr_r = (
                    "<div style='text-align:center;font-size:0.78rem;font-weight:600;color:#64748b;"
                    "margin:0.1rem 0 0.15rem 0;'>Proposed (new)</div>"
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

            if st.button("Apply decision", key="cust_appr_submit", type="primary"):
                try:
                    did_mutate = False
                    if action == "Approve":
                        approve_draft(int(selected_id), approved_by="System User")
                        st.success(f"Draft #{int(selected_id)} approved.")
                        did_mutate = True
                    elif action == "Rework":
                        if not str(note or "").strip():
                            st.warning("Note is required for Rework.")
                        else:
                            rework_draft(int(selected_id), str(note).strip(), reworked_by="System User")
                            st.success(f"Draft #{int(selected_id)} sent for rework.")
                            did_mutate = True
                    elif action == "Dismiss":
                        if not str(note or "").strip():
                            st.warning("Note is required for Dismiss.")
                        else:
                            dismiss_draft(int(selected_id), str(note).strip(), dismissed_by="System User")
                            st.success(f"Draft #{int(selected_id)} dismissed.")
                            did_mutate = True
                    if did_mutate:
                        st.rerun()
                except Exception as e:
                    st.error(f"Error applying action: {e}")
