"""Document management UI (classes, categories, uploads, download)."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from style import render_main_header, render_sub_header, render_sub_sub_header


def render_document_management_ui(
    *,
    documents_available: bool,
    documents_error: str,
    list_document_classes,
    create_document_class,
    update_document_class,
    list_document_categories,
    create_document_category,
    update_document_category,
    list_documents,
    get_document,
) -> None:
    if not documents_available:
        st.error(f"Documents module unavailable: {documents_error}")
        return

    from rbac.subfeature_access import document_can_edit, document_can_view

    if not document_can_view() and not document_can_edit():
        st.warning("You do not have permission to use Document management.")
        return

    _tab_labels: list[str] = []
    if document_can_edit():
        _tab_labels.extend(["Document Classes", "Document Categories"])
    if document_can_view():
        _tab_labels.extend(["All Documents", "Generated Documents"])
    _tabs = st.tabs(_tab_labels)

    def _tab_with(label: str):
        if label not in _tab_labels:
            return None
        return _tabs[_tab_labels.index(label)]

    tab_classes = _tab_with("Document Classes")
    tab_categories = _tab_with("Document Categories")
    tab_all_docs = _tab_with("All Documents")
    tab_generated = _tab_with("Generated Documents")

    if tab_classes is not None:
        with tab_classes:
            render_sub_sub_header("Document Classes Configuration")
            st.write("Manage the high-level grouping of documents (e.g. 'Know Your Customer', 'Agreements').")

            with st.expander("Create New Class", expanded=False):
                with st.form("create_doc_class_form"):
                    new_class_name = st.text_input("Class Name", placeholder="e.g. KYC Documents")
                    new_class_desc = st.text_area("Description")
                    if st.form_submit_button("Save Class"):
                        if new_class_name.strip():
                            try:
                                create_document_class(new_class_name.strip(), new_class_desc.strip())
                                st.success(f"Class '{new_class_name}' created.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error creating class: {e}")
                        else:
                            st.error("Class Name is required.")

            classes = list_document_classes(active_only=False)
            if classes:
                df_classes = pd.DataFrame(classes)
                st.dataframe(
                    df_classes[["id", "name", "description", "is_active", "created_at"]],
                    hide_index=True,
                    use_container_width=True,
                )

                render_sub_sub_header("Edit Class")
                edit_class_id = st.selectbox(
                    "Select Class to Edit",
                    [c["id"] for c in classes],
                    format_func=lambda x: next(c["name"] for c in classes if c["id"] == x),
                )
                selected_class = next(c for c in classes if c["id"] == edit_class_id)

                with st.form("edit_doc_class_form"):
                    edit_c_name = st.text_input("Class Name", value=selected_class["name"])
                    edit_c_desc = st.text_area("Description", value=selected_class["description"] or "")
                    edit_c_active = st.checkbox("Is Active?", value=selected_class["is_active"])

                    if st.form_submit_button("Update Class"):
                        if edit_c_name.strip():
                            try:
                                update_document_class(
                                    edit_class_id,
                                    edit_c_name.strip(),
                                    edit_c_desc.strip(),
                                    edit_c_active,
                                )
                                st.success(f"Class '{edit_c_name}' updated.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error updating class: {e}")
                        else:
                            st.error("Class Name is required.")
            else:
                st.info("No document classes found. Create one above.")

    if tab_categories is not None:
        with tab_categories:
            render_sub_sub_header("Document Categories Configuration")
            st.write("Manage the specific types of documents within classes that can be uploaded.")

            active_classes = list_document_classes(active_only=True)
            class_options = {c["id"]: c["name"] for c in active_classes} if active_classes else {}

            with st.expander("Create New Category", expanded=False):
                with st.form("create_doc_cat_form"):
                    if class_options:
                        new_cat_class_id = st.selectbox(
                            "Document Class",
                            options=list(class_options.keys()),
                            format_func=lambda x: class_options[x],
                        )
                    else:
                        st.warning("Please create a Document Class first.")
                        new_cat_class_id = None

                    new_cat_name = st.text_input("Category Name", placeholder="e.g. Identity Document")
                    new_cat_desc = st.text_area("Description")
                    if st.form_submit_button("Save Category"):
                        if new_cat_name.strip() and new_cat_class_id:
                            try:
                                create_document_category(
                                    new_cat_name.strip(),
                                    new_cat_desc.strip(),
                                    new_cat_class_id,
                                )
                                st.success(f"Category '{new_cat_name}' created.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error creating category: {e}")
                        else:
                            st.error("Category Name and Class are required.")

            cats = list_document_categories(active_only=False)
            if cats:
                df_cats = pd.DataFrame(cats)
                display_cols = ["id", "class_name", "name", "description", "is_active", "created_at"]
                display_cols = [c for c in display_cols if c in df_cats.columns]
                st.dataframe(df_cats[display_cols], hide_index=True, use_container_width=True)

                render_sub_sub_header("Edit Category")
                edit_cat_id = st.selectbox(
                    "Select Category to Edit",
                    [c["id"] for c in cats],
                    format_func=lambda x: next(c["name"] for c in cats if c["id"] == x),
                )
                selected_cat = next(c for c in cats if c["id"] == edit_cat_id)

                with st.form("edit_doc_cat_form"):
                    edit_cat_class_id = None
                    if class_options:
                        default_idx = (
                            list(class_options.keys()).index(selected_cat["class_id"])
                            if selected_cat["class_id"] in class_options
                            else 0
                        )
                        edit_cat_class_id = st.selectbox(
                            "Document Class",
                            options=list(class_options.keys()),
                            format_func=lambda x: class_options[x],
                            index=default_idx,
                        )

                    edit_name = st.text_input("Category Name", value=selected_cat["name"])
                    edit_desc = st.text_area("Description", value=selected_cat["description"] or "")
                    edit_active = st.checkbox("Is Active?", value=selected_cat["is_active"])

                    if st.form_submit_button("Update Category"):
                        if edit_name.strip():
                            try:
                                update_document_category(
                                    edit_cat_id,
                                    edit_name.strip(),
                                    edit_desc.strip(),
                                    edit_active,
                                    edit_cat_class_id,
                                )
                                st.success(f"Category '{edit_name}' updated.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error updating category: {e}")
                        else:
                            st.error("Category Name is required.")
            else:
                st.info("No document categories found. Create one above.")

    if tab_all_docs is not None:
        with tab_all_docs:
            render_sub_sub_header("All Uploaded Documents")
            docs = list_documents()
            if docs:
                display_docs = []
                for d in docs:
                    display_docs.append(
                        {
                            "ID": d["id"],
                            "Entity": f"{d['entity_type'].capitalize()} #{d['entity_id']}",
                            "Category": d["category_name"] or "Uncategorized",
                            "File Name": d["file_name"],
                            "Size (KB)": round(d["file_size"] / 1024, 1),
                            "Uploaded At": d["uploaded_at"],
                            "Uploaded By": d["uploaded_by"],
                        }
                    )
                st.dataframe(pd.DataFrame(display_docs), hide_index=True, use_container_width=True)

                render_sub_sub_header("Download Document")
                dl_doc_id = st.selectbox(
                    "Select Document to Download",
                    [d["id"] for d in docs],
                    format_func=lambda x: next(f"ID {d['id']} - {d['file_name']}" for d in docs if d["id"] == x),
                )
                dl_doc = get_document(dl_doc_id)
                if dl_doc:
                    file_content = dl_doc.get("file_content")
                    if file_content is None:
                        file_content = b""
                    elif isinstance(file_content, memoryview):
                        file_content = file_content.tobytes()
                    elif isinstance(file_content, bytearray):
                        file_content = bytes(file_content)
                    elif isinstance(file_content, str):
                        file_content = file_content.encode("utf-8")
                    st.download_button(
                        label=f"Download {dl_doc['file_name']}",
                        data=file_content,
                        file_name=dl_doc["file_name"],
                        mime=dl_doc["file_type"],
                    )
            else:
                st.info("No documents found in the system.")

    if tab_generated is not None:
        with tab_generated:
            render_sub_sub_header("Autogenerated Documents")
            st.info(
                "System-generated quotations, agreements, and offer letters will appear here once "
                "configured in the product rules."
            )
