from __future__ import annotations

from io import BytesIO
from pathlib import Path

import streamlit as st

from auth.service import AuthService, totp_issuer_name
from auth.totp import provisioning_uri, qr_png_bytes
from dal import get_conn, UserRepository
from middleware import set_current_user, clear_current_user
from style import render_sub_sub_header

from ui.streamlit_feedback import run_with_spinner


def _logo_path() -> Path:
    """Same asset names as ``main._logo_path``: files usually live at repo root, not under ``auth/``."""
    auth_dir = Path(__file__).resolve().parent
    project_root = auth_dir.parent
    for base in (project_root, auth_dir):
        for file_name in ("FarndaCred logo with.svg", "FarndaCred logo with.png"):
            candidate = base / file_name
            if candidate.exists():
                return candidate
    return project_root / "FarndaCred logo with.png"


_AUTH_PANEL_KEY = "_farnda_auth_panel"


def _totp_pending() -> dict | None:
    raw = st.session_state.get("_farnda_totp_pending")
    return raw if isinstance(raw, dict) else None


def _clear_totp_pending() -> None:
    st.session_state.pop("_farnda_totp_pending", None)


def _render_totp_challenge_form() -> None:
    p = _totp_pending()
    if not p or p.get("kind") != "challenge" or not p.get("user_id"):
        _clear_totp_pending()
        st.rerun()
        return

    uid = str(p["user_id"])
    st.markdown("**Two-step verification**")
    st.caption("Enter the 6-digit code from your authenticator app, or a one-time recovery code.")
    code = st.text_input("Authenticator or recovery code", key="farnda_totp_code_input", autocomplete="off")
    c1, c2 = st.columns(2, gap="small")
    with c1:
        if st.button("Verify and continue", type="primary", key="farnda_totp_verify"):
            if not (code or "").strip():
                st.error("Enter a code.")
                return
            try:
                conn = get_conn()
                try:
                    auth = AuthService(conn)
                    user, status = run_with_spinner(
                        "Verifying…",
                        lambda: auth.complete_totp_login(uid, code, ip=None, user_agent=None),
                    )
                finally:
                    conn.close()
            except Exception as e:
                st.error(str(e))
                return
            if status != "ok" or not user:
                st.error("Invalid code.")
                return
            _clear_totp_pending()
            set_current_user(user)
            try:
                from db.tenant_registry import bind_default_tenant_context_safely

                bind_default_tenant_context_safely()
            except Exception:
                pass
            st.success(f"Welcome, {user.full_name}!")
            st.rerun()
    with c2:
        if st.button("Cancel", key="farnda_totp_cancel"):
            _clear_totp_pending()
            st.rerun()


def _render_totp_setup_wizard() -> None:
    p = _totp_pending()
    if not p or p.get("kind") != "setup" or not p.get("user_id"):
        _clear_totp_pending()
        st.rerun()
        return

    uid = str(p["user_id"])
    if "secret" not in p:
        from auth.totp import random_totp_secret

        p["secret"] = random_totp_secret()

    secret = str(p["secret"])
    try:
        conn = get_conn()
        try:
            urepo = UserRepository(conn)
            row_user = urepo.get_by_id(uid)
        finally:
            conn.close()
    except Exception as e:
        st.error(str(e))
        return

    if not row_user or not row_user.is_active:
        _clear_totp_pending()
        st.error("Session expired. Sign in again.")
        st.rerun()
        return

    issuer = totp_issuer_name()
    uri = provisioning_uri(secret=secret, email=row_user.email, issuer=issuer)
    st.markdown("**Set up authenticator**")
    st.caption(
        f"Scan the QR in Google Authenticator (or another TOTP app). Issuer shown as **{issuer}**."
    )
    try:
        st.image(BytesIO(qr_png_bytes(uri)), width=220)
    except Exception as e:
        st.warning(f"Could not render QR ({e}). Enter the secret manually in your app (advanced).")
        st.code(secret)

    st.caption("Or open this URI in a TOTP app that supports manual entry (secret is sensitive — do not share).")
    v = st.text_input("Enter the 6-digit code to confirm", key="farnda_totp_enroll_verify", autocomplete="off")
    if st.button("Activate two-step verification", type="primary", key="farnda_totp_enroll_submit"):
        if not (v or "").strip():
            st.error("Enter the verification code.")
            return
        try:
            conn = get_conn()
            try:
                auth = AuthService(conn)
                ok, plain_codes, err = run_with_spinner(
                    "Saving…",
                    lambda: auth.finalize_totp_enrollment(
                        uid,
                        secret,
                        v,
                        ip=None,
                        user_agent=None,
                    ),
                )
            finally:
                conn.close()
        except Exception as e:
            st.error(str(e))
            return
        if not ok:
            st.error(err or "Could not enable two-step verification.")
            return
        # Finish sign-in, then show recovery codes on the main app (full width), not this narrow column.
        try:
            conn2 = get_conn()
            try:
                auth2 = AuthService(conn2)
                user2 = run_with_spinner(
                    "Finishing sign-in…",
                    lambda: auth2.complete_session_after_totp_setup(uid, ip=None, user_agent=None),
                )
            finally:
                conn2.close()
        except Exception as e:
            st.error(str(e))
            return
        if not user2:
            st.error("Could not complete sign-in.")
            return
        _clear_totp_pending()
        st.session_state["_farnda_enrollment_recovery_codes"] = list(plain_codes)
        set_current_user(user2)
        try:
            from db.tenant_registry import bind_default_tenant_context_safely

            bind_default_tenant_context_safely()
        except Exception:
            pass
        st.success("Two-step verification is on. Next screen: save your recovery codes.")
        st.rerun()

    if st.button("Cancel sign-in", key="farnda_totp_setup_cancel"):
        _clear_totp_pending()
        st.rerun()


def _render_recovery_form() -> None:
    render_sub_sub_header("Recover or reset password (super-admin / vendor)")
    st.caption(
        "Use a **one-time recovery code** from when you enabled two-step verification, plus a **new password**. "
        "If you have no codes left, use a database or infrastructure break-glass procedure."
    )
    email = st.text_input("Account email", key="farnda_rec_email", autocomplete="username")
    backup = st.text_input("Recovery code", key="farnda_rec_backup", autocomplete="one-time-code")
    np1 = st.text_input("New password", type="password", key="farnda_rec_np1", autocomplete="new-password")
    np2 = st.text_input("Confirm new password", type="password", key="farnda_rec_np2", autocomplete="new-password")

    if st.button("Reset password", type="primary", key="farnda_rec_submit"):
        if not email or not backup or not np1:
            st.error("All fields are required.")
            return
        if np1 != np2:
            st.error("Passwords do not match.")
            return
        try:
            conn = get_conn()
            try:
                auth = AuthService(conn)
                ok, msg = run_with_spinner(
                    "Updating…",
                    lambda: auth.recover_password_with_backup_code(
                        email=email,
                        backup_code=backup,
                        new_password=np1,
                        ip=None,
                        user_agent=None,
                    ),
                )
            finally:
                conn.close()
        except Exception as e:
            st.error(str(e))
            return
        if ok:
            st.success(msg)
        else:
            st.error(msg)


def login_form():
    if st.session_state.pop("_farnda_reg_just_created", None):
        st.success("Account created. You can now sign in.")
    email = st.text_input("Email", key="login_email", autocomplete="username")
    password = st.text_input("Password", type="password", key="login_password", autocomplete="current-password")

    # One compact row: Sign in | Register | Recovery Codes | Reset Password (equal columns keep labels on one line).
    c_login, c_reg, c_rec, c_rst = st.columns(4, gap="small", vertical_alignment="bottom")
    with c_login:
        sign_in_clicked = st.button(
            "Sign in", type="primary", key="login_submit", use_container_width=True
        )
    with c_reg:
        if st.button("Register", key="auth_link_register", type="tertiary", use_container_width=True):
            st.session_state[_AUTH_PANEL_KEY] = "register"
            st.rerun()
    with c_rec:
        if st.button("Recovery Codes", key="auth_link_recover", type="tertiary", use_container_width=True):
            st.session_state[_AUTH_PANEL_KEY] = "recover"
            st.rerun()
    with c_rst:
        if st.button("Reset Password", key="auth_link_reset", type="tertiary", use_container_width=True):
            st.session_state[_AUTH_PANEL_KEY] = "recover"
            st.rerun()

    if sign_in_clicked:
        if not email or not password:
            st.error("Please enter both email and password.")
            return

        try:
            conn = get_conn()
        except Exception as e:
            st.error(f"Database connection error: {e}")
            return

        try:
            auth = AuthService(conn)
            user, status = run_with_spinner(
                "Signing in…",
                lambda: auth.authenticate(email=email, password=password, ip=None, user_agent=None),
            )
        finally:
            conn.close()

        if status == "locked":
            st.error(
                "Your account is locked due to too many failed attempts. Please try again later or contact an administrator."
            )
            return

        if status != "ok" and status not in ("totp_required", "setup_totp_required"):
            st.error("Invalid email or password.")
            return

        if not user:
            st.error("Invalid email or password.")
            return

        if not user.is_active:
            st.error("Your account is inactive. Please contact an administrator.")
            return

        if status == "totp_required":
            st.session_state["_farnda_totp_pending"] = {"kind": "challenge", "user_id": str(user.id)}
            st.rerun()
            return

        if status == "setup_totp_required":
            st.session_state["_farnda_totp_pending"] = {"kind": "setup", "user_id": str(user.id)}
            st.rerun()
            return

        st.session_state.pop(_AUTH_PANEL_KEY, None)
        set_current_user(user)
        try:
            from db.tenant_registry import bind_default_tenant_context_safely

            bind_default_tenant_context_safely()
        except Exception:
            pass
        st.success(f"Welcome, {user.full_name}!")
        st.rerun()


def registration_form():
    render_sub_sub_header("Register (Borrower)")
    email = st.text_input("Email", key="reg_email")
    full_name = st.text_input("Full name", key="reg_full_name")
    password = st.text_input("Password", type="password", key="reg_password")

    if st.button("Create account"):
        if not (email and full_name and password):
            st.error("All fields are required.")
            return

        try:
            conn = get_conn()
        except Exception as e:
            st.error(f"Database connection error: {e}")
            return

        try:
            users = UserRepository(conn)
            if users.get_by_email(email):
                # Generic error to avoid enumeration
                st.error("Unable to create account.")
                return

            auth = AuthService(conn)

            def _hash_and_create():
                pw_hash = auth.hash_password(password)
                return users.create_user(
                    email=email,
                    password_hash=pw_hash,
                    full_name=full_name,
                    role="BORROWER",
                )

            run_with_spinner("Creating account…", _hash_and_create)
        finally:
            conn.close()

        st.session_state["_farnda_reg_just_created"] = True
        st.session_state[_AUTH_PANEL_KEY] = "login"
        st.rerun()


def auth_page():
    col_left, col_center, col_right = st.columns([1, 2, 1])
    with col_center:
        logo_path = _logo_path()
        if logo_path.exists():
            logo_left, logo_mid, logo_right = st.columns([1, 2, 1])
            with logo_mid:
                st.image(str(logo_path), width=320)
        else:
            st.markdown(
                '<p class="farnda-auth-wordmark-fallback">FarndaCred</p>',
                unsafe_allow_html=True,
            )

        if _totp_pending():
            kind = _totp_pending().get("kind")
            if kind == "challenge":
                _render_totp_challenge_form()
                return
            if kind == "setup":
                _render_totp_setup_wizard()
                return
            _clear_totp_pending()

        if "current_user" in st.session_state:
            u = st.session_state["current_user"]
            st.info(f"Logged in as {u['email']} ({u['role']})")
            if st.button("Log out"):
                clear_current_user()
                st.rerun()
            return

        panel = st.session_state.get(_AUTH_PANEL_KEY, "login")
        if panel not in ("login", "register", "recover"):
            panel = "login"

        if panel == "register":
            if st.button("← Sign in", type="tertiary", key="auth_back_from_register"):
                st.session_state[_AUTH_PANEL_KEY] = "login"
                st.rerun()
            registration_form()
        elif panel == "recover":
            if st.button("← Sign in", type="tertiary", key="auth_back_from_recover"):
                st.session_state[_AUTH_PANEL_KEY] = "login"
                st.rerun()
            _render_recovery_form()
        else:
            login_form()


def render_totp_recovery_regeneration_sidebar(user: dict) -> None:
    """
    SUPERADMIN / VENDOR with 2FA: show how many backup codes exist and allow regeneration
    (requires current TOTP). Plain codes cannot be read back from the DB.
    """
    role = user.get("role") or ""
    if role not in ("SUPERADMIN", "VENDOR") or not user.get("two_factor_enabled"):
        return
    uid = str(user.get("id") or "").strip()
    if not uid:
        return

    with st.sidebar:
        with st.expander("Recovery codes", expanded=False):
            st.caption(
                "Codes are only shown **once** when created. The database stores hashes only, "
                "so lost codes cannot be retrieved. Generate a new set with your authenticator; "
                "any previous **unused** codes stop working."
            )
            try:
                conn = get_conn()
                try:
                    n = UserRepository(conn).count_unused_backup_codes(uid)
                finally:
                    conn.close()
                st.caption(f"Unused codes on file: **{n}**")
            except Exception:
                pass

            pending = st.session_state.get("_farnda_regen_backup_codes")
            if isinstance(pending, list) and pending:
                st.text_area(
                    "New recovery codes (copy now)",
                    value="\n".join(pending),
                    height=140,
                    key="farnda_regen_codes_display",
                )
                if st.button("Done — I saved these", key="farnda_regen_dismiss"):
                    st.session_state.pop("_farnda_regen_backup_codes", None)
                    st.rerun()
                return

            totp = st.text_input(
                "Authenticator code",
                key="farnda_regen_totp",
                autocomplete="one-time-code",
            )
            if st.button("Generate new recovery codes", key="farnda_regen_do"):
                if not (totp or "").strip():
                    st.error("Enter the 6-digit code from your app.")
                    return
                try:
                    conn = get_conn()
                    try:
                        auth = AuthService(conn)
                        ok, plain, err = run_with_spinner(
                            "Generating…",
                            lambda: auth.regenerate_backup_codes(uid, totp),
                        )
                    finally:
                        conn.close()
                except Exception as e:
                    st.error(str(e))
                    return
                if not ok:
                    st.error(err or "Could not generate codes.")
                    return
                st.session_state["_farnda_regen_backup_codes"] = plain
                st.rerun()
