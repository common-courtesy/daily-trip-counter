# streamlit_excel_cleaner.py
import os
import io
import base64
import pandas as pd
import streamlit as st
from io import BytesIO
import csv
from openpyxl.styles import PatternFill, Border, Side
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

def _normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make headers case/spacing insensitive and map provider variants
    to a canonical set that the rest of the code expects.
    """
    def norm(s: str) -> str:
        return str(s).strip().lower().replace("_", " ")

    canon = {
        # times/dates (common)
        "pickup time (local)": "Pickup Time (Local)",
        "pickup time (utc)": "Pickup Time (UTC)",
        "request time (local)": "Request Time (Local)",
        "request time (utc)": "Request Time (UTC)",
        "pickup date (local)": "Pickup Date (Local)",
        "pickup date (utc)": "Pickup Date (UTC)",
        "request date (local)": "Request Date (Local)",
        "request date (utc)": "Request Date (UTC)",

        # your new variants (exact strings from your message)
        "pick up date": "Pickup Date (Local)",
        "pick up time": "Pickup Time (Local)",
        "rider full name": "Rider Name",
        "pick up address": "Pickup Address",
        "drop off address": "Drop-off Address",
        "pickup to drop-off miles": "Pickup to Drop-off Miles",
        "rider phone #": "Passenger Number",
        "dispatcher email": "Dispatcher Email",
        "internal code": "Internal Code",
        "transaction amount": "Transaction Amount",
        "transaction type": "Transaction Type",

        # identity / contact (general)
        "rider phone number": "Passenger Number",
        "employee id": "Employee ID",
        "member id": "Member ID",
        "rider id": "Rider ID",

        # status / notes
        "ride status": "Ride Status",
        "concierge internal note": "Internal Note",
        "expense note": "Internal Note",

        # addresses (other common labels)
        "pickup address": "Pickup Address",
        "drop-off address": "Drop-off Address",
        "drop off address": "Drop-off Address",  # just in case
        "distance (miles)": "Pickup to Drop-off Miles",  # useful fallback
    }

    df = df.rename(columns={c: canon.get(norm(c), c) for c in df.columns})

    # If only "Rider Name" exists, split into First/Last for grouping/sorting
    if "Rider Name" in df.columns and ("First Name" not in df.columns or "Last Name" not in df.columns):
        name_series = df["Rider Name"].fillna("").astype(str)
        parts = name_series.str.strip().str.rsplit(" ", n=1)
        df["First Name"] = parts.str[0].fillna("").str.strip()
        df["Last Name"]  = parts.str[1].fillna("").str.strip()
        one_token = ~name_series.str.contains(" ")
        df.loc[one_token, "Last Name"] = ""

    return df

# --- Add near the top (e.g., under the other constants) ---

UBER_DETAIL_COLUMNS = [
    "Trip/Eats ID","Transaction Timestamp (UTC)","Request Date (UTC)","Request Time (UTC)",
    "Request Date (Local)","Request Time (Local)","Request Type","Pickup Date (UTC)",
    "Pickup Time (UTC)","Pickup Date (Local)","Pickup Time (Local)","Drop-off Date (UTC)",
    "Drop-off Time (UTC)","Drop-off Date (Local)","Drop-off Time (Local)",
    "Request Timezone Offset from UTC","First Name","Last Name","Email","Employee ID",
    "Service","City","Distance (mi)","Haversine Distance (mi)","Duration (min)","Pickup Address",
    "Pickup Latitude","Pickup Longitude","Drop-off Address","Drop Off Latitude","Drop Off Longitude",
    "Ride Status","Expense Code","Expense Memo","Invoices","Program","Group","Payment Method",
    "Transaction Type","Fare in Local Currency (excl. Taxes)","Taxes in Local Currency",
    "Tip in Local Currency","Transaction Amount in Local Currency (incl. Taxes)","Local Currency Code",
    "Fare in USD (excl. Taxes)","Taxes in USD","Tip in USD","Transaction Amount in USD (incl. Taxes)",
    "Estimated Service and Technology Fee (incl. Taxes, if any) in USD","Health Dashboard URL",
    "Invoice Number","Driver First Name","Guest First Name","Guest Last Name","Guest Phone Number",
    "Deductions in Local Currency","Member ID","Plan ID","Network Transaction Id","IsGroupOrder",
    "Fulfilment Type","Country","Cancellation type","Membership Savings(Local Currency)",
    "Granular Service Purpose Type"
]

def build_uber_detail_sheet(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=UBER_DETAIL_COLUMNS)

    raw = df_raw.copy()

    # 🔧 Ensure unique column names before selecting by list
    raw = raw.loc[:, ~raw.columns.duplicated()]

    for col in UBER_DETAIL_COLUMNS:
        if col not in raw.columns:
            raw[col] = ""

    detail = raw[UBER_DETAIL_COLUMNS].copy()
    for c in detail.columns:
        if pd.api.types.is_object_dtype(detail[c]):
            detail[c] = detail[c].fillna("").astype(str).str.strip()
    return detail

# List of columns to hide (delete)
columns_to_hide = [
    "Ride ID", "Pickup Time (UTC)", "Pickup Timezone offset from UTC", "Pickup Date (UTC)",
    "Drop-off Time (Local)", "Drop-off Time (UTC)", "Drop-off Timezone", "Drop-off Date (Local)", "Drop-off Date (UTC)", "Email",
    "Pickup City", "Pickup State", "Pickup Zip Code", "Requester Name",
    "Drop-off City", "Drop-off State", "Drop-off Zip Code",
    "Request Address", "Request City", "Request State", "Request Zip Code",
    "Destination Address", "Destination City", "Destination State", "Destination Zip Code",
    "Duration (minutes)", "Ride Fare", "Ride Fees", "Ride Discounts", "Ride Tip", "Ride Cost",
    "Business Services Fee", "Transaction Date (UTC)", "Transaction Time (UTC)", "Transaction Currency", "Transaction Outcome",
    "Expense Code", "Expense Note", "Ride Type", "Employee ID", "Custom Tag 1", "Custom Tag 2",
    "Fare Type", "Scheduled Ride Id", "Flex Ride Id", "Flex Ride", "Pickup Latitude", "Pickup Longitude",
    "Drop-off Latitude", "Drop-off Longitude",
    "Trip/Eats ID", "Transaction Timestamp (UTC)", "Request Date (UTC)", "Request Time (UTC)", "Request Date (Local)", "Request Time (Local)",
    "Request Type", "Request Timezone Offset from UTC", "Service", "City", "Haversine Distance (mi)", "Duration (min)", "Drop Off Latitude",
    "Drop Off Longitude", "Expense Code", "Invoices", "Program", "Group", "Payment Method", "Fare in Local Currency (excl. Taxes)", "Taxes in Local Currency",
    "Tip in Local Currency", "Taxes in Local Currency", "Tip in Local Currency",
    "Local Currency Code", "Fare in USD (excl. Taxes)", "Taxes in USD", "Tip in USD", "Transaction Amount in USD (incl. Taxes)", "Estimated Service and Technology Fee (incl. Taxes, if any) in USD",
    "Health Dashboard URL", "Invoice Number", "Driver First Name", "Deductions in Local Currency", "Member ID", "Plan ID", "Network Transaction Id",
    "IsGroupOrder", "Fulfilment Type", "Country", "Cancellation type", "Membership Savings(Local Currency)", "Granular Service Purpose Type"
]

DAILY_MIN_COLS = [
    # date/time we might use downstream
    "Pickup Date (Local)", "Request Date (Local)",
    "Pickup Time (Local)", "Request Time (Local)",
    "Pickup Time (UTC)", "Request Time (UTC)",
    # identity
    "First Name", "Last Name", "Rider Name",
    "Passenger Number", "Member ID", "Employee ID", "Rider ID",
    # extras used by the daily builder
    "Pickup Address", "Drop-off Address",
    "Pickup to Drop-off Miles", "Distance (miles)",
    "Transaction Amount", "Dispatcher Email", "Internal Code", "Transaction Type",
    # status / notes
    "Ride Status", "Internal Note",
]

def _ensure_daily_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Make sure df has all DAILY_MIN_COLS and everything is string-like."""
    if df is None or df.empty:
        return pd.DataFrame(columns=DAILY_MIN_COLS)

    out = df.copy()

    # 💡 Drop duplicate column names before reindexing
    out = out.loc[:, ~out.columns.duplicated()]

    # Add any missing columns as empty strings
    for c in DAILY_MIN_COLS:
        if c not in out.columns:
            out[c] = ""

    # Keep only the columns we care about (preserves order)
    out = out[[c for c in DAILY_MIN_COLS if c in out.columns]]

    # Coerce everything to string to avoid type conflicts
    for c in out.columns:
        out[c] = out[c].astype(object).fillna("").astype(str).str.strip()

    return out

internal_note_values = ["FCC", "FCM", "FCSH", "FCSC", "DTF", "DTFCE"]

def _coalesce_duplicate_columns(df: pd.DataFrame, only: list[str] | None = None) -> pd.DataFrame:
    """
    For any duplicated column names, combine them row-wise by taking the first
    non-empty value across the duplicates, keep a single column with that name,
    and drop the rest. If `only` is provided, only coalesce those names.
    """
    cols_list = list(df.columns)
    dup_names = pd.Series(cols_list)[pd.Series(cols_list).duplicated()].unique().tolist()
    if only is not None:
        dup_names = [n for n in dup_names if n in only]

    for name in dup_names:
        # where the first occurrence currently lives
        first_pos = cols_list.index(name)

        # grab all duplicates of this name
        same_name_cols = [c for c in df.columns if c == name]
        block = df[same_name_cols].astype(object).fillna("").astype(str).applymap(str.strip)

        # row-wise: first non-empty wins
        merged = block.apply(lambda row: next((v for v in row if v != ""), ""), axis=1)

        # drop all duplicates of this name
        df = df.drop(columns=same_name_cols)

        # re-insert a single merged column back at the original first position
        df.insert(first_pos, name, merged)

        # refresh list for next iteration
        cols_list = list(df.columns)

    return df

def _col(df: pd.DataFrame, name: str, default: str = "") -> pd.Series:
    """Return a string Series for column `name`, or a default-filled Series if missing."""
    if name in df.columns:
        return df[name].astype(object).fillna(default).astype(str)
    # preserve index/length
    return pd.Series([default] * len(df), index=df.index, dtype="object")


def _is_canceled_series(status_col: pd.Series) -> pd.Series:
    """
    True for any canceled-like status (including refunds).
    """
    s = status_col.fillna("").astype(str).str.upper()
    squeezed = s.str.replace(r"[\s\-_]", "", regex=True)

    canceled_tokens = {
        "CANCELED", "CANCELLED",
        "DRIVERCANCELED", "DRIVER_CANCELLED",
        "USERCANCELED", "USERCANCELLED",
        "RIDERCANCELED", "RIDERCANCELLED",
        "REFUND",  # ← treat refunds like canceled for totals/✓x
    }

    return squeezed.isin(canceled_tokens) | s.str.contains("CANCEL", na=False)

def detect_header(uploaded_file):
    uploaded_file.seek(0)
    for idx in [0, 4, 5]:
        try:
            df = pd.read_csv(uploaded_file, header=idx, nrows=1)
            if any("trip/eats id" in col.lower() for col in df.columns):
                uploaded_file.seek(0)
                print('I found the headers on index:', idx)
                return idx
        except Exception:
            pass
        uploaded_file.seek(0)
    return None

def clean_file_without_headers(df):
    df = _normalize_headers(df)

    # If guest name columns exist, use them
    name_headers = ["First Name", "Last Name", "Guest First Name", "Guest Last Name"]
    if all(h in df.columns for h in name_headers):
        df = df.drop(columns=["First Name", "Last Name"])
        df = df.rename(columns={"Guest First Name": "First Name", "Guest Last Name": "Last Name"})

    # Common renames
    df = df.rename(columns={
        "Distance (mi)": "Pickup to Drop-off Miles",
        "Transaction Amount in Local Currency (incl. Taxes)": "Transaction Amount",
        "Guest Phone Number": "Passenger Number",
        "Expense Memo": "Internal Note",
    })

    # Combine Email variants if present
    if 'Email' in df.columns and 'Requester Email' in df.columns:
        df['Email Info'] = df['Email'].combine_first(df['Requester Email'])
        df.drop(['Email', 'Requester Email'], axis=1, inplace=True)
    elif 'Email' in df.columns:
        df.rename(columns={"Email": "Email Info"}, inplace=True)
    elif 'Requester Email' in df.columns:
        df.rename(columns={"Requester Email": "Email Info"}, inplace=True)

    # Keep everything we might need later
    desired_columns = [
        # date/time
        "Pickup Date (Local)", "Request Date (Local)",
        "Pickup Time (Local)", "Request Time (Local)", "Pickup Time (UTC)", "Request Time (UTC)",
        # identity
        "First Name", "Last Name", "Rider Name",
        "Passenger Number", "Member ID", "Employee ID", "Rider ID",
        # your requested extras
        "Pickup Address", "Drop-off Address",
        "Pickup to Drop-off Miles", "Transaction Amount",
        "Dispatcher Email", "Internal Code", "Transaction Type",
        # status / notes
        "Ride Status", "Internal Note",
    ]
    present = [c for c in desired_columns if c in df.columns]
    return df[present].copy()

def clean_file(uploaded_file):
    try:
        print("\n📥 File received:", uploaded_file.name)
        is_common_courtesy = False

        if uploaded_file.name.endswith(".csv"):
            # ✅ REPLACE your current preview/if-block with this:
            peek = pd.read_csv(uploaded_file, nrows=8, header=None, dtype=str).fillna("")
            uploaded_file.seek(0)
            has_common_courtesy = peek.apply(
                lambda r: r.astype(str).str.contains("common courtesy", case=False, na=False)
            ).any().any()

            if has_common_courtesy:
                header_row = detect_header(uploaded_file)  # your existing helper
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, header=header_row if header_row is not None else 4)
                is_common_courtesy = True
            else:
                df = pd.read_csv(uploaded_file)

            # keep your existing BOM/whitespace cleanup
            df.columns = df.columns.str.replace('\ufeff', '', regex=False).str.strip()

        else:
            # (xlsx branch unchanged)
            df = pd.read_excel(uploaded_file)
            df.columns = df.columns.str.replace('\ufeff', '', regex=False).str.strip()

        # Normalize headers
        df = _normalize_headers(df)

        # Coalesce duplicates introduced by normalization (especially "Internal Note")
        df = _coalesce_duplicate_columns(df, only=["Internal Note"])

        # Keep RAW before hiding/renaming
        df_raw = df.copy()

        # Drop noisy columns
        custom_columns_to_hide = columns_to_hide.copy()
        if is_common_courtesy and "Email" in custom_columns_to_hide:
            custom_columns_to_hide.remove("Email")
        df = df.drop(columns=[c for c in custom_columns_to_hide if c in df.columns], errors="ignore")

        # Now define renames and apply them (do NOT use column_rename_map before this point)
        column_rename_map = {
            "Distance (mi)": "Distance (miles)",
            "Transaction Amount in Local Currency (incl. Taxes)": "Transaction Amount",
            "Guest Phone Number": "Passenger Number",
            "Expense Memo": "Internal Note",
            "Email": "Email Info",
            "Requester Email": "Email Info",
        }
        df.rename(columns=column_rename_map, inplace=True)

        # Make names unique in the cleaned view
        df = df.loc[:, ~df.columns.duplicated()]

        # Handle guest name columns if present
        name_headers = ["First Name", "Last Name", "Guest First Name", "Guest Last Name"]
        if all(h in df.columns for h in name_headers):
            df = df.drop(columns=["First Name", "Last Name"])
            df = df.rename(columns={"Guest First Name": "First Name", "Guest Last Name": "Last Name"})

        # Trim basics (NaN-safe)
        for c in ["First Name", "Last Name", "Passenger Number"]:
            if c in df.columns:
                df[c] = df[c].fillna("").astype(str).str.strip()

        # 🔧 Normalize phone now so downstream is clean
        if "Passenger Number" in df.columns:
            df["Passenger Number"] = _normalize_phone(df["Passenger Number"])

        # Narrow to the columns we actually use for the minimal sheet
        df_clean = clean_file_without_headers(df)

        if df_clean is None or df_clean.empty:
            df_clean = pd.DataFrame(columns=[
                "First Name","Last Name","Passenger Number",
                "Member ID","Employee ID","Rider ID",
                "Pickup Time (Local)","Request Time (Local)","Pickup Time (UTC)","Request Time (UTC)",
                "Ride Status","Internal Note"
            ])

        return df_clean.fillna(""), df_raw.fillna("")


    except Exception as e:
        print("Error:", e)
        return None, None

def _normalize_phone(s: pd.Series) -> pd.Series:
    # to string, strip
    s = s.astype(object).fillna("").astype(str).str.strip()
    # drop trailing ".0" from float-looking values
    s = s.str.replace(r"\.0$", "", regex=True)
    # remove spaces, dashes, parentheses, etc. (keep digits and optional leading +)
    s = s.str.replace(r"[^\d+]", "", regex=True)
    # optional: strip leading US country code for 11-digit numbers
    s = s.str.replace(r"^\+?1(?=\d{10}$)", "", regex=True)
    return s

def build_daily_trip_sheet(df, include_refunds_bottom=True):
    """
    One row per trip. Totals exclude canceled-like rows (CANCELED/DRIVER_CANCELED/REFUND).
    Totals appear only on the last non-canceled trip for each rider (or 0 on last row if all canceled).
    Group/sort order: Internal Note -> First Name -> Last Name -> Rider Phone # -> refunds last -> time.
    """
    import numpy as np
    import pandas as pd

    # choose a time column (prefer local)
    time_cols = ["Pickup Time (Local)", "Request Time (Local)", "Pickup Time (UTC)", "Request Time (UTC)"]
    time_col = next((c for c in time_cols if c in df.columns), None)
    df = df.copy()
    df["_TIME_"] = df[time_col].fillna("").astype(str).str.strip() if time_col else ""

    # date column (optional)
    date_col = next((c for c in ["Pickup Date (Local)", "Request Date (Local)"] if c in df.columns), None)
    df["_DATE_"] = df[date_col].fillna("").astype(str).str.strip() if date_col else ""

    # --- names ---
    first = df.get("First Name", pd.Series([""] * len(df))).fillna("").astype(str).str.strip()
    last  = df.get("Last Name",  pd.Series([""] * len(df))).fillna("").astype(str).str.strip()
    if (first.eq("").all() and last.eq("").all()) and ("Rider Name" in df.columns):
        name_series = df["Rider Name"].fillna("").astype(str)
        parts = name_series.str.strip().str.rsplit(" ", n=1)
        first = parts.str[0].fillna("").str.strip()
        last  = parts.str[1].fillna("").str.strip()
        first = first.mask(first.eq(name_series), name_series)
        last  = last.mask(first.eq(name_series), "")

    phone = df.get("Passenger Number", pd.Series([""] * len(df))).fillna("").astype(str).str.strip()
    phone = _normalize_phone(phone)

    # ---- status / refunds ----
    status_show = df.get("Ride Status", pd.Series([""] * len(df))).fillna("").astype(str).str.strip()
    amt_raw = df["Transaction Amount"] if "Transaction Amount" in df.columns else pd.Series([None] * len(df), index=df.index)
    amt_num = pd.to_numeric(pd.Series(amt_raw).astype(str).str.replace(r"[^\d\.\-]", "", regex=True), errors="coerce")
    refund_mask = amt_num.lt(0)
    status_show = status_show.mask(refund_mask, "REFUND")
    is_canceled = _is_canceled_series(status_show) | refund_mask

    # ---- extras ----
    pickup_addr   = _col(df, "Pickup Address")
    dropoff_addr  = _col(df, "Drop-off Address")
    if "Pickup to Drop-off Miles" in df.columns:
        miles = _col(df, "Pickup to Drop-off Miles")
    elif "Distance (miles)" in df.columns:
        miles = _col(df, "Distance (miles)")
    else:
        miles = pd.Series([""] * len(df), index=df.index)
    trans_amount  = _col(df, "Transaction Amount")
    dispatcher    = _col(df, "Dispatcher Email")

    # Use whichever exists for the INTERNAL NOTE/CODE and keep that visible name as "Internal Note"
    internal_source = "Internal Code" if "Internal Code" in df.columns else ("Internal Note" if "Internal Note" in df.columns else None)
    internal_visible = _col(df, internal_source) if internal_source else pd.Series([""] * len(df), index=df.index)

    trans_type    = _col(df, "Transaction Type")

    # ---- output table ----
    trip_taken = np.select([refund_mask, is_canceled], ["-", "x"], default="✓")

    out = pd.DataFrame({
        "Pick up Date": df["_DATE_"],
        "Pick Up Time": df["_TIME_"],
        "First Name": first,
        "Last Name": last,
        "Rider Phone #": phone,
        "Pick Up Address": pickup_addr,
        "Drop Off Address": dropoff_addr,
        "Pickup to Drop of Miles": miles,
        "Transaction Amount": trans_amount,
        "Dispatcher Email": dispatcher,
        "Internal Note": internal_visible,   # show this label in the sheet
        "Transaction Type": trans_type,
        "Ride Status": status_show,
        "Trip Taken": trip_taken,
        "_is_canceled": is_canceled.values,
        "Notes": ''
    })

    # helpers for sorting / filtering
    out["_is_refund"] = status_show.str.upper().eq("REFUND").values
    out["_internal_sort"] = out["Internal Note"].astype(str).str.upper().str.strip()

    # ✅ sort/group: Internal Note → First → Last → Phone → refunds last → time
    sort_cols = ["_internal_sort", "First Name", "Last Name", "Rider Phone #", "_is_refund", "Pick Up Time"]
    out = out.sort_values(by=sort_cols, kind="stable", na_position="last").reset_index(drop=True)

    # --- split into VALID (top) vs INVALID (footer) by allowed internal note tokens ---
    allowed = set(internal_note_values)  # {"FCC","FCM","FCSH","FCSC","DTF","DTFCE"}
    ic = out["_internal_sort"]  # already upper/stripped
    valid_mask = pd.Series(False, index=out.index)
    for token in allowed:
        valid_mask = valid_mask | ic.str.contains(rf"\b{token}\b", regex=True, na=False)

    # Columns to show (hide helper columns)
    show_cols = [c for c in out.columns if c not in ["_is_canceled", "_is_refund", "_internal_sort"]]

    # Refund rows (for optional footer)
    refund_rows = out.loc[out["_is_refund"], show_cols].copy()

    # Build TOP data from valid rows, with "Total Trips" on last non-canceled row per rider
    out_valid = out.loc[valid_mask].copy()
    grp_keys = ["Internal Note", "First Name", "Last Name", "Rider Phone #"]  # keep grouping by internal first
    if not out_valid.empty:
        totals_series = out_valid.loc[~out_valid["_is_canceled"]].groupby(grp_keys, dropna=False).size()
        out_valid["Total Trips"] = ""
        grouped = out_valid.groupby(grp_keys, sort=False, dropna=False)
        blocks = []
        keys_list = list(grouped.groups.keys())
        show_cols_top = [c for c in show_cols if c != "Total Trips"] + ["Total Trips"]

        for i, key in enumerate(keys_list):
            g = grouped.get_group(key).copy()
            g_nc = g.loc[~g["_is_canceled"]]
            if len(g_nc) > 0:
                idx_place = g_nc.index.max()
                g.loc[idx_place, "Total Trips"] = int(totals_series.get(key, 0))
            else:
                g.loc[g.index.max(), "Total Trips"] = 0

            blocks.append(g[show_cols_top])
            if i < len(keys_list) - 1:
                blocks.append(pd.DataFrame([{c: "" for c in show_cols_top}]))

        final_df = pd.concat(blocks, ignore_index=True).fillna("")
    else:
        final_df = out[show_cols].iloc[0:0].copy()

    # --- Footer 1: INVALID internal notes (with spacing) ---
    out_invalid = out.loc[~valid_mask, show_cols].copy()
    if not out_invalid.empty:
        spacer_above = pd.concat([pd.DataFrame([{c: "" for c in show_cols}]) for _ in range(10)], ignore_index=True)
        title_invalid = pd.DataFrame([{c: "" for c in show_cols}])
        title_invalid.iloc[0, 0] = "Internal note is not Forsyth ,Fulton, or an incorrect interal note was added"

        footer_blocks = []
        footer_groups = out_invalid.groupby(grp_keys, dropna=False)
        fkeys = list(footer_groups.groups.keys())
        for i, k in enumerate(fkeys):
            g = footer_groups.get_group(k)
            footer_blocks.append(g)
            if i < len(fkeys) - 1:
                footer_blocks.append(pd.DataFrame([{c: "" for c in show_cols}]))

        bad_rows_spaced = pd.concat(footer_blocks, ignore_index=True)
        spacer_below = pd.concat([pd.DataFrame([{c: "" for c in show_cols}]) for _ in range(10)], ignore_index=True)

        final_df = pd.concat([final_df, spacer_above, title_invalid, bad_rows_spaced, spacer_below],
                             ignore_index=True).fillna("")

    # --- Footer 2: Refunds (optional) ---
    if include_refunds_bottom and not refund_rows.empty:
        final_df = pd.concat([final_df, pd.DataFrame([{c: "" for c in show_cols}])],
                             ignore_index=True)

        title_refunds = pd.DataFrame([{c: "" for c in show_cols}])
        title_refunds.iloc[0, 0] = "Refunds"

        r_blocks = []
        r_groups = refund_rows.groupby(grp_keys, dropna=False)
        rkeys = list(r_groups.groups.keys())
        for i, k in enumerate(rkeys):
            g = r_groups.get_group(k)
            r_blocks.append(g)
            if i < len(rkeys) - 1:
                r_blocks.append(pd.DataFrame([{c: "" for c in show_cols}]))

        refunds_spaced = pd.concat(r_blocks, ignore_index=True)
        final_df = pd.concat([final_df, title_refunds, refunds_spaced],
                             ignore_index=True).fillna("")

    return final_df

def sort_and_merge(file1_obj, file2_obj):
    """
    Use the same robust single-file cleaner for each file, then merge the
    cleaned (minimal) DataFrames. This guarantees Common Courtesy / header
    offsets, guest names, and renames are handled identically.
    """
    import pandas as pd

    # Clean both files with the SAME pipeline
    df1_clean, _ = clean_file(file1_obj)
    df2_clean, _ = clean_file(file2_obj)

    # Guard against None
    if df1_clean is None:
        df1_clean = pd.DataFrame()
    if df2_clean is None:
        df2_clean = pd.DataFrame()

    merged = pd.concat([df1_clean, df2_clean], ignore_index=True)

    # Optional: trim basics again (harmless if already trimmed)
    for c in ["First Name", "Last Name", "Passenger Number"]:
        if c in merged.columns:
            merged[c] = merged[c].astype(str).str.strip()

    # Stable sort for nicer grouping later
    sort_keys = [k for k in ["Last Name", "First Name", "Rider Phone #", "Pickup Time (Local)", "Request Time (Local)"] if k in merged.columns]
    if sort_keys:
        merged = merged.sort_values(by=sort_keys, kind="stable").reset_index(drop=True)

    return merged
        

# --- Streamlit UI ---
st.set_page_config(page_title="Daily Trip Counter", layout="centered")
st.title("📊 Daily Trip Counter")

# Controls
highlight_refunds = st.toggle(
    "Highlight refund rows (yellow)",
    value=False,
    help="When on, any row with a refund will be highlighted in yellow in the export."
)

include_refunds_bottom = st.checkbox(
    "Include Refunds at Bottom",
    value=True,
    help="When checked, refund rows are removed from the main table and listed in a footer section titled ‘Refunds’."
)

col1, col2 = st.columns(2)
with col1:
    uploaded_file_1 = st.file_uploader("File 1 (.xlsx or .csv)", type=["xlsx", "csv"], key="file_1")
with col2:
    uploaded_file_2 = st.file_uploader("File 2 (.xlsx or .csv)", type=["xlsx", "csv"], key="file_2")

run = st.button("🧹 Gather daily count file")

if run:
    if not uploaded_file_1 and not uploaded_file_2:
        st.warning("Please upload at least one file.")
    else:
        # We may need both the cleaned (minimal) df and the raw df
        rider_only_df = None
        uber_detail_df = None
        out_filename = "daily_trips.xlsx"

        # CASE A: both files provided → merge cleaned + merge raw details
        if uploaded_file_1 and uploaded_file_2:
            # Build merged minimal using your helper
            merged_clean = sort_and_merge(uploaded_file_1, uploaded_file_2)

            # Also build a merged RAW to feed the detail sheet (re-run clean_file to get raws)
            df1_clean, df1_raw = clean_file(uploaded_file_1)
            df2_clean, df2_raw = clean_file(uploaded_file_2)
            if df1_raw is None:
                import pandas as pd
                df1_raw = pd.DataFrame()
            if df2_raw is None:
                import pandas as pd
                df2_raw = pd.DataFrame()
            import pandas as pd
            merged_raw = pd.concat([df1_raw, df2_raw], ignore_index=True) if not df1_raw.empty or not df2_raw.empty else pd.DataFrame()

            # Build the rider-only daily sheet from the merged minimal
            rider_only_df = build_daily_trip_sheet(merged_clean, include_refunds_bottom=include_refunds_bottom)
            # Build Uber detail from merged raw
            uber_detail_df = build_uber_detail_sheet(merged_raw)

            out_filename = "daily_trips_merged.xlsx"

        # CASE B: only one file present → clean that one
        else:
            single = uploaded_file_1 if uploaded_file_1 else uploaded_file_2
            cleaned_df, raw_df = clean_file(single)

            if cleaned_df is None or cleaned_df.empty:
                st.error("❌ Could not clean this file or it has no valid rows.")
                st.stop()

            rider_only_df = build_daily_trip_sheet(cleaned_df, include_refunds_bottom=include_refunds_bottom)
            uber_detail_df = build_uber_detail_sheet(raw_df)
            base = (single.name or "daily_trips").rsplit(".", 1)[0]
            out_filename = f"{base}_cleaned.xlsx"

        # ---- Display + Download ----
        if (rider_only_df is None or rider_only_df.empty) and (uber_detail_df is None or uber_detail_df.empty):
            st.info("No trips found.")
        else:
            st.success("✅ Sheets ready.")
            st.dataframe(rider_only_df.head(50) if rider_only_df is not None else None)

        # Build workbook buffer
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            if rider_only_df is not None:
                rider_only_df.to_excel(w, index=False, sheet_name="DailyTrips")
            if uber_detail_df is not None and not uber_detail_df.empty:
                # Uncomment the next line if you want the Uber detail sheet included in the export
                # (Your previous code had it commented out.)
                # uber_detail_df.to_excel(w, index=False, sheet_name="UberDetail")
                pass
        buf.seek(0)

        # --- Apply yellow highlight to refund rows if toggled ON ---
        if highlight_refunds and rider_only_df is not None and not rider_only_df.empty:
            from openpyxl import load_workbook
            from openpyxl.styles import PatternFill

            # Determine which rows are refunds from the preview dataframe
            amt_num = pd.to_numeric(
                rider_only_df["Transaction Amount"].astype(str).str.replace(r"[^\d\.\-]", "", regex=True),
                errors="coerce"
            )
            refund_mask = rider_only_df["Trip Taken"].astype(str).eq("-") | amt_num.lt(0)

            if refund_mask.any():
                wb = load_workbook(buf)
                ws = wb["DailyTrips"]

                fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

                # Excel rows are 1-based and row 1 is header → add 2 to df index
                rows_to_color = (refund_mask[refund_mask].index + 2).tolist()
                max_col = ws.max_column

                for r in rows_to_color:
                    for c in range(1, max_col + 1):
                        ws[f"{get_column_letter(c)}{r}"].fill = fill

                # Save back into a fresh buffer
                buf = BytesIO()
                wb.save(buf)
                buf.seek(0)

        st.download_button(
            "📥 Download",
            buf,
            file_name=out_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
