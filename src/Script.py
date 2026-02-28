"""
AdmitGuard Pipeline — Optimized Version
========================================
Improvements over previous version:
  1. Pipeline Status written back to Raw sheet (reliable processed tracking)
  2. Run Log tab for full audit trail of every execution
  3. Exception buffers read from Config sheet (not hardcoded)
  4. Interview sheet uses append logic (preserves panel notes)
  5. Test score validation before merge
  6. Orphan test score detection (scores with no matching candidate)
  7. Duplicate test score handling (keeps highest score per email)
  8. Credentials loaded from env variable OR local file (CI/CD safe)
  9. All steps wrapped in try/except with centralized error logging
 10. Single helper functions — no repeated code
"""

import os
import re
import json
import tempfile
import gspread
import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials

# ════════════════════════════════════════════════════
#  SHEET IDs
# ════════════════════════════════════════════════════
RAW_SHEET_ID        = "1z5yjAEWEry2K3k8YsFB5Q5aIZyCgjjpMnUl6XusEjo4"
OUTPUT_SHEET_ID     = "1JVP-zjlpwIt11jNm9zAbcX_UqtJxsa0MNBW9G0Qs2fk"
TEST_SCORE_SHEET_ID = "1o-ZM4XZb2qdvKNCCgl-fs-xKUW4Op6rjxahAfXk25is"
INTERVIEW_SHEET_ID  = "10AQXOdSMwnY-pbbIBVxyL8zQWCHcnuwvapLrDBzKN10"

# ════════════════════════════════════════════════════
#  TAB NAMES
# ════════════════════════════════════════════════════
REJECTED_TAB  = "Rejected_Records"     # Hard failures
EXCEPTION_TAB = "Exception"            # Borderline — needs manual review
TEST_TAB      = "Sheet1"          # Tab name inside test score sheet
INTERVIEW_TAB = "Sheet1"       
RAW_TAB      = "Sheet1"
CLEAN_TAB    = "Clean_Data"
REJECTED_TAB = "Rejected_Records"
CONFIG_TAB   = "Config"  
RUN_LOG_TAB   = "Run Log"

# ════════════════════════════════════════════════════
#  MANDATORY FIELDS
# ════════════════════════════════════════════════════
MANDATORY_FIELDS = [
    "First Name", "Last Name", "Phone Number", "Email Address",
    "Adhar Number", "Gender", "Date of Birth", "State", "City",
    "Highest Qualification", "Graduation Year", "CGPA", "Total Percentage",
]

# ════════════════════════════════════════════════════
#  RUN TRACKER — collects errors/stats across all steps
# ════════════════════════════════════════════════════
run = {
    "run_id":                 datetime.now().strftime("%Y%m%d-%H%M%S"),
    "start_time":             datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "end_time":               "",
    "raw_rows_read":          0,
    "new_rows_found":         0,
    "clean_written":          0,
    "rejected_written":       0,
    "exception_written":      0,
    "exceptions_approved":    0,
    "interview_added":        0,   # new candidates added this run
    "interview_total":        0,   # total in interview sheet after this run
    "interview_not_selected": 0,   # had scores but below threshold
    "errors":                 [],
    "status":                 "Running",
}

errors = run["errors"]   # shorthand reference


# ════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════

def get_sheet_emails(sheet_obj, tab_name, col="Email Address"):
    """Return a lowercase set of emails from a sheet tab. Empty set on failure."""
    try:
        data = sheet_obj.worksheet(tab_name).get_all_records()
        df   = pd.DataFrame(data)
        if not df.empty and col in df.columns:
            return set(df[col].astype(str).str.strip().str.lower())
    except Exception:
        pass
    return set()


def get_sheet_df(sheet_obj, tab_name):
    """Return a DataFrame from a sheet tab. Empty DataFrame on failure."""
    try:
        data = sheet_obj.worksheet(tab_name).get_all_records()
        return pd.DataFrame(data)
    except Exception:
        return pd.DataFrame()


def write_sheet(sheet_obj, tab_name, df):
    """Clear and rewrite a tab. Skips if df is empty."""
    if df.empty:
        print(f"   ⚠️  Nothing to write → '{tab_name}'")
        return
    df = df.fillna("").astype(str)
    ws = sheet_obj.worksheet(tab_name)
    ws.clear()
    ws.update([df.columns.tolist()] + df.values.tolist())
    print(f"   📝 '{tab_name}' → {len(df)} rows written")


def append_sheet(sheet_obj, tab_name, new_df, existing_df):
    """
    Append only new rows to a sheet tab.
    If the sheet is empty, writes fresh including headers.
    Returns the combined dataframe.
    """
    if new_df.empty:
        print(f"   ℹ️  No new rows to append → '{tab_name}'")
        return existing_df

    ws      = sheet_obj.worksheet(tab_name)
    new_df  = new_df.fillna("").astype(str)

    if existing_df.empty:
        ws.clear()
        ws.update([new_df.columns.tolist()] + new_df.values.tolist())
        print(f"   📝 '{tab_name}' → {len(new_df)} rows written (first write)")
        return new_df

    # Align columns to existing before appending
    for col in existing_df.columns:
        if col not in new_df.columns:
            new_df[col] = ""
    new_df = new_df[existing_df.columns.tolist()]
    ws.append_rows(new_df.values.tolist(), value_input_option="RAW")
    print(f"   📝 '{tab_name}' → {len(new_df)} new rows appended")
    return pd.concat([existing_df, new_df], ignore_index=True)


def ts():
    """Current timestamp string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def section(title):
    print(f"\n{'='*55}")
    print(f"  {title}")
    print(f"{'='*55}")


# ════════════════════════════════════════════════════
#  CONNECT
# ════════════════════════════════════════════════════
section("CONNECT — Google Sheets")

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Load credentials from environment variable (CI/CD) or local file
creds_json = os.environ.get("GOOGLE_CREDENTIALS")
if creds_json:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write(creds_json)
        creds_path = f.name
    print("   🔐 Credentials loaded from environment variable")
else:
    creds_path = "/Users/omshinde/Desktop/Python/Project1/credentials.json"
    print("   🔐 Credentials loaded from credentials.json")

creds  = Credentials.from_service_account_file(creds_path, scopes=scopes)
client = gspread.authorize(creds)

raw_sheet        = client.open_by_key(RAW_SHEET_ID)
output_sheet     = client.open_by_key(OUTPUT_SHEET_ID)
test_score_sheet = client.open_by_key(TEST_SCORE_SHEET_ID)
interview_sheet  = client.open_by_key(INTERVIEW_SHEET_ID)

print(f"   ✅ RAW         → {raw_sheet.title}")
print(f"   ✅ OUTPUT       → {output_sheet.title}")
print(f"   ✅ TEST SCORES  → {test_score_sheet.title}")
print(f"   ✅ INTERVIEW    → {interview_sheet.title}")


# ════════════════════════════════════════════════════
#  STEP 1 — READ RAW DATA + CONFIG
# ════════════════════════════════════════════════════
section("STEP 1 — Read Raw Data & Config")

raw_ws      = raw_sheet.worksheet(RAW_TAB)
raw_data    = raw_ws.get_all_records()
config_data = output_sheet.worksheet(CONFIG_TAB).get_all_records()
config      = {row["Parameter"]: str(row["Value"]) for row in config_data}

df = pd.DataFrame(raw_data)
run["raw_rows_read"] = len(df)
print(f"   📥 {len(df)} raw records loaded")
print(f"   📋 Config: {config}")

if df.empty:
    print("⚠️  Raw sheet is empty. Nothing to process.")
    exit()

# ── Read exception buffers from Config (not hardcoded) ──
EXCEPTION_BUFFER_PCT  = float(config.get("Exception Buffer PCT",  1.0))
EXCEPTION_BUFFER_CGPA = float(config.get("Exception Buffer CGPA", 0.1))

# ── Read thresholds ─────────────────────────────────────
MIN_PCT        = float(config.get("Min Percentage",      60))
MIN_CGPA       = float(config.get("Min CGPA",            6.0))
GRAD_YR_MIN    = int(config.get("Graduation Year Min",   2010))
GRAD_YR_MAX    = int(config.get("Graduation Year Max",   2025))
MAX_EXP        = float(config.get("Max Experience",      40))
MIN_TEST_SCORE = float(config.get("Min Test Score",      40))

# ── Standardise raw fields ───────────────────────────────
df["First Name"]    = df["First Name"].astype(str).str.strip().str.title()
df["Last Name"]     = df["Last Name"].astype(str).str.strip().str.title()
df["Email Address"] = df["Email Address"].astype(str).str.strip().str.lower()
# Strip non-digits first, then remove leading country code if present
# Handles: +919876543210 (13 chars) → 9876543210
#          919876543210  (12 chars) → 9876543210
#          09876543210   (11 chars) → 9876543210 (some forms add leading 0)
#          9876543210    (10 chars) → unchanged
def normalise_phone(raw):
    p = str(raw).strip()
    p = re.sub(r"[^0-9]", "", p)          # remove +, -, spaces, brackets
    if len(p) == 13 and p.startswith("091"):
        p = p[3:]                          # 091XXXXXXXXXX → 10 digits
    elif len(p) == 12 and p.startswith("91"):
        p = p[2:]                          # 91XXXXXXXXXX  → 10 digits
    elif len(p) == 11 and p.startswith("0"):
        p = p[1:]                          # 0XXXXXXXXXX   → 10 digits
    return p

df["Phone Number"] = df["Phone Number"].apply(normalise_phone)
df["Adhar Number"]  = df["Adhar Number"].astype(str).str.replace(r"[^0-9]", "", regex=True).str.strip()
df["State"]         = df["State"].astype(str).str.strip().str.title()
df["City"]          = df["City"].astype(str).str.strip().str.title()

# ── Add Pipeline Status column to raw if missing ─────────
raw_headers = raw_ws.row_values(1)
if "Pipeline Status" not in raw_headers:
    pipeline_status_col = len(raw_headers) + 1
    raw_ws.update_cell(1, pipeline_status_col, "Pipeline Status")
    print("   ➕ 'Pipeline Status' column added to Raw sheet")
else:
    pipeline_status_col = raw_headers.index("Pipeline Status") + 1
    print(f"   📌 'Pipeline Status' column found at col {pipeline_status_col}")

# ── IMPORTANT: Re-read ALL raw values AFTER ensuring Pipeline Status column exists ──
# Must be a fresh read so header_row includes the Pipeline Status column
all_raw_values = raw_ws.get_all_values()
header_row     = all_raw_values[0] if all_raw_values else []

# Compute the column letter ONCE here using the actual col number
# Use gspread's utility correctly — rowcol_to_a1(row, col) returns e.g. "A1"
# We only want the column letter part, so we strip the trailing "1"
_ps_a1         = gspread.utils.rowcol_to_a1(1, pipeline_status_col)   # e.g. "S1" or "AA1"
pipeline_status_col_letter = _ps_a1[:-1]   # "S" or "AA"
print(f"   📌 Pipeline Status column letter: {pipeline_status_col_letter} (col {pipeline_status_col})")


# ════════════════════════════════════════════════════
#  STEP 2 — VALIDATE + SEGREGATE
# ════════════════════════════════════════════════════
section("STEP 2 — Validation & Segregation")

# ── Build already-processed set from Pipeline Status column ──

already_processed = set()
if "Pipeline Status" in header_row:
    ps_col_idx  = header_row.index("Pipeline Status")
    email_col_idx = header_row.index("Email Address") if "Email Address" in header_row else None
    if email_col_idx is not None:
        for data_row in all_raw_values[1:]:
            if len(data_row) > ps_col_idx and data_row[ps_col_idx].strip():
                if len(data_row) > email_col_idx:
                    already_processed.add(data_row[email_col_idx].strip().lower())

print(f"   📌 {len(already_processed)} already-processed email(s) will be skipped")

# ── Load existing output sheets for append operations ────
existing_clean_df    = get_sheet_df(output_sheet, CLEAN_TAB)
existing_rejected_df = get_sheet_df(output_sheet, REJECTED_TAB)
existing_exception_df = get_sheet_df(output_sheet, EXCEPTION_TAB)

clean_rows     = []
rejected_rows  = []
exception_rows = []
seen_emails    = set(already_processed)   # seed with already-processed
seen_adhars    = set()

# Seed seen_adhars from already processed records
for existing_df in [existing_clean_df, existing_rejected_df, existing_exception_df]:
    if not existing_df.empty and "Adhar Number" in existing_df.columns:
        seen_adhars.update(
            existing_df["Adhar Number"].astype(str).str.strip().tolist()
        )

# Build a map of email → actual sheet row number (1-indexed, header = row 1)
# So data row 1 = sheet row 2, data row 2 = sheet row 3, etc.
email_to_sheet_row = {}
if "Email Address" in header_row:
    email_col_idx_raw = header_row.index("Email Address")
    for i, data_row in enumerate(all_raw_values[1:], start=2):   # start=2 skips header
        if len(data_row) > email_col_idx_raw:
            em = data_row[email_col_idx_raw].strip().lower()
            email_to_sheet_row[em] = i

# Collect all status updates — write in ONE batch call at the end
# instead of one update_cell per row (much faster, avoids API rate limits)
pipeline_status_updates = []   # list of (sheet_row_number, status_string)

for index, row in df.iterrows():
    row_email = str(row.get("Email Address", "")).strip().lower()

    # ── Skip already processed ───────────────────────
    if row_email in already_processed:
        print(f"   ⏭️  Skip (already processed): {row_email}")
        continue

    hard_failures = []
    soft_failures = []

    # ── 1. Mandatory fields ──────────────────────────
    for field in MANDATORY_FIELDS:
        val = str(row.get(field, "")).strip()
        if not val or val.lower() in ["nan", "none"]:
            hard_failures.append(f"'{field}' is missing")

    # ── 2. Email format ──────────────────────────────
    if row_email and not re.match(r"^[\w\.-]+@[\w\.-]+\.\w{2,}$", row_email):
        hard_failures.append(f"Invalid email format: '{row_email}'")

    # ── 3. Duplicate email ───────────────────────────
    if row_email in seen_emails:
        hard_failures.append(f"Duplicate email: '{row_email}'")
    else:
        seen_emails.add(row_email)

    # ── 4. Phone ─────────────────────────────────────
    phone = str(row.get("Phone Number", "")).strip()
    if phone and not re.match(r"^[6-9]\d{9}$", phone):
        hard_failures.append(
            f"Invalid phone '{phone}' (must be 10 digits, start with 6-9)"
        )

    # ── 5. Aadhaar ───────────────────────────────────
    adhar = str(row.get("Adhar Number", "")).strip()
    if adhar and not re.match(r"^\d{12}$", adhar):
        hard_failures.append(f"Invalid Aadhaar '{adhar}' (must be 12 digits)")
    if adhar in seen_adhars:
        hard_failures.append(f"Duplicate Aadhaar: '{adhar}'")
    else:
        seen_adhars.add(adhar)

    # ── 6. Percentage ────────────────────────────────
    pct_raw = str(row.get("Total Percentage", "")).strip()
    if pct_raw and pct_raw.lower() not in ["nan", "none", ""]:
        try:
            pct = float(pct_raw)
            if not (0.0 <= pct <= 100.0):
                hard_failures.append(f"Percentage {pct} out of range (0–100)")
            elif pct < (MIN_PCT - EXCEPTION_BUFFER_PCT):
                hard_failures.append(
                    f"Percentage {pct}% is below minimum {MIN_PCT}%"
                )
            elif pct < MIN_PCT:
                soft_failures.append(
                    f"Percentage {pct}% is slightly below minimum {MIN_PCT}% "
                    f"(within {EXCEPTION_BUFFER_PCT}% buffer)"
                )
        except ValueError:
            hard_failures.append(f"Invalid percentage value: '{pct_raw}'")

    # ── 7. CGPA ──────────────────────────────────────
    cgpa_raw = str(row.get("CGPA", "")).strip()
    if cgpa_raw and cgpa_raw.lower() not in ["nan", "none", ""]:
        try:
            cgpa = float(cgpa_raw)
            if not (0.0 <= cgpa <= 10.0):
                hard_failures.append(f"CGPA {cgpa} out of range (0–10)")
            elif cgpa < (MIN_CGPA - EXCEPTION_BUFFER_CGPA):
                hard_failures.append(
                    f"CGPA {cgpa} is below minimum {MIN_CGPA}"
                )
            elif cgpa < MIN_CGPA:
                soft_failures.append(
                    f"CGPA {cgpa} is slightly below minimum {MIN_CGPA} "
                    f"(within {EXCEPTION_BUFFER_CGPA} buffer)"
                )
        except ValueError:
            hard_failures.append(f"Invalid CGPA value: '{cgpa_raw}'")

    # ── 8. Graduation year ───────────────────────────
    try:
        grad_year = int(str(row.get("Graduation Year", 0)).strip() or 0)
        if not (GRAD_YR_MIN <= grad_year <= GRAD_YR_MAX):
            hard_failures.append(
                f"Graduation year {grad_year} out of range ({GRAD_YR_MIN}–{GRAD_YR_MAX})"
            )
    except (ValueError, TypeError):
        hard_failures.append("Invalid graduation year")

    # ── 9. Experience ────────────────────────────────
    exp_raw = str(row.get("Total Experience", "")).strip()
    if exp_raw and exp_raw.lower() not in ["nan", "none", ""]:
        try:
            exp = float(exp_raw)
            if exp < 0 or exp > MAX_EXP:
                hard_failures.append(
                    f"Experience {exp} yrs out of range (0–{MAX_EXP})"
                )
        except ValueError:
            hard_failures.append(f"Invalid experience value: '{exp_raw}'")

    # ── Route ────────────────────────────────────────
    # Convert row to dict and remove Raw-sheet-only columns
    # so they don't pollute Clean / Rejected / Exception sheets
    row_dict = row.to_dict()
    row_dict.pop("Pipeline Status", None)   # belongs only in Raw sheet
    row_dict.pop("Submission ID",   None)   # Tally metadata — not needed in output
    row_dict.pop("Respondent ID",   None)   # Tally metadata — not needed in output
    row_dict.pop("Submitted at",    None)   # Tally metadata — not needed in output

    name      = f"{row.get('First Name','')} {row.get('Last Name','')}".strip()
    sheet_row = email_to_sheet_row.get(row_email)   # actual Google Sheet row number

    if hard_failures:
        row_dict["Rejection Reasons"] = "; ".join(hard_failures)
        row_dict["Rejected At"]       = ts()
        rejected_rows.append(row_dict)
        if sheet_row:
            pipeline_status_updates.append((sheet_row, "Processed - Rejected"))
        print(f"   ❌ {name:<22} → REJECTED  | {'; '.join(hard_failures)}")

    elif soft_failures:
        row_dict["Exception Reason"] = "; ".join(soft_failures)
        row_dict["Status"]           = "Pending"
        row_dict["Reviewer Remark"]  = ""
        row_dict["Flagged At"]       = ts()
        exception_rows.append(row_dict)
        if sheet_row:
            pipeline_status_updates.append((sheet_row, "Processed - Exception"))
        print(f"   ⚠️  {name:<22} → EXCEPTION | {'; '.join(soft_failures)}")

    else:
        row_dict["Processed At"] = ts()
        clean_rows.append(row_dict)
        if sheet_row:
            pipeline_status_updates.append((sheet_row, "Processed - Clean"))
        print(f"   ✅ {name:<22} → CLEAN")

# ── Write all Pipeline Status updates in ONE batch call ─
if pipeline_status_updates:
    print(f"   💾 Writing Pipeline Status for {len(pipeline_status_updates)} candidates...")
    batch_data = []
    for sheet_row, status in pipeline_status_updates:
        # Use pre-computed column letter (computed once above, handles multi-letter cols like AA)
        cell_addr = f"{pipeline_status_col_letter}{sheet_row}"
        batch_data.append({
            "range": cell_addr,
            "values": [[status]]
        })
    raw_ws.batch_update(batch_data, value_input_option="RAW")
    print(f"   ✅ Pipeline Status written for all {len(pipeline_status_updates)} candidates")
    # Print exactly what was written for verification
    for sheet_row, status in pipeline_status_updates:
        print(f"      Row {sheet_row} → {status}")
else:
    print("   ℹ️  No new Pipeline Status updates needed")

# ── Build new-row dataframes ─────────────────────────────
new_clean_df     = pd.DataFrame(clean_rows)
new_rejected_df  = pd.DataFrame(rejected_rows)
new_exception_df = pd.DataFrame(exception_rows)

# ── Strip Raw-sheet-only columns ─────────────────────────
# Pipeline Status belongs ONLY in Raw sheet — strip from all output sheets
# Submission ID / Respondent ID / Submitted at are stripped from Clean + Rejected
# BUT kept in Exception so they can be recovered when an exception is approved
STRIP_ALL    = ["Pipeline Status"]
STRIP_CLEAN_REJECTED = ["Submission ID", "Respondent ID", "Submitted at"]

for _df in [new_clean_df, new_rejected_df, new_exception_df]:
    for _col in STRIP_ALL:
        if _col in _df.columns:
            _df.drop(columns=[_col], inplace=True)

# Strip metadata from Clean and Rejected only (not Exception)
for _df in [new_clean_df, new_rejected_df]:
    for _col in STRIP_CLEAN_REJECTED:
        if _col in _df.columns:
            _df.drop(columns=[_col], inplace=True)

# ── Append new rows to output sheets ────────────────────
clean_df     = append_sheet(output_sheet, CLEAN_TAB,     new_clean_df,     existing_clean_df)
rejected_df  = append_sheet(output_sheet, REJECTED_TAB,  new_rejected_df,  existing_rejected_df)
exception_df = append_sheet(output_sheet, EXCEPTION_TAB, new_exception_df, existing_exception_df)

run["new_rows_found"]   = len(clean_rows) + len(rejected_rows) + len(exception_rows)
run["clean_written"]    = len(clean_rows)
run["rejected_written"] = len(rejected_rows)
run["exception_written"]= len(exception_rows)

print(f"\n   📊 This run: {len(clean_rows)} clean | {len(rejected_rows)} rejected | {len(exception_rows)} exception")


# ════════════════════════════════════════════════════
#  STEP 3 — EXCEPTION HANDLING
# ════════════════════════════════════════════════════
section("STEP 3 — Exception Handling (Approved → Clean Data)")

try:
    live_ex_df = get_sheet_df(output_sheet, EXCEPTION_TAB)

    if live_ex_df.empty or "Status" not in live_ex_df.columns:
        print("   ℹ️  No exception records or Status column missing")
    else:
        live_ex_df["Status"] = live_ex_df["Status"].astype(str).str.strip().str.lower()
        approved_df  = live_ex_df[live_ex_df["Status"] == "approved"].copy()
        pending_df   = live_ex_df[live_ex_df["Status"] == "pending"]
        rejected_ex  = live_ex_df[live_ex_df["Status"] == "rejected"]

        if not approved_df.empty:
            # Only move records not already in Clean Data
            live_clean_emails = get_sheet_emails(output_sheet, CLEAN_TAB)
            approved_df["Email Address"] = (
                approved_df["Email Address"].astype(str).str.strip().str.lower()
            )
            truly_new = approved_df[
                ~approved_df["Email Address"].isin(live_clean_emails)
            ].copy()

            if not truly_new.empty:
                truly_new["Processed At"] = ts()

                # ── Re-attach Tally metadata directly from Raw sheet ─
                # Fresh read of raw sheet as a DataFrame — most reliable approach
                # Uses pandas so column name matching is exact and visible
                raw_full_data = raw_ws.get_all_records()
                raw_full_df   = pd.DataFrame(raw_full_data)

                print(f"   🔍 Raw sheet columns: {raw_full_df.columns.tolist()}")

                # Normalise email in raw for matching
                raw_full_df["Email Address"] = (
                    raw_full_df["Email Address"].astype(str).str.strip().str.lower()
                )

                # Detect which Tally metadata columns actually exist in the raw sheet
                tally_meta_cols = ["Submission ID", "Respondent ID", "Submitted at"]
                existing_meta_cols = [c for c in tally_meta_cols if c in raw_full_df.columns]
                missing_meta_cols  = [c for c in tally_meta_cols if c not in raw_full_df.columns]

                if missing_meta_cols:
                    print(f"   ⚠️  These Tally columns not found in Raw sheet: {missing_meta_cols}")
                    print(f"      Available columns: {raw_full_df.columns.tolist()}")

                if existing_meta_cols:
                    # Build slim lookup df: Email + metadata cols only
                    meta_slim = raw_full_df[["Email Address"] + existing_meta_cols].copy()
                    meta_slim = meta_slim.drop_duplicates(subset=["Email Address"])

                    # Normalise email in truly_new for matching
                    truly_new["Email Address"] = (
                        truly_new["Email Address"].astype(str).str.strip().str.lower()
                    )

                    # Drop existing metadata cols from truly_new to avoid _x/_y
                    for col in existing_meta_cols:
                        if col in truly_new.columns:
                            truly_new = truly_new.drop(columns=[col])

                    # Merge metadata back in
                    truly_new = truly_new.merge(
                        meta_slim, on="Email Address", how="left"
                    )

                    # Verify merge worked — print values for each approved candidate
                    for _, r in truly_new.iterrows():
                        em = r.get("Email Address", "")
                        vals = {c: r.get(c, "") for c in existing_meta_cols}
                        print(f"   🔗 {em} → {vals}")
                else:
                    print("   ⚠️  No Tally metadata columns found — skipping re-attach")

                # ── Drop exception-only columns before moving to Clean Data ──
                truly_new = truly_new.drop(
                    columns=["Exception Reason", "Flagged At",
                             "Status", "Reviewer Remark", "Pipeline Status"], errors="ignore"
                )

                ws_clean = output_sheet.worksheet(CLEAN_TAB)
                truly_new = truly_new.fillna("").astype(str)
                ws_clean.append_rows(truly_new.values.tolist(), value_input_option="RAW")
                print(f"   ✅ {len(truly_new)} approved exception(s) → Clean Data")
                run["exceptions_approved"] = len(truly_new)

                # ── Update Pipeline Status in Raw sheet ──────────────
                ps_updates = []
                for _, exc_row in truly_new.iterrows():
                    exc_email = str(exc_row.get("Email Address", "")).strip().lower()
                    sheet_row = email_to_sheet_row.get(exc_email)
                    if sheet_row:
                        ps_updates.append({
                            "range": f"{pipeline_status_col_letter}{sheet_row}",
                            "values": [["Approved - Clean"]]
                        })

                if ps_updates:
                    raw_ws.batch_update(ps_updates, value_input_option="RAW")
                    print(f"   📌 Pipeline Status updated to 'Approved - Clean' for {len(ps_updates)} row(s)")

                # Remove approved from Exception sheet — keep Pending + Rejected only
                remaining = live_ex_df[live_ex_df["Status"] != "approved"].copy()
                ws_ex = output_sheet.worksheet(EXCEPTION_TAB)
                ws_ex.clear()
                if not remaining.empty:
                    remaining = remaining.fillna("").astype(str)
                    ws_ex.update([remaining.columns.tolist()] + remaining.values.tolist())
                print(f"   🗑️  Approved records removed from Exception sheet")
            else:
                print("   ℹ️  Approved records already in Clean Data — no duplicates added")

        print(f"   ⏳ {len(pending_df)} record(s) Pending review")
        print(f"   ❌ {len(rejected_ex)} exception(s) Rejected by reviewer")

except Exception as e:
    msg = f"Step 3 error: {e}"
    errors.append(msg)
    print(f"   ❌ {msg}")


# ════════════════════════════════════════════════════
#  STEP 4 — MAP TEST SCORES
# ════════════════════════════════════════════════════
section("STEP 4 — Map Test Scores to Clean Data")

try:
    test_raw = test_score_sheet.worksheet(TEST_TAB).get_all_records()
    test_df  = pd.DataFrame(test_raw)

    if test_df.empty:
        print("   ⚠️  Test Scores sheet is empty. Skipping.")
    else:
        print(f"   📋 Test score columns: {test_df.columns.tolist()}")

        # ── Validate test scores before merge ────────────
        test_df["Test Score"] = pd.to_numeric(test_df["Test Score"], errors="coerce")

        invalid_scores = test_df[
            test_df["Test Score"].isna() |
            ~test_df["Test Score"].between(0, 100)
        ]
        if not invalid_scores.empty:
            print(f"   ⚠️  {len(invalid_scores)} invalid score(s) removed before merge:")
            for _, bad in invalid_scores.iterrows():
                print(f"      {bad.get('Email ID','')} → score: {bad.get('Test Score','')}")
            errors.append(f"{len(invalid_scores)} invalid test score(s) found")

        # Keep only valid scores
        test_df = test_df[test_df["Test Score"].between(0, 100)].copy()

        # ── Standardise email ─────────────────────────────
        test_df["Email ID"] = test_df["Email ID"].astype(str).str.strip().str.lower()

        # ── Keep highest score per email ──────────────────
        test_slim = (
            test_df[["Email ID", "Test Score"]]
            .sort_values("Test Score", ascending=False)
            .drop_duplicates(subset=["Email ID"], keep="first")
            .rename(columns={"Email ID": "Email Address"})
        )
        print(f"   📊 {len(test_slim)} unique test scores loaded")

        # ── Detect orphan scores (no matching candidate) ──
        live_clean_df = get_sheet_df(output_sheet, CLEAN_TAB)
        if not live_clean_df.empty and "Email Address" in live_clean_df.columns:
            clean_emails  = set(live_clean_df["Email Address"].astype(str).str.strip().str.lower())
            test_emails   = set(test_slim["Email Address"])
            orphan_emails = test_emails - clean_emails
            if orphan_emails:
                print(f"   ⚠️  {len(orphan_emails)} test score(s) with no matching Clean Data candidate:")
                for em in orphan_emails:
                    print(f"      {em} (may be rejected/exception/not yet processed)")

            # ── Drop old Test Score column to avoid _x/_y ─
            if "Test Score" in live_clean_df.columns:
                live_clean_df = live_clean_df.drop(columns=["Test Score"])

            # ── Merge ─────────────────────────────────────
            live_clean_df["Email Address"] = (
                live_clean_df["Email Address"].astype(str).str.strip().str.lower()
            )
            merged_df = live_clean_df.merge(test_slim, on="Email Address", how="left")

            matched   = merged_df["Test Score"].notna().sum()
            unmatched = len(merged_df) - matched
            print(f"   🔗 {matched}/{len(merged_df)} candidates matched with test scores")

            if unmatched > 0:
                no_score_emails = merged_df[merged_df["Test Score"].isna()]["Email Address"].tolist()
                print(f"   ⚠️  {unmatched} candidate(s) have no test score yet:")
                for em in no_score_emails:
                    print(f"      {em}")

            write_sheet(output_sheet, CLEAN_TAB, merged_df)
            clean_df = merged_df   # update reference for Step 5
            print(f"   ✅ Clean Data updated with Test Score column")
        else:
            print("   ⚠️  Clean Data is empty. Skipping test score mapping.")
            clean_df = pd.DataFrame()

except gspread.exceptions.WorksheetNotFound:
    msg = f"'{TEST_TAB}' tab not found in Test Scores sheet"
    errors.append(msg)
    print(f"   ⚠️  {msg}")
    clean_df = get_sheet_df(output_sheet, CLEAN_TAB)
except Exception as e:
    msg = f"Step 4 error: {e}"
    errors.append(msg)
    print(f"   ❌ {msg}")
    clean_df = get_sheet_df(output_sheet, CLEAN_TAB)


# ════════════════════════════════════════════════════
#  STEP 5 — FINAL SCREENING → INTERVIEW SHEET
# ════════════════════════════════════════════════════
section("STEP 5 — Final Screening → Interview Sheet")

try:
    if clean_df.empty or "Test Score" not in clean_df.columns:
        print("   ⚠️  Clean Data empty or no Test Score column. Skipping.")
    else:
        # ── Load existing interview sheet emails ──────────
        existing_interview_emails = get_sheet_emails(
            interview_sheet, INTERVIEW_TAB, col="Email Address"
        )
        existing_interview_df = get_sheet_df(interview_sheet, INTERVIEW_TAB)
        print(f"   📌 {len(existing_interview_emails)} candidates already in Interview sheet")

        # ── Filter candidates with valid test scores ───────
        scored = clean_df[
            clean_df["Test Score"].notna() &
            (clean_df["Test Score"].astype(str).str.strip() != "")
        ].copy()
        scored["Test Score"] = pd.to_numeric(scored["Test Score"], errors="coerce")
        scored = scored.dropna(subset=["Test Score"])

        # ── Apply threshold ───────────────────────────────
        shortlisted  = scored[scored["Test Score"] >= MIN_TEST_SCORE].copy()
        not_selected = scored[scored["Test Score"] <  MIN_TEST_SCORE].copy()

        # ── Only add candidates NOT already in Interview ──
        shortlisted["Email Address"] = (
            shortlisted["Email Address"].astype(str).str.strip().str.lower()
        )
        new_shortlisted = shortlisted[
            ~shortlisted["Email Address"].isin(existing_interview_emails)
        ].copy()

        # ── Always track these counts for run log ────────
        run["interview_added"]        = 0   # new ones added this run
        run["interview_total"]        = len(existing_interview_emails)   # before this run
        run["interview_not_selected"] = len(not_selected)

        if not new_shortlisted.empty:
            # Sort new additions by score descending
            new_shortlisted = new_shortlisted.sort_values("Test Score", ascending=False)
            new_shortlisted["Interview Status"] = "Pending"
            new_shortlisted["Interview Date"]   = ""
            new_shortlisted["Interviewer"]      = ""

            # Combine with existing and re-rank all together
            combined = pd.concat([existing_interview_df, new_shortlisted], ignore_index=True)
            combined["Test Score"] = pd.to_numeric(combined["Test Score"], errors="coerce")
            combined = combined.sort_values("Test Score", ascending=False).reset_index(drop=True)

            # Re-assign ranks from scratch
            if "Rank" in combined.columns:
                combined = combined.drop(columns=["Rank"])
            combined.insert(0, "Rank", range(1, len(combined) + 1))

            write_sheet(interview_sheet, INTERVIEW_TAB, combined)

            # Update run tracker AFTER write succeeds
            run["interview_added"] = len(new_shortlisted)
            run["interview_total"] = len(combined)
            print(f"   🎯 {len(new_shortlisted)} new candidate(s) added to Interview sheet")
            print(f"   📋 Total in Interview sheet: {len(combined)}")
        else:
            print(f"   ℹ️  No new candidates to add (all {len(existing_interview_emails)} already shortlisted)")

        print(f"   ❌ {len(not_selected)} candidate(s) below test score threshold ({MIN_TEST_SCORE})")

except Exception as e:
    msg = f"Step 5 error: {e}"
    errors.append(msg)
    print(f"   ❌ {msg}")


# ════════════════════════════════════════════════════
#  STEP 6 — WRITE RUN LOG
# ════════════════════════════════════════════════════
section("STEP 6 — Write Run Log")

run["end_time"] = ts()
run["status"]   = "Failed" if len(errors) > 0 and run["new_rows_found"] == 0 \
                  else "Partial" if errors \
                  else "Success"
run["errors"]   = "; ".join(errors) if errors else "None"

log_row = [
    run["run_id"],
    run["start_time"],
    run["end_time"],
    run["raw_rows_read"],
    run["new_rows_found"],
    run["clean_written"],
    run["rejected_written"],
    run["exception_written"],
    run["exceptions_approved"],
    run["interview_added"],        # new candidates added THIS run
    run["interview_total"],        # total in interview sheet after this run
    run["interview_not_selected"], # had scores but below threshold
    run["errors"],
    run["status"],
]

try:
    log_ws      = output_sheet.worksheet(RUN_LOG_TAB)
    log_headers = log_ws.row_values(1)

    if not log_headers:
        # First time — write headers
        headers = [
            "Run ID", "Start Time", "End Time", "Raw Rows Read",
            "New Rows Found", "Clean Written", "Rejected Written",
            "Exception Written", "Exceptions Approved",
            "Interview Added (This Run)", "Interview Total", "Interview Not Selected",
            "Errors", "Status",
        ]
        log_ws.update([headers, log_row])
    else:
        log_ws.append_rows([log_row], value_input_option="RAW")

    print(f"   📒 Run logged → {run['run_id']} | Status: {run['status']}")
    print(f"      Interview Added: {run['interview_added']} | Total: {run['interview_total']} | Not Selected: {run['interview_not_selected']}")

except gspread.exceptions.WorksheetNotFound:
    print(f"   ⚠️  '{RUN_LOG_TAB}' tab not found. Create it in output sheet to enable logging.")
except Exception as e:
    print(f"   ⚠️  Could not write run log: {e}")


# ════════════════════════════════════════════════════
#  FINAL SUMMARY
# ════════════════════════════════════════════════════
total     = run["raw_rows_read"]
new_found = run["new_rows_found"]

print(f"\n{'='*55}")
print(f"  📊  PIPELINE SUMMARY — {run['run_id']}")
print(f"{'='*55}")
print(f"   Total raw rows read       : {total}")
print(f"   New rows this run         : {new_found}")
print(f"   ✅ Clean written          : {run['clean_written']}")
print(f"   ❌ Rejected written       : {run['rejected_written']}")
print(f"   ⚠️  Exception written      : {run['exception_written']}")
print(f"   🔄 Exceptions approved    : {run['exceptions_approved']}")
print(f"   🎯 Interview added (run)  : {run['interview_added']}")
print(f"   📋 Interview total        : {run['interview_total']}")
print(f"   ❌ Interview not selected : {run['interview_not_selected']}")
print(f"   🚦 Run status             : {run['status']}")
if errors:
    print(f"   ⚠️  Errors                 : {'; '.join(errors)}")
print(f"{'='*55}")
print(f"\n🎉 Pipeline complete! [{run['end_time']}]\n")
