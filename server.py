# server.py (patched)
from flask import Flask, request, jsonify, send_file, Response
import os
import pandas as pd
from datetime import datetime, time
import io
import traceback

# Try to import pyodbc (may be missing in environments without driver)
try:
    import pyodbc
except Exception:
    pyodbc = None

app = Flask(__name__)

# ------------------- CONFIG -------------------
UPLOAD_FOLDER = "uploads"
TEMPLATE_FOLDER = "templates"
DATA_FOLDER = "data"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(TEMPLATE_FOLDER, exist_ok=True)
os.makedirs(DATA_FOLDER, exist_ok=True)

# Database configuration via environment variables (do NOT hardcode credentials)
DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
DB_SERVER = os.getenv("DB_SERVER")          # e.g. "HISCOO,1433" or "host:port"
DB_NAME = os.getenv("DB_NAME")
DB_UID = os.getenv("DB_UID")
DB_PWD = os.getenv("DB_PWD")

# Table env vars
UPLOAD_TABLE = os.getenv("UPLOAD_TABLE", "Hisup")
DOWNLOAD_TABLE = os.getenv("DOWNLOAD_TABLE", "HisupFinal")
# Keep TABLE_NAME for compatibility with older code (fallback)
TABLE_NAME = os.getenv("TABLE_NAME", DOWNLOAD_TABLE)

# Column names used for filtering (adjust if your DB uses different names)
DATE_COLUMN = os.getenv("DATE_COLUMN", "Date")
SHELTER_COLUMN = os.getenv("SHELTER_COLUMN", "Shelter")


# --- DB helpers ---
def db_configured():
    """
    Return True if DB credentials appear present and pyodbc is available.
    """
    return (pyodbc is not None) and all([DB_SERVER, DB_NAME, DB_UID, DB_PWD])


def get_connection():
    """
    Create and return a pyodbc connection using the environment variables.
    Raises RuntimeError if DB not configured.
    """
    if not db_configured():
        raise RuntimeError("Database not configured. Set DB_SERVER, DB_NAME, DB_UID, DB_PWD environment variables.")
    conn_str = (
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_UID};"
        f"PWD={DB_PWD};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=False)


# ---------- Helpers ----------
def parse_date_try(v):
    """
    Try to parse a date-like value into a datetime (or None).
    Accepts strings like YYYY-MM-DD and common formats.
    """
    if v is None or (isinstance(v, float) and pd.isna(v)) or (isinstance(v, str) and v.strip() == ""):
        return None
    if isinstance(v, datetime):
        return v
    # Try common formats
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(str(v), fmt)
        except Exception:
            pass
    try:
        return pd.to_datetime(v)
    except Exception:
        return None


# ------------------- ROUTES -------------------

# Serve main page
@app.route("/")
def index():
    return send_file("index.html")  # ensure index.html exists in project root


# Ping route for status
@app.route("/ping")
def ping():
    return "Pong", 200


# Upload Excel: save file locally AND (if DB configured) insert into UPLOAD_TABLE
@app.route("/api/upload-excel", methods=["POST"])
def upload_excel():
    try:
        shelter = request.form.get("shelter", "")
        date_of_rpt = request.form.get("dateOfRpt", "")
        excel_file = request.files.get("excelFile")
        if excel_file is None:
            return jsonify({"error": "No file uploaded (field name 'excelFile' expected)."}), 400

        # Save uploaded file locally
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        safe_shelter = shelter.replace(" ", "_") if shelter else "noshelter"
        safe_date = date_of_rpt.replace(" ", "_") if date_of_rpt else "nodate"
        filename = f"{safe_shelter}_{safe_date}_{timestamp}.xlsx"
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        excel_file.save(filepath)

        # Read into DataFrame
        df = pd.read_excel(filepath)
        preview = df.head().to_dict(orient="records")

        inserted = 0
        if db_configured():
            # Ensure column names are safe for SQL (bracket them in SQL)
            df_cols = list(df.columns)
            # Convert DATE_COLUMN to datetime if present
            if DATE_COLUMN in df_cols:
                df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], errors="coerce")

            columns = [c for c in df_cols]
            cols_escaped = ",".join(f"[{c}]" for c in columns)
            placeholders = ",".join(["?"] * len(columns))
            insert_sql = f"INSERT INTO {UPLOAD_TABLE} ({cols_escaped}) VALUES ({placeholders})"

            values = []
            for _, row in df.iterrows():
                row_vals = []
                for c in columns:
                    v = row[c]
                    if pd.isna(v):
                        row_vals.append(None)
                    else:
                        if isinstance(v, pd.Timestamp):
                            row_vals.append(v.to_pydatetime())
                        else:
                            row_vals.append(v)
                values.append(tuple(row_vals))

            conn = get_connection()
            cur = conn.cursor()
            try:
                try:
                    cur.fast_executemany = True
                except Exception:
                    pass
                cur.executemany(insert_sql, values)
                conn.commit()
                inserted = len(values)
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()
                conn.close()

        return jsonify({
            "message": "Upload successful",
            "preview": preview,
            "saved_file": filepath,
            "inserted_rows": inserted
        }), 201

    except Exception as e:
        tb = traceback.format_exc()
        return jsonify({"error": str(e), "trace": tb}), 500


# Download: query DOWNLOAD_TABLE (preferred) or merge local files as fallback
# Expects JSON POST: {"shelters":["A","B"], "dates":["2025-12-01","2025-12-02"]}
@app.route("/download", methods=["POST"])
def download_data():
    try:
        data = request.get_json(force=True, silent=True) or {}
        shelters = data.get("shelters", [])
        dates = data.get("dates", [])

        # Respect separate env var for download table if present
        DOWNLOAD_TABLE_CURRENT = os.getenv("DOWNLOAD_TABLE", DOWNLOAD_TABLE)

        # If DB configured => query DB
        if db_configured():
            where_clauses = []
            params = []

            # Shelters filter
            if shelters:
                placeholders = ",".join(["?"] * len(shelters))
                where_clauses.append(f"[{SHELTER_COLUMN}] IN ({placeholders})")
                params.extend(shelters)

            # Dates filter
            if dates:
                parsed_dates = []
                for ds in dates:
                    try:
                        d = datetime.strptime(ds, "%Y-%m-%d")
                    except Exception:
                        d = parse_date_try(ds)
                        if d is None:
                            return jsonify({"error": f"Invalid date format: {ds}. Use YYYY-MM-DD"}), 400
                    parsed_dates.append(d.date())

                placeholders = ",".join(["?"] * len(parsed_dates))
                where_clauses.append(f"CONVERT(date, [{DATE_COLUMN}]) IN ({placeholders})")
                params.extend([d.isoformat() for d in parsed_dates])

            # Build SQL: if no filters then select entire table
            if where_clauses:
                where_sql = " AND ".join(where_clauses)
                sql = f"SELECT * FROM {DOWNLOAD_TABLE_CURRENT} WHERE {where_sql}"
            else:
                sql = f"SELECT * FROM {DOWNLOAD_TABLE_CURRENT}"

            # Execute query
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(sql, params)
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
            cur.close()
            conn.close()

            if not rows:
                return jsonify({"error": "No data matching your filters in database"}), 404

            # Build DataFrame and return Excel (in-memory)
            data_rows = []
            for r in rows:
                row = {}
                for i, v in enumerate(r):
                    if isinstance(v, datetime):
                        row[cols[i]] = v.isoformat(sep=" ")
                    else:
                        row[cols[i]] = v
                data_rows.append(row)
            out_df = pd.DataFrame(data_rows, columns=cols)

            out_io = io.BytesIO()
            with pd.ExcelWriter(out_io, engine="openpyxl") as writer:
                out_df.to_excel(writer, index=False, sheet_name="export")
            out_io.seek(0)

            filename = f"{DOWNLOAD_TABLE_CURRENT}_export_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
            # Flask send_file for in-memory BytesIO uses download_name for newer Flask versions
            return send_file(out_io, download_name=filename, as_attachment=True,
                             mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # Fallback to merging local uploaded files if DB not configured
        else:
            all_files = [os.path.join(UPLOAD_FOLDER, f) for f in os.listdir(UPLOAD_FOLDER) if f.endswith(".xlsx")]
            if not all_files:
                return jsonify({"error": "No uploaded files found and DB not configured"}), 404
            df_list = [pd.read_excel(f) for f in all_files]
            merged_df = pd.concat(df_list, ignore_index=True)
            if DATE_COLUMN not in merged_df.columns or SHELTER_COLUMN not in merged_df.columns:
                return jsonify({"error": f"Local files must contain columns '{DATE_COLUMN}' and '{SHELTER_COLUMN}'"}), 400
            merged_df[DATE_COLUMN] = pd.to_datetime(merged_df[DATE_COLUMN], errors="coerce").dt.date

            filtered_df = merged_df
            if shelters:
                filtered_df = filtered_df[filtered_df[SHELTER_COLUMN].isin(shelters)]
            if dates:
                filtered_dates = [datetime.strptime(d, "%Y-%m-%d").date() for d in dates]
                filtered_df = filtered_df[filtered_df[DATE_COLUMN].isin(filtered_dates)]

            if filtered_df.empty:
                return jsonify({"error": "No data matching your filters (local files)"}), 404

            out_path = os.path.join(DATA_FOLDER, f"{DOWNLOAD_TABLE_CURRENT}_local_export.xlsx")
            filtered_df.to_excel(out_path, index=False)
            return send_file(out_path, as_attachment=True)

    except Exception as e:
        tb = traceback.format_exc()
        return jsonify({"error": str(e), "trace": tb}), 500


# Download template
@app.route("/download-template")
def download_template():
    template_file = os.path.join(TEMPLATE_FOLDER, "HFTallySheet_ENv3.0.xlsx")
    if os.path.exists(template_file):
        return send_file(template_file, as_attachment=True)
    else:
        return "Template not found", 404


# ------------------- MAIN -------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
