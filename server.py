from flask import Flask, request, jsonify, send_file
import os
import pandas as pd
from datetime import datetime

app = Flask(__name__)

# ------------------- CONFIG -------------------
UPLOAD_FOLDER = "uploads"
TEMPLATE_FOLDER = "templates"
DATA_FOLDER = "data"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(TEMPLATE_FOLDER, exist_ok=True)
os.makedirs(DATA_FOLDER, exist_ok=True)

# ------------------- ROUTES -------------------

# Serve main page
@app.route("/")
def index():
    return send_file("index.html")  # Make sure index.html is in the same folder as server.py

# Ping route for status
@app.route("/ping")
def ping():
    return "Pong", 200

# Upload Excel
@app.route("/api/upload-excel", methods=["POST"])
def upload_excel():
    try:
        shelter = request.form["shelter"]
        date_of_rpt = request.form["dateOfRpt"]
        excel_file = request.files["excelFile"]

        # Save uploaded file
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"{shelter}_{date_of_rpt}_{timestamp}.xlsx"
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        excel_file.save(filepath)

        # Optional: preview first 5 rows
        df = pd.read_excel(filepath)
        preview = df.head().to_dict(orient="records")

        return jsonify({"message": "Upload successful", "preview": preview})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

# Download Excel filtered by shelters & dates
# Download: query SQL table (preferred) or merge local files as fallback
# Expects JSON POST: {"shelters":["A","B"], "dates":["2025-12-01","2025-12-02"]}
@app.route("/download", methods=["POST"])
def download_data():
    try:
        data = request.get_json(force=True, silent=True) or {}
        shelters = data.get("shelters", [])
        dates = data.get("dates", [])

        # Respect separate env var for download table if present
        DOWNLOAD_TABLE = os.getenv("DOWNLOAD_TABLE", TABLE_NAME)

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

                # Use CONVERT(date, col) so it works if DB column is datetime
                placeholders = ",".join(["?"] * len(parsed_dates))
                where_clauses.append(f"CONVERT(date, [{DATE_COLUMN}]) IN ({placeholders})")
                # SQL Server accepts ISO date strings for parameters
                params.extend([d.isoformat() for d in parsed_dates])

            # Build SQL: if no filters then select entire table
            if where_clauses:
                where_sql = " AND ".join(where_clauses)
                sql = f"SELECT * FROM {DOWNLOAD_TABLE} WHERE {where_sql}"
            else:
                sql = f"SELECT * FROM {DOWNLOAD_TABLE}"

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

            # Build DataFrame and return Excel
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

            filename = f"{DOWNLOAD_TABLE}_export_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
            return send_file(out_io, download_name=filename, as_attachment=True, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

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

            out_path = os.path.join(DATA_FOLDER, f"{DOWNLOAD_TABLE}_local_export.xlsx")
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
    app.run(host="0.0.0.0", port=5000, debug=True)
