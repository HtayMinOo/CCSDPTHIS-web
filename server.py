# server.py
import os
import io
import logging
import traceback
from datetime import date

import pandas as pd
import pyodbc
from flask import Flask, request, jsonify, send_file, render_template, send_from_directory, abort
from werkzeug.middleware.proxy_fix import ProxyFix

# Optional CORS (only if ENABLE_CORS=1)
try:
    from flask_cors import CORS
    _cors_available = True
except Exception:
    _cors_available = False

# ------------------- App setup -------------------
app = Flask(
    __name__,
    static_folder="static",
    template_folder="templates"
)

# If behind a proxy (Render, Heroku, etc.)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

# Enable CORS only if explicit environment variable set (safer)
if _cors_available and os.environ.get("ENABLE_CORS", "0") == "1":
    CORS(app)
    app.logger.info("CORS enabled (ENABLE_CORS=1)")

# Basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ------------------- Configuration (use env vars) -------------------
SQL_DRIVER = os.environ.get("SQL_DRIVER", "{ODBC Driver 18 for SQL Server}")
SQL_SERVER = os.environ.get("SQL_SERVER", "HISCOO")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "HISVer3")
SQL_USER = os.environ.get("SQL_USER", "sa")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "ccsdpt")
# Primary table the upload endpoint writes to
UPLOAD_TABLE = os.environ.get("UPLOAD_TABLE", "HISup")        # default from your original file
# Default download table — can be overridden via env var
DOWNLOAD_TABLE = os.environ.get("DOWNLOAD_TABLE", "HisupFinal")

# Optional: If you want to disable encrypt on local dev, set ODBC_ENCRYPT="no"
ODBC_ENCRYPT = os.environ.get("ODBC_ENCRYPT", "no").lower()
TRUST_SERVER_CERT = os.environ.get("TRUST_SERVER_CERT", "yes").lower()

# ------------------- DB connection helper -------------------
def get_db_connection():
    """
    Returns a pyodbc connection using configured environment variables.
    Make sure to set these in production (do not commit credentials).
    """
    # Build connection string
    conn_parts = [
        f"DRIVER={SQL_DRIVER}",
        f"SERVER={SQL_SERVER}",
        f"DATABASE={SQL_DATABASE}",
        f"UID={SQL_USER}",
        f"PWD={SQL_PASSWORD}",
    ]
    # Add encrypt/trust options (explicit)
    conn_parts.append(f"Encrypt={ODBC_ENCRYPT}")
    conn_parts.append(f"TrustServerCertificate={TRUST_SERVER_CERT}")

    conn_str = ";".join(conn_parts) + ";"
    logger.debug(f"Connecting to DB: SERVER={SQL_SERVER} DATABASE={SQL_DATABASE} USER={SQL_USER}")
    return pyodbc.connect(conn_str, autocommit=False)

# ------------------- Health endpoints -------------------
@app.route("/ping")
def ping():
    return "pong", 200

@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok"}), 200

# ------------------- Insert helper -------------------
def insert_data_into_sql(df, table_name=UPLOAD_TABLE):
    """
    Insert dataframe into SQL Server using fast_executemany for speed.
    Expects df to contain columns: Description, Value, Version, Shelter, DateOfRpt
    Returns number of rows inserted (approx via cursor.rowcount).
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.fast_executemany = True

        insert_sql = f"""
            INSERT INTO {table_name} (Description, Value, Version, Shelter, DateOfRpt)
            VALUES (?, ?, ?, ?, ?)
        """

        # Ensure correct column order and types
        data = df[['Description', 'Value', 'Version', 'Shelter', 'DateOfRpt']].values.tolist()
        cursor.executemany(insert_sql, data)
        conn.commit()
        inserted = cursor.rowcount if cursor.rowcount is not None else len(data)
        return inserted
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            logger.exception("Error closing cursor")
        try:
            if conn:
                conn.close()
        except Exception:
            logger.exception("Error closing connection")

# ------------------- Upload Excel Endpoint -------------------
@app.route('/api/upload-excel', methods=['POST'])
def upload_file():
    try:
        # File check
        if 'excelFile' not in request.files:
            return jsonify({'error': 'No file uploaded. field name must be "excelFile"'}), 400

        file = request.files['excelFile']
        shelter = request.form.get('shelter')
        date_of_rpt = request.form.get('dateOfRpt')

        if not shelter or not date_of_rpt:
            return jsonify({'error': 'Shelter and Date of Report are required.'}), 400

        # Read Excel into pandas (sheet 'JSON', columns B/C/D -> index 1,2,3)
        file_stream = io.BytesIO(file.read())
        df = pd.read_excel(
            file_stream,
            sheet_name='JSON',
            header=None,
            skiprows=1,
            usecols=[1, 2, 3]
        )

        if df.empty:
            return jsonify({'error': 'No data found in Excel sheet.'}), 400

        df.columns = ['Description', 'Value', 'Version']

        # Clean/convert
        df['Description'] = df['Description'].astype(str).str.strip()
        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
        df['Version'] = pd.to_numeric(df['Version'], errors='coerce')

        before_drop = len(df)
        df = df.dropna(subset=['Description', 'Value', 'Version'])
        dropped_rows = before_drop - len(df)

        if df.empty:
            return jsonify({'error': 'All rows are invalid or missing required data.'}), 400

        # Add shelter and DateOfRpt
        df['Shelter'] = shelter
        # Ensure DateOfRpt stored as datetime.date (not pandas.Timestamp) for pyodbc compatibility
        try:
            dt = pd.to_datetime(date_of_rpt)
            df['DateOfRpt'] = df.apply(lambda _: dt.date(), axis=1)
        except Exception as e:
            return jsonify({'error': f'Invalid date format for dateOfRpt. Use YYYY-MM-DD. Details: {e}'}), 400

        logger.info("Preview before insert:\n%s", df.head(10).to_string(index=False))
        if dropped_rows > 0:
            logger.info("Skipped %d rows with missing or invalid data.", dropped_rows)

        inserted_rows = insert_data_into_sql(df, table_name=UPLOAD_TABLE)

        return jsonify({
            'message': f'Successfully uploaded {inserted_rows} rows for shelter {shelter} on {date_of_rpt}.',
            'skipped_rows': dropped_rows,
            'preview': df.head(10).to_dict(orient='records')
        }), 200

    except Exception as e:
        logger.exception("Upload failed")
        return jsonify({'error': f'Processing failed: {e}'}), 500

# ------------------- Download Endpoint -------------------
@app.route('/download', methods=['POST'])
def download_data():
    try:
        # Expect JSON body with "shelters": [...], "dates": [...]
        if request.is_json:
            data = request.get_json()
        else:
            # fallback: try form data
            data = request.form.to_dict(flat=False)
            # Normalize: convert single values to list if needed
            if 'shelters' in data and isinstance(data['shelters'], str):
                data['shelters'] = [data['shelters']]
            if 'dates' in data and isinstance(data['dates'], str):
                data['dates'] = [data['dates']]

        shelters = data.get('shelters') or []
        dates = data.get('dates') or []

        if not shelters or not dates:
            return jsonify({'error': 'Shelters and dates are required.'}), 400

        # Normalize shelter strings
        shelters = [str(s).strip() for s in shelters if str(s).strip()]

        # Parse and normalize dates: accept many formats, convert to ISO date strings
        try:
            date_objects = [pd.to_datetime(d).date() for d in dates]
        except Exception as e:
            return jsonify({'error': f'Invalid date(s) provided. Use YYYY-MM-DD. Details: {e}'}), 400

        # Build placeholders and parameters for pyodbc ('?' style)
        placeholders_shelter = ','.join('?' for _ in shelters)
        placeholders_date = ','.join('?' for _ in date_objects)

        query = f"""
            SELECT Description, Value, Shelter, DateOfRpt
            FROM {DOWNLOAD_TABLE}
            WHERE Shelter IN ({placeholders_shelter})
              AND CAST(DateOfRpt AS DATE) IN ({placeholders_date})
            ORDER BY Shelter, DateOfRpt
        """

        params = []
        # Ensure order of params matches placeholders order
        params.extend(shelters)
        params.extend([d.isoformat() for d in date_objects])

        conn = None
        try:
            conn = get_db_connection()
            # pandas will pass params to the DBAPI; format uses '?' placeholders for pyodbc
            df = pd.read_sql_query(query, conn, params=params)
        finally:
            if conn:
                conn.close()

        if df.empty:
            return jsonify({'error': 'No data found for the selected filters.'}), 404

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='HISup')
        output.seek(0)

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='HISup_data.xlsx'
        )

    except Exception as e:
        logger.exception("Download Error")
        return jsonify({'error': f'Failed to generate download: {e}'}), 500

# ------------------- Serve HTML (home) -------------------
@app.route('/')
def home():
    # Prefer templates/his.html if present (so you can move his.html into templates/)
    try:
        # If template exists, render it (allows template features)
        template_path = os.path.join(app.template_folder, 'his.html')
        if os.path.exists(template_path):
            return render_template('his.html')
        # Fallback: return the file content from the filesystem
        html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'his.html')
        if os.path.exists(html_path):
            return open(html_path, encoding='utf-8').read()
        # If file missing, return a clear error
        return jsonify({'error': 'his.html not found on server.'}), 404
    except Exception as e:
        logger.exception("Failed to serve home page")
        return jsonify({'error': f'Failed to serve home page: {e}'}), 500

# ------------------- Template Download Endpoint -------------------
@app.route('/download-template')
def download_template():
    try:
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'HFTallySheet_ENv3.0.xlsx')
        logger.info("Looking for template file at: %s", file_path)
        if not os.path.exists(file_path):
            return jsonify({'error': f'❌ File not found at: {file_path}'}), 404
        return send_file(file_path, as_attachment=True)
    except Exception as e:
        logger.exception("Template Download Error")
        return jsonify({'error': f'Failed to send template: {e}'}), 500

# ------------------- Run Server -------------------
if __name__ == '__main__':
    # Port required by Render and other PaaS platforms
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', '0') == '1'
    app.run(debug=debug, port=port, host='0.0.0.0')

from flask import Flask, jsonify

app = Flask(__name__)

# Example route
@app.route("/")
def home():
    return jsonify({"message": "Hello from Render!"})

# Add more routes here as needed
# @app.route("/data")
# def data():
#     return "Some data"

# This must be at the end of the file
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))  # Render will set the PORT environment variable
    app.run(host="0.0.0.0", port=port)
