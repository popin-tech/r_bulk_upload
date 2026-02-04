import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from io import BytesIO
import base64
from dataclasses import asdict
from typing import Any, Dict
from openpyxl import Workbook

from flask import Flask, jsonify, request, send_from_directory, render_template, send_file, Response, session, redirect, url_for

from services.auth import AuthError, GoogleUser, verify_google_token
from services.broadciel_client import BroadcielClient
from services.upload_service import UploadParsingError, parse_excel, parse_excel_df, excel_to_campaign_json, generate_excel_from_api_data
from services.campaign_bulk_processor import CampaignBulkProcessor
from services.bh_service import BHService
from services.bh_sync import BHSyncService
from database import db, init_db

app = Flask(__name__)

# Configuration
BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-change-me-please")
app.config["GOOGLE_CLIENT_ID"] = os.getenv("GOOGLE_CLIENT_ID", "")
app.config["BROADCIEL_API_BASE_URL"] = os.getenv(
    "BROADCIEL_API_BASE_URL",
    "https://broadciel.ads.rixbeedesk.com/api/v2",
)
app.config["BROADCIEL_API_KEY"] = os.getenv("BROADCIEL_API_KEY", "")
app.config["BROADCIEL_API_KEY"] = os.getenv("BROADCIEL_API_KEY", "")
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_CONTENT_LENGTH_MB", "20")) * 1024 * 1024
app.config["ENABLE_FRONTEND"] = os.getenv("ENABLE_FRONTEND", "true").lower() == "true"
app.config["CRON_SECRET"] = os.getenv("CRON_SECRET", "f6d0f127521aec64a31c2840ac7039f3")

# Database Config
# Format: mysql+pymysql://user:password@host:port/dbname
connection_name = os.getenv("CLOUDSQL_CONNECTION_NAME")
db_user = os.getenv("DB_USER", "popin")
db_pass = os.getenv("DB_PASS", "popIn_gcp_2026")
db_name = os.getenv("DB_NAME", "budget_hunter")

if connection_name:
    # Cloud Run -> Cloud SQL via Unix Socket
    app.config["SQLALCHEMY_DATABASE_URI"] = f"mysql+pymysql://{db_user}:{db_pass}@/{db_name}?unix_socket=/cloudsql/{connection_name}"
else:
    # Local -> Cloud SQL via Public IP (TCP)
    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("SQLALCHEMY_DATABASE_URI", f"mysql+pymysql://{db_user}:{db_pass}@35.234.61.181:3306/{db_name}")

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ECHO"] = False

# Initialize DB
# SSL Configuration for Cloud SQL
ca_cert_path = BASE_DIR / "server-ca.pem"
if ca_cert_path.exists():
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
        "connect_args": {
            "ssl": {
                "ca": str(ca_cert_path),
                "check_hostname": False,
            }
        }
    }

init_db(app)
_ACCOUNTS_CACHE: list[dict[str, Any]] | None = None
_ALLOWED_EMAILS_CACHE: set[str] | None = None
_TOKEN_BY_EMAIL: dict[str, str] = {}

def _load_accounts() -> list[dict[str, Any]]:
    """Load accounts from config/account.json once and cache them."""
    global _ACCOUNTS_CACHE, _TOKEN_BY_EMAIL

    if _ACCOUNTS_CACHE is not None:
        return _ACCOUNTS_CACHE

    json_path = CONFIG_DIR / "account.json"
    if not json_path.exists():
        app.logger.warning("account.json not found in config folder: %s", json_path)
        _ACCOUNTS_CACHE = []
        _TOKEN_BY_EMAIL = {}
        return _ACCOUNTS_CACHE

    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    _ACCOUNTS_CACHE = data
    _TOKEN_BY_EMAIL = {
        (item.get("email") or "").lower(): item.get("token", "")
        for item in data
        if item.get("email") and item.get("token")
    }
    app.logger.info("Loaded %d accounts from %s", len(_ACCOUNTS_CACHE), json_path)
    return _ACCOUNTS_CACHE

def _load_allowed_emails() -> set[str]:
    """Load allowed user emails from static/allowed_emails.json once and cache them."""
    global _ALLOWED_EMAILS_CACHE

    if _ALLOWED_EMAILS_CACHE is not None:
        return _ALLOWED_EMAILS_CACHE

    json_path = CONFIG_DIR / "allowed_emails.json"
    if not json_path.exists():
        app.logger.warning("allowed_emails.json not found in config folder: %s", json_path)
        _ALLOWED_EMAILS_CACHE = set()
        return _ALLOWED_EMAILS_CACHE

    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Expecting: ["email1@example.com", "email2@example.com", ...]
    emails = {str(e).lower() for e in data if e}
    _ALLOWED_EMAILS_CACHE = emails
    app.logger.info("Loaded %d allowed emails from %s", len(emails), json_path)
    return _ALLOWED_EMAILS_CACHE


def _get_token_for_email(email: str) -> str | None:
    """Lookup Broadciel token by account email from account.json."""
    if not email:
        return None
    _load_accounts()
    return _TOKEN_BY_EMAIL.get(email.lower())

def _get_token_from_header() -> str:
    header = request.headers.get("Authorization", "")
    if header.startswith("Bearer "):
        return header.split(" ", 1)[1]
    return ""


def _require_user() -> GoogleUser:
    # 1. Check Flask Session first
    if "user" in session:
        user_data = session["user"]
        # Reconstruct GoogleUser from session dict
        return GoogleUser(
            email=user_data.get("email"),
            name=user_data.get("name"),
            sub=user_data.get("sub"),
            picture=user_data.get("picture")
        )

    # 2. Check Authorization Header (API access)
    token = _get_token_from_header()
    if not token:
         return _error("No session or token provided.", 401)

    try:
        user = verify_google_token(token, app.config["GOOGLE_CLIENT_ID"])
    except AuthError as exc:
        return _error(str(exc), 401)

    email = (user.email or "").lower()
    allowed_emails = _load_allowed_emails()
    if email not in allowed_emails:
        return _error("You are not authorized to use this app.", 403)
    
    return user

def _broadciel_client(account_email: str = None, raw_token: str = None) -> BroadcielClient:
    return BroadcielClient(
        base_url=app.config["BROADCIEL_API_BASE_URL"],
        api_key=app.config["BROADCIEL_API_KEY"],
        account_email=account_email,
        raw_token=raw_token,
    )


def _error(message: str, status: int):
    payload = {"error": message}
    data = json.dumps(payload, ensure_ascii=False)
    return Response(
        response=data,
        status=status,
        mimetype="application/json; charset=utf-8",
    )
    response = jsonify({"error": message})
    response.status_code = status
    return response

@app.route("/api/login", methods=["POST"])
def api_login():
    """Exchange Google Token for Server-side Session."""
    data = request.get_json() or {}
    token = data.get("token")
    
    if not token:
        return _error("Missing token.", 400)

    try:
        user = verify_google_token(token, app.config["GOOGLE_CLIENT_ID"])
    except AuthError as exc:
        return _error(str(exc), 401)

    email = (user.email or "").lower()
    allowed_emails = _load_allowed_emails()
    if email not in allowed_emails:
        return _error("You are not authorized to use this app.", 403)

    # Set session
    session["user"] = asdict(user)
    session.permanent = True  # Use permanent session (default 31 days)
    
    return jsonify({"status": "ok", "user": asdict(user)})


@app.route("/api/logout", methods=["POST"])
def api_logout():
    """Clear Server-side Session."""
    session.clear()
    return jsonify({"status": "ok"})
@app.route("/api/accounts", methods=["GET"])
def list_accounts():
    user = _require_user()
    if not isinstance(user, GoogleUser):
        return user  # 401 / 403

    accounts = _load_accounts()
    items = [
        {"name": item.get("name"), "email": item.get("email")}
        for item in accounts
        if item.get("email")
    ]
    return jsonify({"accounts": items})


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/api/me", methods=["GET"])
def me():
    user = _require_user()
    if isinstance(user, GoogleUser):
        return jsonify({"user": asdict(user)})
    return user  # error response

@app.route("/api/template", methods=["GET"])
def download_template():
    user = _require_user()
    if not isinstance(user, GoogleUser):
        return user  # 401 / 403

    # File lives in /app/static/campaign_sheet_template.xlsx inside the container
    return send_from_directory(
        "static",
        "campaign_sheet_template.xlsx",
        as_attachment=True,
        download_name="campaign_sheet_template.xlsx",
    )

@app.route("/api/upload-preview", methods=["POST"])
def upload_preview():
    user = _require_user()
    if not isinstance(user, GoogleUser):
        return user

    file = request.files.get("sheet")
    if not file:
        return _error("Missing Excel file (sheet).", 400)

    try:
        preview = parse_excel(file.read())
    except UploadParsingError as exc:
        return _error(str(exc), 400)

    return jsonify({"preview": preview, "uploaded_by": asdict(user)})



@app.route("/api/download-excel", methods=["POST"])
def download_excel():
    user = _require_user()
    if not isinstance(user, GoogleUser):
        return user

    data = request.get_json() or {}
    account_email = data.get("account_email")
    if not account_email:
        return _error("Missing account_email parameter.", 400)

    try:
        # Get Token
        raw_token = _get_token_for_email(account_email)
        if not raw_token:
            return _error(f"No raw token found for account: {account_email}", 400)
            
        client = _broadciel_client(account_email, raw_token)
        
        # Fetch All Data
        app.logger.info(f"Fetching all data for {account_email}...")
        campaigns = client.fetch_all_campaigns()
        groups = client.fetch_all_ad_groups()
        creatives = client.fetch_all_ad_creatives()
        
        # Fetch AI Audiences for ID->Name mapping (Download)
        audience_id_map = {}
        try:
            audiences = client.fetch_ai_audiences()
            # Construct map: id -> name
            for item in audiences:
                a_name = item.get("audience_name")
                a_id = item.get("audience_id")
                if a_name and a_id:
                    audience_id_map[int(a_id)] = str(a_name).strip()
            app.logger.info(f"Loaded {len(audience_id_map)} AI audiences for download mapping.")
        except Exception as e:
            app.logger.warning(f"Failed to fetch AI audiences: {e}. ID mapping will be disabled.")

        app.logger.info(f"Fetched {len(campaigns)} campaigns, {len(groups)} groups, {len(creatives)} creatives.")
        
        # Generate Excel
        excel_bytes = generate_excel_from_api_data(campaigns, groups, creatives, audience_id_map=audience_id_map)
        
        # Base64 Encode
        b64_data = base64.b64encode(excel_bytes).decode('utf-8')
        
        filename = f"structure_{account_email}_{len(campaigns)}cpg.xlsx"
        
        return jsonify({
            "status": "ok",
            "file_base64": b64_data,
            "filename": filename
        })
        
    except Exception as e:
        app.logger.error(f"Download failed: {str(e)}")
        # Print stack trace for debugging
        import traceback
        traceback.print_exc()
        return _error(f"Download failed: {str(e)}", 500)


@app.route("/api/commit", methods=["POST"])
def commit():
    # 1) auth
    user = _require_user()
    if not isinstance(user, GoogleUser):
        return user

    # 2) get excel file and account_email from form
    upload = request.files.get("file")
    if upload is None or upload.filename == "":
        return _error("No Excel file uploaded.", 400)
    
    account_email = request.form.get("account_email")
    if not account_email:
        return _error("Missing account_email parameter.", 400)

    try:
        # 3) read bytes
        file_bytes = upload.read()

        # 4) get raw token and create client with auto token exchange (moved up)
        raw_token = _get_token_for_email(account_email)
        if not raw_token:
            return _error(f"No raw token found for account: {account_email}", 400)
        
        client = _broadciel_client(account_email, raw_token)

        # 5) Fetch AI Audiences for name mapping
        audience_map = {}
        try:
            audiences = client.fetch_ai_audiences()
            # Construct map: name -> id. 
            # Note: The user said "audience_name" in API response maps to "audience_id"
            for item in audiences:
                a_name = item.get("audience_name")
                a_id = item.get("audience_id")
                if a_name and a_id:
                    audience_map[str(a_name).strip()] = int(a_id)
            app.logger.info(f"Loaded {len(audience_map)} AI audiences for mapping.")
        except Exception as e:
            app.logger.warning(f"Failed to fetch AI audiences: {e}. Name mapping will be disabled.")
            print(f"Failed to fetch AI audiences: {e}")

        # 6) full DataFrame
        df = parse_excel_df(file_bytes)

        # 7) to JSON (campaign only), passing audience_map
        campaign_payload = excel_to_campaign_json(df, audience_name_map=audience_map)
        app.logger.info("=== Campaign JSON Parsed ===")
        # Always log as JSON string (copy-paste ready)
        import json
        app.logger.info(json.dumps(campaign_payload, ensure_ascii=False, indent=2))   # Cloud Run logs
        print("=== Campaign JSON Parsed ===")
        print(json.dumps(campaign_payload, ensure_ascii=False, indent=2))
        
        # 8) process campaigns using CampaignBulkProcessor
        processor = CampaignBulkProcessor(client)
        processing_result = processor.process_bulk_campaigns(campaign_payload)
        
        app.logger.info("=== Campaign Processing Complete ===")
        app.logger.info(json.dumps(processing_result, ensure_ascii=False, indent=2))
        print("=== Campaign Processing Complete ===")
        print(json.dumps(processing_result, ensure_ascii=False, indent=2))
        
        # 8) return processing results
        return jsonify({
            "status": "ok",
            "account_email": account_email,
            "processing_result": processing_result
        })

    except UploadParsingError as exc:
        return _error(str(exc), 400)
    except Exception as exc:
        app.logger.error(f"Commit failed: {str(exc)}")
        return _error(f"Commit failed: {str(exc)}", 500)






if app.config.get("ENABLE_FRONTEND", False):
    @app.route("/")
    def index():
        # Do NOT send account_emails to the template anymore
        return render_template("login.html")

    #@app.route("/login")
    #def login():
    #    return render_template("login.html")

    @app.route("/cmp")
    def cmp():
        if "user" not in session:
            return redirect(url_for("index"))
        return render_template("cmp.html")

    @app.route("/bh")
    def bh_index():
        if "user" not in session:
            return redirect(url_for("index"))
        return render_template("bh.html")

    # --- Budget Hunter APIs ---
    
    @app.route("/api/bh/upload", methods=["POST"])
    def bh_upload():
        user = _require_user()
        if not isinstance(user, GoogleUser): return user
        
        file = request.files.get("file")
        if not file: return _error("No file uploaded", 400)
        
        svc = BHService()
        try:
            result = svc.process_excel_upload(file.stream, user.email)
            return jsonify({"status": "ok", "result": result})
        except Exception as e:
            app.logger.error(f"BH Upload Failed: {e}")
            return _error(str(e), 500)

    @app.route("/api/bh/accounts", methods=["GET"])
    def bh_list_accounts():
        user = _require_user()
        if not isinstance(user, GoogleUser): return user
        
        search_term = request.args.get("search", "")
        scope = request.args.get("scope", "mine")
        
        owner_filter = None
        if scope == "mine":
            owner_filter = user.email
        
        svc = BHService()
        try:
            data = svc.get_accounts(owner_filter, search_term)
            return jsonify({"status": "ok", "accounts": data})
        except Exception as e:
            return _error(str(e), 500)



    @app.route("/api/bh/download", methods=["POST"])
    def bh_download():
        user = _require_user()
        if not isinstance(user, GoogleUser): return user
        
        data = request.get_json() or {}
        search_term = data.get("search", "")
        scope = data.get("scope", "mine")
        
        owner_filter = None
        if scope == "mine":
            owner_filter = user.email

        svc = BHService()
        try:
            excel_bytes = svc.export_accounts_excel(owner_filter, search_term)
            filename = f"budget_hunter_{datetime.now().strftime('%Y%m%d')}.xlsx"
            
            return send_file(
                BytesIO(excel_bytes),
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=filename
            )
        except Exception as e:
            return _error(str(e), 500)

    @app.route("/api/bh/sync", methods=["GET"])
    def bh_sync():
        user = _require_user()
        if not isinstance(user, GoogleUser): return user
        
        # Stream logs
        def generate():
            svc = BHSyncService()
            # Default to syncing 'yesterday' implicitly in service
            for msg in svc.sync_daily_stats():
                yield msg

        from flask import stream_with_context
        return Response(stream_with_context(generate()), mimetype='text/event-stream')

    @app.route("/api/bh/account/<account_id>", methods=["POST"])
    def bh_update_account(account_id):
        user = _require_user()
        if not isinstance(user, GoogleUser): return user
        
        data = request.get_json()
        svc = BHService()
        try:
            svc.update_account(account_id, data, user.email)
            return jsonify({"status": "ok"})
        except Exception as e:
            return _error(str(e), 500)

    @app.route("/api/bh/account/<account_id>/daily", methods=["GET"])
    def bh_account_daily(account_id):
        user = _require_user()
        if not isinstance(user, GoogleUser): return user
        
        svc = BHService()
        try:
            stats = svc.get_account_daily_stats(account_id)
            return jsonify({"status": "ok", "stats": stats})
        except Exception as e:
            return _error(str(e), 500)

    # --- BH Cron Jobs (Secure) ---
    def _require_cron_auth():
        # Check X-Scheduler-Secret or similar header
        secret = request.headers.get("X-Scheduler-Secret")
        expected = app.config.get("CRON_SECRET")
        if not expected:
            # If no secret is configured, deny all cron requests for security
            return _error("Server Cron Secret not configured.", 500)
        
        if secret != expected:
            return _error("Unauthorized Cron Request", 401)
        return None

    @app.route("/api/bh/cron/daily_sync", methods=["POST"])
    def bh_cron_daily_sync():
        # Security Check
        auth_err = _require_cron_auth()
        if auth_err: return auth_err
        
        svc = BHSyncService()
        yesterday_dt = datetime.utcnow() + timedelta(hours=8) - timedelta(days=1)
        target_date = yesterday_dt.strftime('%Y-%m-%d')
        
        app.logger.info(f"CRON: Starting Daily Sync for {target_date}")
        
        logs = []
        try:
            # Consume generator to run logic
            for msg in svc.sync_daily_stats(target_date=target_date):
                # msg is "data: {...}\n\n", we parse it for logging
                if "{" in msg:
                    try:
                        json_str = msg.replace("data: ", "").strip()
                        data = json.loads(json_str)
                        logs.append(data.get('msg', ''))
                    except: pass
            
            app.logger.info("CRON: Daily Sync Completed.")
            return jsonify({"status": "ok", "date": target_date, "logs": logs})
        except Exception as e:
            app.logger.error(f"CRON Error: {e}")
            return _error(str(e), 500)

    @app.route("/api/bh/cron/intraday_sync", methods=["POST"])
    def bh_cron_intraday_sync():
        # Security Check
        auth_err = _require_cron_auth()
        if auth_err: return auth_err
        
        svc = BHSyncService()
        app.logger.info(f"CRON: Starting Data Integrity Check")
        
        logs = []
        try:
            for msg in svc.sync_consistency_check():
                if "{" in msg:
                    try:
                        json_str = msg.replace("data: ", "").strip()
                        data = json.loads(json_str)
                        logs.append(data.get('msg', ''))
                    except: pass
            
            app.logger.info("CRON: Integrity Check Completed.")
            return jsonify({"status": "ok", "logs": logs})
        except Exception as e:
            app.logger.error(f"CRON Error: {e}")
            return _error(str(e), 500)

# --- Cache Busting Helper ---
@app.context_processor
def override_url_for():
    return dict(url_for=versioned_url_for)

def versioned_url_for(endpoint, **values):
    if endpoint == 'static':
        filename = values.get('filename', None)
        if filename:
            file_path = os.path.join(app.root_path, endpoint, filename)
            try:
                values['v'] = int(os.path.getmtime(file_path))
            except OSError:
                pass
    return url_for(endpoint, **values)

if __name__ == "__main__":
    init_db(app)
    app.run(host="0.0.0.0", port=8080, debug=True)
