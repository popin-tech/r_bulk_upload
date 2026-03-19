import os
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from io import BytesIO
import base64
from dataclasses import asdict
from typing import Any, Dict
from openpyxl import Workbook

from flask import Flask, jsonify, request, send_from_directory, render_template, send_file, Response, session, redirect, url_for, current_app

from services.auth import AuthError, GoogleUser, verify_google_token
from services.broadciel_client import BroadcielClient
from services.upload_service import UploadParsingError, parse_excel, parse_excel_df, excel_to_campaign_json, generate_excel_from_api_data
from services.campaign_bulk_processor import CampaignBulkProcessor
from services.bh_service import BHService
from services.bh_sync import BHSyncService
from services.media_service import MediaService
from functools import wraps
from database import db, init_db, BHAccount, BHDailyStats, User, BHAccountAE, BHDAccountToken

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
is_cloud_run = "K_SERVICE" in os.environ
app.config["LOCAL_DEV"] = os.getenv("LOCAL_DEV", str(not is_cloud_run)).lower() == "true"

# Database Config
# Format: mysql+pymysql://user:password@host:port/dbname
connection_name = os.getenv("CLOUDSQL_CONNECTION_NAME")
db_user = os.getenv("DB_USER", "popin")
db_pass = os.getenv("DB_PASS", "popIn_gcp_2026")
db_name = os.getenv("DB_NAME", "budget_hunter")

if connection_name:
    # Cloud Run -> Cloud SQL via Unix Socket
    # Use empty host and pass socket via connect_args for PyMySQL stability
    app.config["SQLALCHEMY_DATABASE_URI"] = f"mysql+pymysql://{db_user}:{db_pass}@/{db_name}"
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
        "connect_args": {
            "unix_socket": f"/cloudsql/{connection_name}"
        }
    }
else:
    # Local -> Cloud SQL via Public IP (TCP)
    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("SQLALCHEMY_DATABASE_URI", f"mysql+pymysql://{db_user}:{db_pass}@35.234.61.181:3306/{db_name}")
    
    # SSL Configuration (Only for TCP)
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

def _is_user_authorized(email: str) -> dict | None:
    """從資料庫檢查使用者是否存在且啟用，並回傳角色與權限"""
    if not email:
        return None
    user = User.query.filter_by(email=email.lower()).first()
    if user and user.is_active:
        return {
            "role": user.role,
            "access_modules": user.access_modules or []
        }
    return None

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = _require_user() # This also checks is_active usually via DB search if session invalid
        if isinstance(user, Response):
             return user
        # But we need to ensure the role is admin
        auth_info = _is_user_authorized(user.email)
        if not auth_info or auth_info.get("role") != "admin":
            return render_template("login.html", error="Only admins can access this page."), 403
        return f(*args, **kwargs)
    return decorated_function

def module_required(module_name: str):
    """
    Ensure the user has access to the specified module.
    Checks session["access_modules"].
    If unauthorized, redirects to the access_denied page for HTML requests,
    or returns 403 JSON for API requests.
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            user = _require_user()
            if isinstance(user, Response):
                # HTML page requests: redirect to login instead of returning JSON error
                if not request.path.startswith("/api/"):
                    return redirect(url_for("index"))
                return user
            
            access_modules = session.get("access_modules", [])
            if module_name not in access_modules:
                # API requests usually accept JSON
                if request.path.startswith("/api/"):
                    return _error(f"無存取權限: 需要 {module_name} 模組權限", 403)
                
                # HTML template pages
                return render_template(
                    "access_denied.html", 
                    module=module_name, 
                    email=(user.email or "Unknown")
                ), 403
                
            return f(*args, **kwargs)
        return decorated_function
    return decorator


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


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = _require_user()
        if isinstance(user, Response):
            return user
        return f(*args, **kwargs)
    return decorated_function

def _require_user() -> GoogleUser | Response:
    # 0. Bypass for Local Development
    if app.config.get("LOCAL_DEV"):
        dev_email = "benson@popin.cc"
        auth_data = _is_user_authorized(dev_email)
        if auth_data:
            session["user_role"] = auth_data["role"]
            session["access_modules"] = auth_data["access_modules"]
            
        return GoogleUser(
            email=dev_email,
            name="Local Dev User",
            sub="mock-sub-123",
            picture=""
        )

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
    auth_data = _is_user_authorized(email)
    if not auth_data:
        return _error("您的帳號尚未開啟權限或已停用，請聯繫 Admin。", 403)
    
    # 將重要權限存入 session
    session["user_role"] = auth_data["role"]
    session["access_modules"] = auth_data["access_modules"]
    
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

@app.route("/api/bh/account/<int:id>", methods=["POST"])
@module_required("bh")
def bh_update_account(id):
    data = request.json
    acc = BHAccount.query.get(id)
    if not acc:
        return jsonify({"status": "error", "message": "Account not found"}), 404
    
    # Update main fields
    if "budget" in data: acc.budget = data["budget"]
    if "start_date" in data: acc.start_date = data["start_date"]
    if "end_date" in data: acc.end_date = data["end_date"]
    if "agent" in data: acc.agent = data["agent"]
    if "cpc_goal" in data: acc.cpc_goal = data["cpc_goal"]
    if "cpa_goal" in data: acc.cpa_goal = data["cpa_goal"]
    if "ctr_goal" in data: acc.ctr_goal = data["ctr_goal"]
    if "d_token" in data:
         # Need to find and update in BHDAccountToken
         from database import BHDAccountToken
         token_rec = BHDAccountToken.query.filter_by(account_id=acc.account_id).first()
         if token_rec:
             token_rec.token = data["d_token"]
         else:
             new_token = BHDAccountToken(
                 account_id=acc.account_id,
                 account_name=acc.account_name,
                 token=data["d_token"]
             )
             db.session.add(new_token)

    # --- Sync AE Associations ---
    if "ae_emails" in data:
        new_emails = set(data["ae_emails"])
        # Remove old ones not in new list
        BHAccountAE.query.filter_by(bh_account_id=id).filter(~BHAccountAE.ae_email.in_(new_emails)).delete(synchronize_session=False)
        # Add new ones
        existing_emails = {r.ae_email for r in BHAccountAE.query.filter_by(bh_account_id=id).all()}
        for email in new_emails:
            if email not in existing_emails:
                db.session.add(BHAccountAE(bh_account_id=id, ae_email=email))
    
    db.session.commit()
    return jsonify({"status": "ok"})

@app.route("/api/bh/account/<int:id>/aes", methods=["GET"])
@module_required("bh")
def get_bh_account_aes(id):
    mappings = BHAccountAE.query.filter_by(bh_account_id=id).all()
    return jsonify({"status": "ok", "ae_emails": [m.ae_email for m in mappings]})

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
    
    # 檢查並取得資料庫權限
    auth_data = _is_user_authorized(email)
    if not auth_data:
        # 特別處理 benson@popin.cc 以免空資料庫無法線上登入
        if email == "benson@popin.cc":
            new_user = User(
                name=user.name or "Benson",
                email=email,
                role="admin",
                is_active=True,
                access_modules=["cmp", "bh", "media"]
            )
            db.session.add(new_user)
            db.session.commit()
            auth_data = {
                "role": "admin",
                "access_modules": ["cmp", "bh", "media"]
            }
        else:
            return _error("您的帳號尚未開啟權限或已停用，請聯繫 Admin。", 403)

    # Set session
    session["user"] = asdict(user)
    session["user_role"] = auth_data["role"]
    session["access_modules"] = auth_data["access_modules"]
    session.permanent = True  # Use permanent session (default 31 days)
    
    return jsonify({"status": "ok", "user": asdict(user)})


@app.route("/api/logout", methods=["POST"])
def api_logout():
    """Clear Server-side Session."""
    session.clear()
    return jsonify({"status": "ok"})

@app.route("/logout", methods=["GET"])
def logout():
    """Clear Session and redirect to home."""
    session.clear()
    return redirect(url_for("index"))

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
        # Inject Mock Session for Local Development
        if app.config.get("LOCAL_DEV"):
            session["user"] = {
                "email": "benson@popin.cc",
                "name": "Local Dev User",
                "sub": "mock-sub-123",
                "picture": ""
            }
            session.permanent = True
            return redirect(url_for("media_index")) # 預設導向其中一個 Dashboard，例如 media
            
        # Do NOT send account_emails to the template anymore
        return render_template("login.html")

    #@app.route("/login")
    #def login():
    #    return render_template("login.html")

    @app.route("/cmp")
    @module_required("cmp")
    def cmp():
        if "user" not in session:
            return redirect(url_for("index"))
        return render_template("cmp.html")

    @app.route("/bh")
    @module_required("bh")
    def bh_index():
        if "user" not in session:
            return redirect(url_for("index"))
        return render_template("bh.html")

    @app.route("/media")
    @module_required("media")
    def media_index():
        if "user" not in session:
            return redirect(url_for("index"))
        return render_template("media.html")

    # ==========================================
    # Admin Interface & User Management
    # ==========================================

    @app.route("/admin/users")
    @admin_required
    def admin_users_page():
        return render_template("admin_users.html")

    @app.route("/api/admin/users", methods=["GET"])
    @login_required
    def get_admin_users():
        users = User.query.all()
        return jsonify({"status": "ok", "users": [u.to_dict() for u in users]})

    @app.route("/api/admin/users", methods=["POST"])
    @admin_required
    def create_admin_user():
        data = request.json
        email = data.get("email", "").lower().strip()
        if not email:
            return jsonify({"status": "error", "message": "Email is required"}), 400
        
        existing = User.query.filter_by(email=email).first()
        if existing:
            return jsonify({"status": "error", "message": "User already exists"}), 400
        
        new_user = User(
            name=data.get("name"),
            email=email,
            role=data.get("role", "viewer"),
            is_active=data.get("is_active", True),
            access_modules=data.get("access_modules", ["cmp", "bh", "media"])
        )
        db.session.add(new_user)
        db.session.commit()
        return jsonify({"status": "ok", "user": new_user.to_dict()})

    @app.route("/api/admin/users/<int:user_id>", methods=["PUT"])
    @admin_required
    def update_admin_user(user_id):
        data = request.json
        user = User.query.get(user_id)
        if not user:
            return jsonify({"status": "error", "message": "User not found"}), 404
        
        if "name" in data: user.name = data["name"]
        if "role" in data: user.role = data["role"]
        if "is_active" in data: user.is_active = data["is_active"]
        if "access_modules" in data: user.access_modules = data["access_modules"]
        
        db.session.commit()
        return jsonify({"status": "ok", "user": user.to_dict()})

    @app.route("/admin/accounts")
    @admin_required
    def admin_accounts_page():
        return render_template("admin_accounts.html")

    @app.route("/api/admin/accounts", methods=["GET"])
    @admin_required
    def get_admin_accounts():
        json_path = CONFIG_DIR / "account.json"
        if not json_path.exists():
            return jsonify({"status": "ok", "accounts": []})
        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return jsonify({"status": "ok", "accounts": data})

    @app.route("/api/admin/accounts", methods=["POST"])
    @admin_required
    def save_admin_accounts():
        data = request.json
        if not isinstance(data, list):
            return jsonify({"status": "error", "message": "Expected a list of accounts"}), 400
        
        json_path = CONFIG_DIR / "account.json"
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        global _ACCOUNTS_CACHE
        _ACCOUNTS_CACHE = None
        _load_accounts()
        
        return jsonify({"status": "ok"})

    # ------------------------------------------
    # --- Media Dashboard APIs ---
    @app.route("/api/media/dashboard", methods=["GET"])
    @module_required("media")
    def get_media_dashboard():
        user = _require_user()
        if not isinstance(user, GoogleUser): 
            return user
            
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
            
        try:
            svc = MediaService()
            data = svc.get_dashboard_data(start_date, end_date)
            if "error" in data:
                 return _error(data["error"], 400)
            return jsonify({"status": "ok", "data": data})
        except Exception as e:
            app.logger.error(f"Media Dashboard API Error: {str(e)}")
            return _error(f"無法取得 Dashboard 資料: {str(e)}", 500)

    # --- Budget Hunter APIs ---
    
    @app.route("/api/bh/upload", methods=["POST"])
    @module_required("bh")
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
    @module_required("bh")
    def bh_list_accounts():
        user = _require_user()
        if not isinstance(user, GoogleUser): return user
        
        search_term = request.args.get("search", "")
        scope = request.args.get("scope", "mine")
        
        owner_filter = None
        if scope == "mine":
            owner_filter = user.email
            
        # Admin Override: benson@popin.cc always sees all accounts
        if user.email == "benson@popin.cc":
            owner_filter = None
        
        svc = BHService()
        try:
            data = svc.get_accounts(owner_filter, search_term)
            return jsonify({"status": "ok", "accounts": data})
        except Exception as e:
            return _error(str(e), 500)



    @app.route("/api/bh/download", methods=["POST"])
    @module_required("bh")
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
    @module_required("bh")
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


    @app.route("/api/bh/account_pk/<int:pk_id>/sync_full", methods=["GET"])
    @module_required("bh")
    def bh_account_full_sync(pk_id):
        user = _require_user()
        if not isinstance(user, GoogleUser): return user
        
        custom_start = request.args.get('start_date')
        custom_end = request.args.get('end_date')

        # Limit custom sync to benson
        if (custom_start or custom_end) and user.email != 'benson@popin.cc':
            return _error("Unauthorized for custom sync", 403)
            
        svc = BHSyncService()
        # Capture app context here
        app_obj = current_app._get_current_object()
        
        # Helper to intercept generator output for logging
        def stream_with_logging():
            app_obj.logger.info(f"UI Trigger: Starting Full Sync for Account PK={pk_id} by {user.email}")
            start_time = time.time()
            try:
                for msg in svc.sync_account_full_range_by_pk(pk_id, app_obj, custom_start, custom_end):
                    yield msg
                elapsed = time.time() - start_time
                app_obj.logger.info(f"UI Trigger: Full Sync for PK={pk_id} Completed in {int(elapsed)} seconds.")
            except Exception as e:
                app_obj.logger.error(f"UI Trigger: Full Sync for PK={pk_id} Failed: {e}")
                
        return Response(stream_with_logging(), mimetype='text/event-stream')
    @app.route("/api/bh/accounts/bulk-status", methods=["POST"])
    @module_required("bh")
    def bh_bulk_status():
        user = _require_user()
        if not isinstance(user, GoogleUser): return user
        
        data = request.get_json() or {}
        account_ids = data.get("account_ids", [])
        status = data.get("status")
        
        if not account_ids or not status:
            return _error("Missing account_ids or status", 400)
            
        svc = BHService()
        try:
            count = svc.update_accounts_status(account_ids, status)
            app.logger.info(f"User {user.email} bulk updated {len(account_ids)} accounts to {status}. Success: {count}")
            return jsonify({"status": "ok", "updated_count": count})
        except Exception as e:
            return _error(str(e), 500)

    @app.route("/api/bh/account/<account_id>/daily", methods=["GET"])
    @module_required("bh")
    def bh_account_daily(account_id):
        user = _require_user()
        if not isinstance(user, GoogleUser): return user
        
        svc = BHService()
        try:
            stats = svc.get_account_daily_stats(int(account_id))
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
        start_time = time.time()
        
        logs = []
        try:
            # Consume generator to run logic
            for msg in svc.sync_daily_stats(target_date=target_date):
                # msg is "data: {...}\n\n", we parse it for logging
                if "{" in msg:
                    try:
                        json_str = msg.replace("data: ", "").strip()
                        data = json.loads(json_str)
                        msg_text = data.get('msg', '')
                        if msg_text:
                            # print(f"[CRON-LOG] {msg_text}", flush=True)
                            logs.append(msg_text)
                    except: pass

            elapsed = time.time() - start_time
            time_str = f"{int(elapsed // 60)}分{int(elapsed % 60)}秒({int(elapsed)}秒)"
            
            app.logger.info(f"CRON: Daily Sync Completed. Duration: {time_str}")
            logs.append(f"總耗時時間 {time_str}")
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
        start_time = time.time()
        
        logs = []
        try:
            for msg in svc.sync_consistency_check():
                if "{" in msg:
                    try:
                        json_str = msg.replace("data: ", "").strip()
                        data = json.loads(json_str)
                        msg_text = data.get('msg', '')
                        if msg_text:
                            print(f"[CRON-LOG] {msg_text}", flush=True)
                            logs.append(msg_text)
                    except: pass
            
            elapsed = time.time() - start_time
            time_str = f"{int(elapsed // 60)}分{int(elapsed % 60)}秒({int(elapsed)}秒)"

            app.logger.info(f"CRON: Integrity Check Completed. Duration: {time_str}")
            logs.append(f"總耗時時間 {time_str}")
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
    app.run(host="0.0.0.0", port=5001, debug=True)
