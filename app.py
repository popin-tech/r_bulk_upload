import os
import json
from pathlib import Path
from io import BytesIO
from dataclasses import asdict
from typing import Any, Dict
from openpyxl import Workbook

from flask import Flask, jsonify, request, send_from_directory, render_template, send_file, Response, session, redirect, url_for

from services.auth import AuthError, GoogleUser, verify_google_token
from services.broadciel_client import BroadcielClient
from services.upload_service import UploadParsingError, parse_excel, parse_excel_df, excel_to_campaign_json
from services.campaign_bulk_processor import CampaignBulkProcessor

app = Flask(__name__)

# Configuration
BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-change-me-please")
app.config["GOOGLE_CLIENT_ID"] = os.getenv("GOOGLE_CLIENT_ID", "")
app.config["BROADCIEL_API_BASE_URL"] = os.getenv(
    "BROADCIEL_API_BASE_URL",
    "https://broadciel.console.rixbeedesk.com/api/v2",
)
app.config["BROADCIEL_API_KEY"] = os.getenv("BROADCIEL_API_KEY", "")
app.config["MAX_CONTENT_LENGTH"] = int(os.getenv("MAX_CONTENT_LENGTH_MB", "20")) * 1024 * 1024
app.config["ENABLE_FRONTEND"] = os.getenv("ENABLE_FRONTEND", "true").lower() == "true"
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

        # 4) full DataFrame
        df = parse_excel_df(file_bytes)

        # 5) to JSON (campaign only)
        campaign_payload = excel_to_campaign_json(df)
        app.logger.info("=== Campaign JSON Parsed ===")
        # Always log as JSON string (copy-paste ready)
        import json
        app.logger.info(json.dumps(campaign_payload, ensure_ascii=False, indent=2))   # Cloud Run logs
        print("=== Campaign JSON Parsed ===")
        print(json.dumps(campaign_payload, ensure_ascii=False, indent=2))
        
        # 6) get raw token and create client with auto token exchange
        raw_token = _get_token_for_email(account_email)
        if not raw_token:
            return _error(f"No raw token found for account: {account_email}", 400)
        
        client = _broadciel_client(account_email, raw_token)
        
        # 7) process campaigns using CampaignBulkProcessor
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



# 測試用可刪除 - 單純測試 Campaign API
@app.route("/api/test-single-campaign", methods=["GET"])  
def test_single_campaign():
    """測試用可刪除 - 測試單個 Campaign 創建（無需認證）"""
    # 測試用可刪除 - 跳過認證檢查，方便直接用瀏覽器測試
    # user = _require_user()
    # if not isinstance(user, GoogleUser):
    #     return user
        
    # 測試用可刪除 - 單一 Campaign 測試資料
    test_campaign_data = {
        "cpg_name": "test_campaign_2",   # 廣告活動名稱
            "day_budget": 0.01,                  # 你之後可從 Excel 帶入
            "app": {
                "ad_platform": 1,                # R 平台固定值
                "ad_target": "12345"             # 可從 Excel 帶入
            },
            "adomain": "example.com",            # 廣告主 domain，由你決定
            "sponsored": "ad_group_name",  # 看起來比較像「贊助來源」欄位
            "ad_channel": 1 
    }
    
    test_api_token = "U2FsdGVkX18Mia3m6wmozC76iVv8PAlUqHmKjyPmemEriK2voFBZPhH9mfnjFwUtxeNe49pqJNkANXskJcn+TDWHxEBCHOfmzIZvucUQUotflVNbc6wCwv4Qm9dKK+jY+Q5nLKqBKGS+6kSvOiJSBGBt4682bUm7YUejqTuEvUMYW/3jB/QzNxlTBO38EsXY6sX3XTJfGGQjXzs8D2Kl4P1ZPD5Aog6okNkB7beHJOhfD3zLptKQyTV4yxAD/MKCqGolPhefq7cb9LGFrPaMIs0ne5cdJuaKIgdEP0FGAEY="
    
    client = _broadciel_client()
    try:
        # 測試用可刪除 - 直接呼叫 create_campaign
        campaign_id = client.create_campaign(test_campaign_data, test_api_token) 
        
        return jsonify({
            "status": "test_success",
            "message": "測試用可刪除 - 單一 Campaign 創建成功",
            "campaign_id": campaign_id,
            "test_data": test_campaign_data
        })
        
    except Exception as exc:
        return jsonify({
            "status": "test_error",
            "message": "測試用可刪除 - 單一 Campaign 創建失敗", 
            "error": str(exc),
            "test_data": test_campaign_data
        })


# 測試用可刪除 - 單純測試 Ad Group API
@app.route("/api/test-single-ad-group", methods=["GET"])  
def test_single_ad_group():
    """測試用可刪除 - 測試單個 Ad Group 創建（無需認證）"""
    # 測試用可刪除 - 跳過認證檢查，方便直接用瀏覽器測試
    # user = _require_user()
    # if not isinstance(user, GoogleUser):
    #     return user
        
    # 測試用可刪除 - 單一 Ad Group 測試資料
    test_ad_group_data = {
        "cpg_id": 87779,
        "group_name": "test_benson1208-03",
        "target_info": "https://wellness.tw/contents/0003/test",
        "click_url": [
            "https://example.com/"
        ],
        "impression_url": [
            {
                "type": 1,
                "value": "https://example.com"
            }
        ],
        "budget": {
            "market_target": 1,
            "rev_type": 3,
            "price": 0.16,
            "day_budget": 10,
            "conversion_goal": {
                "type": 0,
                "target_value": 1,
                "convert_event": 0
            }
        },
        "schedule": {
            "start_date": "",
            "end_date": "",
            "week_days": [
                1,
                2,
                3,
                4,
                5,
                6,
                7
            ],
            "hours": [
                0,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
                21,
                22,
                23
            ]
        },
        "location": {
            "country": [
                "TWN"
            ],
            "country_type": 1
        },
        "audience_target": {
            "device_type": [],
            "traffic_type": [],
            "platform": [],
            "browser": [],
            "age": [],
            "gender": [],
            # "os_version": {
            #     "min": 16,
            #     "max": 16
            # },
            "ip": {
                "id": 0,
                "type": 1
            },
            "site": {
                "id": 0,
                "type": 1
            },
            "category": {
                "value": [
                    "IAB1"
                ],
                "type": 1
            },
            "keywords": {
                "value": [
                    "棒棒"
                ],
                "type": 1
            },
            "pixel_audience": []
        }
    }
    
    test_api_token = "U2FsdGVkX1+2iGWq/7iDuuv7GIifk+uUwHVN2gKTuh4ZMx2ny6aEo+1FUgIDpmzmb2BfRnVcpIJJQlQjgtVjDzz2pVe5XmEZtqUtCtYugCe46rebgJ23fejjx7OcRviu6NKVZOX+gxsVvv1uV/dKkCbUUZrCRx6gbjRfz5a850eboiAA1D78XZ8pJS5686A6dKzbF9b1dsCkGVZsPov3+TQPznQhVYJK/qnpvuqn3VXDZ3jWAJhEpHbQUtTkMdVaiMArggwDdiAiu71YF0ZUM+o9MrSFRGUxwhcr6La+rIk="
    
    client = _broadciel_client()
    try:
        # 測試用可刪除 - 直接呼叫 create_ad_group
        ad_group_id = client.create_ad_group(test_ad_group_data, test_api_token) 
        
        return jsonify({
            "status": "test_success",
            "message": "測試用可刪除 - 單一 Ad Group 創建成功",
            "ad_group_id": ad_group_id,
            "test_data": test_ad_group_data
        })
        
    except Exception as exc:
        return jsonify({
            "status": "test_error",
            "message": "測試用可刪除 - 單一 Ad Group 創建失敗", 
            "error": str(exc),
            "test_data": test_ad_group_data
        })


# 測試用可刪除 - 單純測試 Ad Creative API
@app.route("/api/test-single-ad-creative", methods=["GET"])  
def test_single_ad_creative():
    """測試用可刪除 - 測試單個 Ad Creative 創建（無需認證）"""
    # 測試用可刪除 - 跳過認證檢查，方便直接用瀏覽器測試
    # user = _require_user()
    # if not isinstance(user, GoogleUser):
    #     return user
        
    # 測試用可刪除 - 單一 Ad Creative 測試資料
    test_ad_creative_data = {
        "group_id": 95272,
        "cr_name": "test_creative_benson1210",
        "cr_title": "測試廣告創意標題",
        "cr_desc": "測試廣告創意描述內容",
        "cr_btn_text": "立即下載",
        "iab": "IAB1",
        "cr_mt_id": 28575
    }
    
    test_api_token = "U2FsdGVkX1+atmcs9/XLTTvV+BXBL3s8fE30HE8uxlWUokXigx+ZodKV7lcejU1kAACEziu0aOFwgZcvopUrkBQ1MEtXhRvlFyfq4s8oCfoQsMloNAdHGkVZFaii+yllkVqCCBeM08xw7yPWxf2i9sIS713MXxYKJgHf+pM3AmbtyiwjZr4enQXpVXIWGAG1A0CZ2tQw82vI50Y8iRrZMWAGegOogONnEnIpGsUWQ2iHjZVtKtyX+5d3hrLA37tQzBnU4m9IRrrm9Vnr6B0mvlNkr7bjnf2YfxFVGJcNaHM="
    
    client = _broadciel_client()
    try:
        # 測試用可刪除 - 直接呼叫 create_creative
        creative_id = client.create_creative(test_ad_creative_data, test_api_token) 
        
        return jsonify({
            "status": "test_success",
            "message": "測試用可刪除 - 單一 Ad Creative 創建成功",
            "creative_id": creative_id,
            "test_data": test_ad_creative_data
        })
        
    except Exception as exc:
        return jsonify({
            "status": "test_error",
            "message": "測試用可刪除 - 單一 Ad Creative 創建失敗", 
            "error": str(exc),
            "test_data": test_ad_creative_data
        })


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
            return redirect(url_for("login"))
        return render_template("cmp.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
