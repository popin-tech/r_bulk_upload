import os
import json
from pathlib import Path
from io import BytesIO
from dataclasses import asdict
from typing import Any, Dict
from openpyxl import Workbook

from flask import Flask, jsonify, request, send_from_directory, render_template, send_file, Response

from services.auth import AuthError, GoogleUser, verify_google_token
from services.broadciel_client import BroadcielClient
from services.upload_service import UploadParsingError, parse_excel, parse_excel_df, excel_to_campaign_json

app = Flask(__name__)

# Configuration
BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-change-me")
app.config["GOOGLE_CLIENT_ID"] = os.getenv("GOOGLE_CLIENT_ID", "")
app.config["BROADCIEL_API_BASE_URL"] = os.getenv(
    "BROADCIEL_API_BASE_URL",
    "https://broadciel.console.rixbeedesk.com/api/ads/v2",
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
    token = _get_token_from_header()
    try:
        user = verify_google_token(token, app.config["GOOGLE_CLIENT_ID"])
    except AuthError as exc:
        return _error(str(exc), 401)

    email = (user.email or "").lower()
    allowed_emails = _load_allowed_emails()
    if email not in allowed_emails:
        return _error("You are not authorized to use this app.", 403)
    return user

def _broadciel_client() -> BroadcielClient:
    return BroadcielClient(
        base_url=app.config["BROADCIEL_API_BASE_URL"],
        api_key=app.config["BROADCIEL_API_KEY"],
    )


def _error(message: str, status: int):
    payload = {"error": message}
    data = json.dumps(payload, ensure_ascii=False)
    return Response(
        response=data,
        status=status,
        mimetype="application/json; charset=utf-8",
    )


@app.route("/api/accounts", methods=["GET"])
def list_accounts():
    user = _require_user()
    if not isinstance(user, GoogleUser):
        return user  # 401 / 403

    accounts = _load_accounts()
    emails = [
        item.get("email")
        for item in accounts
        if item.get("email")
    ]
    return jsonify({"accounts": emails})


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

    # 2) get excel file
    upload = request.files.get("file")
    if upload is None or upload.filename == "":
        return _error("No Excel file uploaded.", 400)

    try:
        # 3) read bytes
        file_bytes = upload.read()

        # 4) full DataFrame
        df = parse_excel_df(file_bytes)

        # 5) to JSON (campaign only)
        campaign_payload = excel_to_campaign_json(df)
        app.logger.info("=== Campaign JSON Parsed ===")
        app.logger.info(campaign_payload)   # Cloud Run logs
        print("=== Campaign JSON Parsed ===")
        print(campaign_payload)

    except UploadParsingError as exc:
        return _error(str(exc), 400)
    except Exception as exc:
        return _error(f"Unexpected error: {exc}", 500)
    return jsonify({
        "status": "ok",
        "campaign": campaign_payload["campaign"]
    })


if app.config.get("ENABLE_FRONTEND", False):
    @app.route("/")
    def index():
        # Do NOT send account_emails to the template anymore
        return render_template("index.html")



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)