# Broadciel Campaign Management Platform — Bulk Upload Tool

This repository contains the source code for the **Broadciel Campaign Management Platform (Bulk Upload Tool)** — a secure internal web application used to:

- Authenticate internal users via **Google Identity Services**
- Download an Excel campaign template
- Upload & preview campaign data
- Validate and commit changes to the **Broadciel API**
- Map each upload to a selected **Broadciel account** using secure backend tokens

The app is designed for deployment on **Google Cloud Run**, with dependencies handled through `requirements.txt`.

---

## 🚀 Features

### ✔ Google Sign-In Authentication (GIS + OAuth2)
- Secure front-end login using Google Identity Services
- Backend verification of Google ID tokens
- Email whitelist for internal usage only
- Login button transforms into:
  **“Logged in as xxx (click to logout)”**

### ✔ Section-Based UI Security
The following sections remain **hidden until authentication**:

- Download Template
- Account Selection
- Excel Upload
- Preview Table
- Commit to Broadciel

Authorization is validated on **both frontend & backend** for security.

---

## ✔ Secure Account Selection

Account list is stored in:

```
static/account.json
```

Format:

```json
[
  {
    "email": "example@domain.com",
    "token": "abc123..."
  }
]
```

- Loaded server-side only
- Frontend receives **email list only**
- Broadciel token is used **only on backend** during commit

---

## ✔ Excel Upload + Preview Flow

- Accepts `.xlsx` / `.xls`
- Reads the file using `openpyxl` via `parse_excel()`
- Returns preview (columns, sample rows, counts)
- Errors returned cleanly as HTTP 400

---

## ✔ Commit to Broadciel API

Backend performs:

1. Validates authentication
2. Validates selected Broadciel account
3. Looks up token by email
4. Calls Broadciel API using `BroadcielClient`
5. Returns response to frontend

All heavy lifting stays server-side for security.

---

## 🧱 Project Structure

```
r_bulk_upload/
│
├── app.py                         # Main Flask application
├── requirements.txt
│
├── services/
│   ├── auth.py                    # Google token verification & user model
│   ├── upload_service.py          # Excel parsing service
│   └── broadciel_client.py        # Broadciel API client
│
├── static/
│   ├── main.js                    # Frontend JS logic
│   ├── styles.css                 # CSS
│   ├── account.json               # Email → token map
│   ├── campaign_sheet_template.xlsx
│   └── broadciellogo.png
│
└── templates/
    └── index.html                 # Main UI page
```

---

## ⚙️ Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SECRET_KEY` | Flask session secret | dev-secret-change-me |
| `GOOGLE_CLIENT_ID` | GIS Client ID | — |
| `BROADCIEL_API_BASE_URL` | Broadciel API base | https://broadciel.console.rixbeedesk.com/api/ads/v2 |
| `BROADCIEL_API_KEY` | API key (if required) | — |
| `MAX_CONTENT_LENGTH_MB` | Upload limit | 20 |
| `ENABLE_FRONTEND` | Serve index.html | true |

---

## 🛠 Local Development

### 1. Create virtual environment

```sh
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Export environment variables

```sh
export GOOGLE_CLIENT_ID=xxxx.apps.googleusercontent.com
export BROADCIEL_API_KEY=your_key
```

### 3. Run locally

```sh
python app.py
```

App will start on:

```
http://localhost:8080
```

---

## ☁️ Deployment (Cloud Run)

### Build & deploy

```sh
gcloud builds submit --tag gcr.io/PROJECT_ID/r-bulk-upload

gcloud run deploy r-bulk-upload     --image gcr.io/PROJECT_ID/r-bulk-upload     --region asia-east1     --platform managed     --allow-unauthenticated=false
```

---

## 🔒 Security Notes

- Broadciel tokens **never** sent to frontend
- Only validated users can call API endpoints
- Every sensitive operation is revalidated on backend
- account.json should contain **service tokens only (not passwords)**
- Google token is cryptographically verified using signatures

---

## 🧪 API Endpoints

| Endpoint | Method | Description | Auth |
|----------|--------|-------------|------|
| `/api/me` | GET | Validate Google token | ✔ |
| `/api/accounts` | GET | Fetch Broadciel account list | ✔ |
| `/api/template` | GET | Download Excel template | ✔ |
| `/api/upload-preview` | POST | Upload Excel → preview | ✔ |
| `/api/commit` | POST | Commit data to Broadciel | ✔ |
| `/api/health` | GET | Health check | ❌ |

---

## 🏁 Development Workflow

1. Create a new branch:

```sh
git checkout -b feature/your-feature
```

2. Commit code:

```sh
git add .
git commit -m "Description of update"
```

3. Push:

```sh
git push -u origin feature/your-feature
```

4. Open Pull Request on GitHub.

---

## 📄 License

Internal proprietary tool — **not for public redistribution**.