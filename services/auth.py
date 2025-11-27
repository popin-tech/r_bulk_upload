from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from google.auth.transport import requests as google_requests
from google.oauth2 import id_token


class AuthError(RuntimeError):
    pass


@dataclass
class GoogleUser:
    email: str
    name: str
    sub: str
    picture: Optional[str] = None


def verify_google_token(token: str, client_id: str) -> GoogleUser:
    if not token:
        raise AuthError("Missing Google ID token.")

    try:
        info: Dict[str, Any] = id_token.verify_oauth2_token(
            token,
            google_requests.Request(),
            audience=client_id,
        )
    except Exception as exc:
        raise AuthError("Invalid Google ID token.") from exc

    return GoogleUser(
        email=info.get("email", ""),
        name=info.get("name", ""),
        sub=info.get("sub", ""),
        picture=info.get("picture"),
    )

