from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests


class BroadcielClient:
    """Lightweight client for the Broadciel Ads v2 API."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.session = session or requests.Session()

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def ping(self) -> Dict[str, Any]:
        resp = self.session.get(f"{self.base_url}/health", headers=self._headers(), timeout=30)
        resp.raise_for_status()
        return resp.json()

    def preview_bulk_changes(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Send a preview payload to Broadciel before committing changes."""
        resp = self.session.post(
            f"{self.base_url}/campaigns/preview",
            headers=self._headers(),
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def upsert_campaigns(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Commit campaign + creative changes."""
        resp = self.session.post(
            f"{self.base_url}/campaigns/bulk",
            headers=self._headers(),
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def fetch_lookup(self, resource: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        resp = self.session.get(
            f"{self.base_url}/{resource}",
            headers=self._headers(),
            params=params or {},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("data", [])

