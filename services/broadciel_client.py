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

    def _auth_headers(self, api_token: str) -> Dict[str, str]:
        """新功能使用的 headers，使用 x-authorization"""
        return {
            "Content-Type": "application/json",
            "x-authorization": api_token,
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

    def create_campaign(self, campaign_request_body: Dict[str, Any], api_token: str) -> int:
        """
        Create a new campaign and return its ID.
        
        Args:
            campaign_request_body: Campaign 創建資料
            api_token: API token (放在 x-authorization header)
            
        Returns:
            Campaign ID (cpg_id)
        """
        resp = self.session.post(
            f"{self.base_url}/ad-campaigns",
            headers=self._auth_headers(api_token),
            json=campaign_request_body,
            timeout=30,
        )
        
        # 先取得回應內容，再檢查狀態
        try:
            response_data = resp.json()
        except Exception:
            resp.raise_for_status()
            raise Exception("Invalid JSON response")
        
        # 檢查 HTTP 狀態碼
        if not resp.ok:
            error_message = response_data.get('message', 'Unknown error')
            error_details = response_data.get('errors', {})
            detailed_error = f"HTTP {resp.status_code}: {error_message}"
            if error_details:
                detailed_error += f" - Details: {error_details}"
            raise Exception(detailed_error)
        
        # 檢查 API 回應中的 code 欄位
        if response_data.get("code") != 200:
            error_message = response_data.get('message', 'Unknown error')
            error_details = response_data.get('errors', {})
            detailed_error = f"API Error (code: {response_data.get('code')}): {error_message}"
            if error_details:
                detailed_error += f" - Details: {error_details}"
            raise Exception(detailed_error)
        
        return response_data.get("data", {}).get("cpg_id", 0)

    def create_ad_group(self, ad_group_request_body: Dict[str, Any], api_token: str) -> int:
        """
        Create a new ad group and return its ID.
        
        Args:
            ad_group_request_body: Ad Group 創建資料
            api_token: API token
            
        Returns:
            Ad Group ID (group_id)
        """
        
        resp = self.session.post(
            f"{self.base_url}/ad-groups",
            headers=self._auth_headers(api_token),
            json=ad_group_request_body,
            timeout=30,
        )
        
        # 先取得回應內容，再檢查狀態
        try:
            response_data = resp.json()
        except Exception:
            # 如果無法解析 JSON，直接拋出 HTTP 錯誤
            resp.raise_for_status()
            raise Exception("Invalid JSON response")
        
        # 檢查 HTTP 狀態碼
        if not resp.ok:
            # 取得 API 實際錯誤訊息
            error_message = response_data.get('message', 'Unknown error')
            error_details = response_data.get('errors', {})
            detailed_error = f"HTTP {resp.status_code}: {error_message}"
            if error_details:
                detailed_error += f" - Details: {error_details}"
            raise Exception(detailed_error)
        
        # 檢查 API 回應中的 code 欄位
        if response_data.get("code") != 200:
            error_message = response_data.get('message', 'Unknown error')
            error_details = response_data.get('errors', {})
            detailed_error = f"API Error (code: {response_data.get('code')}): {error_message}"
            if error_details:
                detailed_error += f" - Details: {error_details}"
            raise Exception(detailed_error)
        
        return response_data.get("data", {}).get("group_id", 0)

    def create_creative(self, creative_request_body: Dict[str, Any], api_token: str) -> int:
        """
        Create a new creative and return its ID.
        
        Args:
            creative_request_body: Creative 創建資料
            api_token: API token
            
        Returns:
            Creative ID (cr_id)
        """
        resp = self.session.post(
            f"{self.base_url}/ad-creatives",
            headers=self._auth_headers(api_token),
            json=creative_request_body,
            timeout=30,
        )
        
        # 先取得回應內容，再檢查狀態
        try:
            response_data = resp.json()
        except Exception:
            resp.raise_for_status()
            raise Exception("Invalid JSON response")
        
        # 檢查 HTTP 狀態碼
        if not resp.ok:
            error_message = response_data.get('message', 'Unknown error')
            error_details = response_data.get('errors', {})
            detailed_error = f"HTTP {resp.status_code}: {error_message}"
            if error_details:
                detailed_error += f" - Details: {error_details}"
            raise Exception(detailed_error)
        
        # 檢查 API 回應中的 code 欄位
        if response_data.get("code") != 200:
            error_message = response_data.get('message', 'Unknown error')
            error_details = response_data.get('errors', {})
            detailed_error = f"API Error (code: {response_data.get('code')}): {error_message}"
            if error_details:
                detailed_error += f" - Details: {error_details}"
            raise Exception(detailed_error)
        
        return response_data.get("data", {}).get("cr_id", 0)

    def delete_campaign(self, campaign_id: int, api_token: str) -> bool:
        """
        刪除 Campaign
        
        Args:
            campaign_id: Campaign ID (cpg_id)
            api_token: API token
            
        Returns:
            bool: 是否刪除成功
        """
        try:
            headers = self._auth_headers(api_token)
            headers["cpg_id"] = str(campaign_id)
            
            resp = self.session.delete(
                f"{self.base_url}/ad-campaigns",
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            response_data = resp.json()
            return response_data.get("code") == 200
        except Exception:
            return False

    def delete_ad_group(self, ad_group_id: int, api_token: str) -> bool:
        """
        刪除 Ad Group
        
        Args:
            ad_group_id: Ad Group ID (group_id)
            api_token: API token
            
        Returns:
            bool: 是否刪除成功
        """
        try:
            headers = self._auth_headers(api_token)
            headers["group_id"] = str(ad_group_id)
            
            resp = self.session.delete(
                f"{self.base_url}/ad-groups",
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            response_data = resp.json()
            return response_data.get("code") == 200
        except Exception:
            return False

    def delete_creative(self, creative_id: int, api_token: str) -> bool:
        """
        刪除 Creative
        
        Args:
            creative_id: Creative ID (cr_id)
            api_token: API token
            
        Returns:
            bool: 是否刪除成功
        """
        try:
            headers = self._auth_headers(api_token)
            headers["cr_id"] = str(creative_id)
            
            resp = self.session.delete(
                f"{self.base_url}/ad-creatives",
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            response_data = resp.json()
            return response_data.get("code") == 200
        except Exception:
            return False

