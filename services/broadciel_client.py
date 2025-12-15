from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests


class BroadcielClient:
    """Lightweight client for the Broadciel Ads v2 API."""
    
    # Debug log 開關
    DEBUG_LOG = True

    def __init__(
        self,
        base_url: str,
        api_key: str = "",
        session: Optional[requests.Session] = None,
        account_email: Optional[str] = None,
        raw_token: Optional[str] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.session = session or requests.Session()
        self._api_token = None  # 私有變數存放交換後的 API token
        
        # 如果提供了 account_email 和 raw_token，自動交換 API token
        if account_email and raw_token:
            self._api_token = self._exchange_token_internal(account_email, raw_token)

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _auth_headers(self, api_token: Optional[str] = None) -> Dict[str, str]:
        """新功能使用的 headers，使用 x-authorization"""
        token = api_token or self._api_token
        if not token:
            raise Exception("No API token available. Please exchange token first.")
        return {
            "Content-Type": "application/json",
            "x-authorization": token,
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

    def create_campaign(self, campaign_request_body: Dict[str, Any]) -> int:
        """
        Create a new campaign and return its ID.
        
        Args:
            campaign_request_body: Campaign 創建資料
            api_token: API token (放在 x-authorization header)
            
        Returns:
            Campaign ID (cpg_id)
        """
        headers = self._auth_headers()
        
        # 印出 POST request 資訊
        if self.DEBUG_LOG:
            import json
            print("=== create_campaign API Request ===")
            print(f"URL: {self.base_url}/ad-campaigns")
            print(f"Headers: {json.dumps(headers, ensure_ascii=False, indent=2)}")
            print(f"Request Body: {json.dumps(campaign_request_body, ensure_ascii=False, indent=2)}")
            print("=" * 40)
        
        resp = self.session.post(
            f"{self.base_url}/ad-campaigns",
            headers=headers,
            json=campaign_request_body,
            timeout=30,
        )
        
        # 完整印出 POST 回應資訊
        if self.DEBUG_LOG:
            print("=== create_campaign API Response ===")
            print(f"URL: {self.base_url}/ad-campaigns")
            print(f"Status Code: {resp.status_code}")
            print(f"Response Text: {resp.text}")
            
            try:
                response_json = resp.json()
                print(f"Response JSON: {json.dumps(response_json, ensure_ascii=False, indent=2)}")
            except Exception as e:
                print(f"Failed to parse JSON: {e}")
            print("=" * 40)
        
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

    def create_ad_group(self, ad_group_request_body: Dict[str, Any]) -> int:
        """
        Create a new ad group and return its ID.
        
        Args:
            ad_group_request_body: Ad Group 創建資料
            api_token: API token
            
        Returns:
            Ad Group ID (group_id)
        """
        
        headers = self._auth_headers()
        
        # 印出 POST request 資訊
        if self.DEBUG_LOG:
            import json
            print("=== create_ad_group API Request ===")
            print(f"URL: {self.base_url}/ad-groups")
            print(f"Headers: {json.dumps(headers, ensure_ascii=False, indent=2)}")
            print(f"Request Body: {json.dumps(ad_group_request_body, ensure_ascii=False, indent=2)}")
            print("=" * 40)
        
        resp = self.session.post(
            f"{self.base_url}/ad-groups",
            headers=headers,
            json=ad_group_request_body,
            timeout=30,
        )
        
        # 完整印出 POST 回應資訊
        print("=== create_ad_group API Response ===")
        print(f"URL: {self.base_url}/ad-groups")
        print(f"Status Code: {resp.status_code}")
        print(f"Response Text: {resp.text}")
        
        try:
            response_json = resp.json()
            print(f"Response JSON: {json.dumps(response_json, ensure_ascii=False, indent=2)}")
        except Exception as e:
            print(f"Failed to parse JSON: {e}")
        print("=" * 40)
        
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

    def create_creative(self, creative_request_body: Dict[str, Any]) -> int:
        """
        Create a new creative and return its ID.
        
        Args:
            creative_request_body: Creative 創建資料
            api_token: API token
            
        Returns:
            Creative ID (cr_id)
        """
        headers = self._auth_headers()
        
        # 印出 POST request 資訊
        if self.DEBUG_LOG:
            import json
            print("=== create_creative API Request ===")
            print(f"URL: {self.base_url}/ad-creatives")
            print(f"Headers: {json.dumps(headers, ensure_ascii=False, indent=2)}")
            print(f"Request Body: {json.dumps(creative_request_body, ensure_ascii=False, indent=2)}")
            print("=" * 40)
        
        resp = self.session.post(
            f"{self.base_url}/ad-creatives",
            headers=headers,
            json=creative_request_body,
            timeout=30,
        )
        
        # 完整印出 POST 回應資訊
        print("=== create_creative API Response ===")
        print(f"URL: {self.base_url}/ad-creatives")
        print(f"Status Code: {resp.status_code}")
        print(f"Response Text: {resp.text}")
        
        try:
            response_json = resp.json()
            print(f"Response JSON: {json.dumps(response_json, ensure_ascii=False, indent=2)}")
        except Exception as e:
            print(f"Failed to parse JSON: {e}")
        print("=" * 40)
        
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

    def delete_campaign(self, campaign_id: int) -> bool:
        """
        刪除 Campaign
        
        Args:
            campaign_id: Campaign ID (cpg_id)
            
        Returns:
            bool: 是否刪除成功
        """
        try:
            headers = self._auth_headers()
            headers["cpg_id"] = str(campaign_id)
            
            # 印出 DELETE request 資訊
            if self.DEBUG_LOG:
                import json
                print("=== delete_campaign API Request ===")
                print(f"URL: {self.base_url}/ad-campaigns")
                print(f"Headers: {json.dumps(headers, ensure_ascii=False, indent=2)}")
                print(f"Campaign ID: {campaign_id}")
                print("=" * 40)
            
            resp = self.session.delete(
                f"{self.base_url}/ad-campaigns",
                headers=headers,
                timeout=30,
            )
            
            # 印出 DELETE response 資訊
            if self.DEBUG_LOG:
                print("=== delete_campaign API Response ===")
                print(f"URL: {self.base_url}/ad-campaigns")
                print(f"Status Code: {resp.status_code}")
                print(f"Response Text: {resp.text}")
                
                try:
                    response_json = resp.json()
                    print(f"Response JSON: {json.dumps(response_json, ensure_ascii=False, indent=2)}")
                except Exception as e:
                    print(f"Failed to parse JSON: {e}")
                print("=" * 40)
            
            resp.raise_for_status()
            response_data = resp.json()
            success = response_data.get("code") == 200
            
            if self.DEBUG_LOG:
                print(f"Delete campaign result: {success}")
                
            return success
        except Exception as e:
            if self.DEBUG_LOG:
                print(f"Delete campaign exception: {e}")
            return False

    def delete_ad_group(self, ad_group_id: int) -> bool:
        """
        刪除 Ad Group
        
        Args:
            ad_group_id: Ad Group ID (group_id)
            
        Returns:
            bool: 是否刪除成功
        """
        try:
            headers = self._auth_headers()
            headers["group_id"] = str(ad_group_id)
            
            if self.DEBUG_LOG:
                import json
                print("=== delete_ad_group API Request ===")
                print(f"URL: {self.base_url}/ad-groups")
                print(f"Headers: {json.dumps(headers, ensure_ascii=False, indent=2)}")
                print(f"Ad Group ID: {ad_group_id}")
                print("=" * 40)
            
            resp = self.session.delete(
                f"{self.base_url}/ad-groups",
                headers=headers,
                timeout=30,
            )
            
            if self.DEBUG_LOG:
                print("=== delete_ad_group API Response ===")
                print(f"Status Code: {resp.status_code}")
                print(f"Response Text: {resp.text}")
                try:
                    response_json = resp.json()
                    print(f"Response JSON: {json.dumps(response_json, ensure_ascii=False, indent=2)}")
                except Exception as e:
                    print(f"Failed to parse JSON: {e}")
                print("=" * 40)
            
            resp.raise_for_status()
            response_data = resp.json()
            success = response_data.get("code") == 200
            
            if self.DEBUG_LOG:
                print(f"Delete ad group result: {success}")
                
            return success
        except Exception as e:
            if self.DEBUG_LOG:
                print(f"Delete ad group exception: {e}")
            return False

    def delete_creative(self, creative_id: int) -> bool:
        """
        刪除 Creative
        
        Args:
            creative_id: Creative ID (cr_id)
            
        Returns:
            bool: 是否刪除成功
        """
        try:
            headers = self._auth_headers()
            headers["cr_id"] = str(creative_id)
            
            if self.DEBUG_LOG:
                import json
                print("=== delete_creative API Request ===")
                print(f"URL: {self.base_url}/ad-creatives")
                print(f"Headers: {json.dumps(headers, ensure_ascii=False, indent=2)}")
                print(f"Creative ID: {creative_id}")
                print("=" * 40)
            
            resp = self.session.delete(
                f"{self.base_url}/ad-creatives",
                headers=headers,
                timeout=30,
            )
            
            if self.DEBUG_LOG:
                print("=== delete_creative API Response ===")
                print(f"Status Code: {resp.status_code}")
                print(f"Response Text: {resp.text}")
                try:
                    response_json = resp.json()
                    print(f"Response JSON: {json.dumps(response_json, ensure_ascii=False, indent=2)}")
                except Exception as e:
                    print(f"Failed to parse JSON: {e}")
                print("=" * 40)
            
            resp.raise_for_status()
            response_data = resp.json()
            success = response_data.get("code") == 200
            
            if self.DEBUG_LOG:
                print(f"Delete creative result: {success}")
                
            return success
        except Exception as e:
            if self.DEBUG_LOG:
                print(f"Delete creative exception: {e}")
            return False

    def _exchange_token_internal(self, account_email: str, raw_token: str) -> str:
        """內部使用的 token 交換方法"""
        if self.DEBUG_LOG:
            print("=== Exchanging Token ===")
            print(f"\tAccount Email: {account_email}")
            print(f"\tRaw Token: {raw_token}")
        
        request_body = {
            "account_name": account_email,
            "api_token": raw_token
        }
        
        resp = self.session.post(
            f"{self.base_url}/auth/tokens",
            headers={"Content-Type": "application/json"},
            json=request_body,
            timeout=30,
        )
        
        # 先取得回應內容，再檢查狀態
        try:
            response_data = resp.json()
        except Exception:
            resp.raise_for_status()
            raise Exception("Invalid JSON response from token exchange")
        
        # 檢查 HTTP 狀態碼
        if not resp.ok:
            error_message = response_data.get('message', 'Unknown error')
            raise Exception(f"HTTP {resp.status_code}: {error_message}")
        
        # 檢查 API 回應中的 code 欄位
        if response_data.get("code") != 200:
            error_message = response_data.get('message', 'Unknown error')
            raise Exception(f"Token exchange failed (code: {response_data.get('code')}): {error_message}")
        
        # 取得交換後的 token
        token = response_data.get("data", {}).get("token")
        if not token:
            raise Exception("No token returned from exchange API")
        
        if self.DEBUG_LOG:
            print(f"\tExchanged Token: {token}\n")
        return token
    
    def exchange_token(self, account_email: str, raw_token: str) -> str:
        """
        交換 API token（對外接口，保持兼容性）
        
        Args:
            account_email: 帳號 email
            raw_token: 從 account.json 讀取的原始 token
            
        Returns:
            可用於 API 呼叫的 token
        """
        token = self._exchange_token_internal(account_email, raw_token)
        self._api_token = token  # 同時更新內部 token
        return token

