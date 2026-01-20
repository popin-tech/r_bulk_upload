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

    def update_campaign(self, campaign_request_body: Dict[str, Any]) -> bool:
        """
        Update an existing campaign.
        
        Args:
            campaign_request_body: Campaign 更新資料 (必須包含 cpg_id)
            
        Returns:
            bool: 是否更新成功
        """
        headers = self._auth_headers()
        
        # EXTRACT ID for URL Path
        cpg_id = campaign_request_body.get("cpg_id")
        if not cpg_id:
            raise Exception("cpg_id is required for update_campaign")

        if self.DEBUG_LOG:
            import json
            print("=== update_campaign API Request ===")
            print(f"URL: {self.base_url}/ad-campaigns/{cpg_id}")
            print(f"Headers: {json.dumps(headers, ensure_ascii=False, indent=2)}")
            print(f"Request Body: {json.dumps(campaign_request_body, ensure_ascii=False, indent=2)}")
            print("=" * 40)
        
        # PUT request for update (ID in Path)
        resp = self.session.put(
            f"{self.base_url}/ad-campaigns/{cpg_id}",
            headers=headers,
            json=campaign_request_body,
            timeout=30,
        )
        
        if self.DEBUG_LOG:
            print("=== update_campaign API Response ===")
            print(f"URL: {self.base_url}/ad-campaigns/{cpg_id}")
            print(f"Status Code: {resp.status_code}")
            print(f"Response Text: {resp.text}")
            try:
                print(f"Response JSON: {json.dumps(resp.json(), ensure_ascii=False, indent=2)}")
            except Exception:
                pass
            print("=" * 40)
            
        try:
            response_data = resp.json()
        except Exception:
            resp.raise_for_status()
            raise Exception("Invalid JSON response")
            
        if not resp.ok:
            error_message = response_data.get('message', 'Unknown error')
            raise Exception(f"HTTP {resp.status_code}: {error_message}")
            
        if response_data.get("code") != 200:
            error_message = response_data.get('message', 'Unknown error')
            errors_detail = response_data.get('errors')
            if errors_detail:
                import json
                error_message += f" | Errors: {json.dumps(errors_detail, ensure_ascii=False)}"
            raise Exception(f"API Error (code: {response_data.get('code')}): {error_message}")
            
        return True

    def update_ad_group(self, ad_group_request_body: Dict[str, Any]) -> bool:
        """Update an existing ad group."""
        headers = self._auth_headers()
        
        # EXTRACT ID for URL Path
        group_id = ad_group_request_body.get("group_id")
        # In case caller didn't ensure group_id is in body (CampaignBulkProcessor does extract it? Let's check)
        # CampaignBulkProcessor._extract_ad_group_data adds group_id if present.
        # But wait, create body usually doesn't need ID. Update body usually does.
        # My processor logic puts ID in body. So it is here.
        if not group_id:
             # Fallback check? No, must fail if no ID.
             raise Exception("group_id is required for update_ad_group")

        if self.DEBUG_LOG:
            import json
            print("=== update_ad_group API Request ===")
            print(f"URL: {self.base_url}/ad-groups/{group_id}")
            print(f"Request Body: {json.dumps(ad_group_request_body, ensure_ascii=False, indent=2)}")
            print("=" * 40)
        
        resp = self.session.put(
            f"{self.base_url}/ad-groups/{group_id}",
            headers=headers,
            json=ad_group_request_body,
            timeout=30,
        )
        
        if self.DEBUG_LOG:
            print("=== update_ad_group API Response ===")
            print(f"Status Code: {resp.status_code}")
            print(f"Response Text: {resp.text}")
            print("=" * 40)
            
        try:
            response_data = resp.json()
        except Exception:
            resp.raise_for_status()
            raise Exception("Invalid JSON response")
            
        if not resp.ok or response_data.get("code") != 200:
            error_message = response_data.get('message', 'Unknown error')
            errors_detail = response_data.get('errors')
            if errors_detail:
                import json
                error_message += f" | Errors: {json.dumps(errors_detail, ensure_ascii=False)}"
            raise Exception(f"Update failed: {error_message}")
            
        return True

    def update_creative(self, creative_request_body: Dict[str, Any]) -> bool:
        """Update an existing creative."""
        headers = self._auth_headers()
        
        # EXTRACT ID for URL Path
        # Note: creative body key could be cr_title etc.
        # Processor puts "cr_id" in body if present.
        # But wait, cURL example body DOES NOT show cr_id inside body data except maybe implicitly?
        # cURL: .../ad-creatives/371500 ... -d '{"cr_status":1...}'
        # Body does NOT have cr_id.
        # Campaign Body HAS cpg_id.
        # AdGroup Body DOES NOT have group_id?
        # Let's re-read cURL carefully.
        # Campaign cURL: "cpg_id": 92769 IS in body.
        # Group cURL: Body does NOT seem to have "group_id".
        # Creative cURL: Body does NOT seem to have "cr_id".
        
        # So my Processor IS putting ID in body for all.
        # It's extra data, usually harmless, but the ID IS REQUIRED FOR URL.
        
        cr_id = creative_request_body.get("cr_id")
        if not cr_id:
             raise Exception("cr_id is required for update_creative")
        
        if self.DEBUG_LOG:
            import json
            print("=== update_creative API Request ===")
            print(f"URL: {self.base_url}/ad-creatives/{cr_id}")
            print(f"Request Body: {json.dumps(creative_request_body, ensure_ascii=False, indent=2)}")
            print("=" * 40)
        
        resp = self.session.put(
            f"{self.base_url}/ad-creatives/{cr_id}",
            headers=headers,
            json=creative_request_body,
            timeout=30,
        )
        
        if self.DEBUG_LOG:
            print("=== update_creative API Response ===")
            print(f"Status Code: {resp.status_code}")
            print(f"Response Text: {resp.text}")
            print("=" * 40)
            
        try:
            response_data = resp.json()
        except Exception:
            resp.raise_for_status()
            raise Exception("Invalid JSON response")
            
        if not resp.ok or response_data.get("code") != 200:
            error_message = response_data.get('message', 'Unknown error')
            errors_detail = response_data.get('errors')
            if errors_detail:
                import json
                error_message += f" | Errors: {json.dumps(errors_detail, ensure_ascii=False)}"
            raise Exception(f"Update failed: {error_message}")
            
        return True

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

    def fetch_ai_audiences(self) -> List[Dict[str, Any]]:
        """
        Fetch the list of AI audiences.
        
        Returns:
            List of audience dictionaries.
        """
        headers = self._auth_headers()
        
        if self.DEBUG_LOG:
            # print("=== fetch_ai_audiences API Request ===")
            # print(f"URL: {self.base_url}/ai-audiences")
            # print("=" * 40)
            pass

        resp = self.session.get(
            f"{self.base_url}/ai-audiences",
            headers=headers,
            timeout=30,
        )

        if self.DEBUG_LOG:
            # print("=== fetch_ai_audiences API Response ===")
            # print(f"Status Code: {resp.status_code}")
            # try:
            #     import json
            #     print(f"Response JSON: {json.dumps(resp.json(), ensure_ascii=False, indent=2)}")
            # except Exception:
            #     pass
            # print("=" * 40)
            pass

        resp.raise_for_status()
        data = resp.json()
        
        if data.get("code") != 200:
            raise Exception(f"Failed to fetch audiences: {data.get('message')}")
            
        # The response structure is data -> data -> [list]
        return data.get("data", {}).get("data", [])


    def _fetch_all_pages(self, endpoint: str, params: Dict[str, Any] = None, page_size: int = 50) -> List[Dict[str, Any]]:
        """
        Generic helper to fetch all items using pagination (start/end).
        """
        all_items = []
        start = 0
        current_params = params.copy() if params else {}
        
        while True:
            current_params["start"] = start
            current_params["end"] = start + page_size
            
            headers = self._auth_headers()
            
            if self.DEBUG_LOG:
                print(f"=== Fetch All Page: {endpoint} {start}-{start+page_size} ===")
            
            resp = self.session.get(
                f"{self.base_url}/{endpoint}",
                headers=headers,
                params=current_params,
                timeout=45,
            )
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("code") != 200:
                raise Exception(f"API Error fetching {endpoint}: {data.get('message')}")
            
            # Extract list and total
            # Response: data -> data (list), data -> total (int)
            inner_data = data.get("data", {})
            items = inner_data.get("data", [])
            total = inner_data.get("total", 0)
            
            all_items.extend(items)
            
            if self.DEBUG_LOG:
                print(f"Fetched {len(items)} items. Total so far: {len(all_items)} / {total}")
            
            # Check if we are done
            # If no items returned, or we have reached/exceeded total
            if not items:
                break
                
            if len(all_items) >= total:
                break
                
            # Next page
            start += len(items) # increment by actual items received
            
        return all_items

    def fetch_all_campaigns(self) -> List[Dict[str, Any]]:
        """
        Fetch ALL campaigns for the authenticated account.
        """
        # params can include filters if needed, e.g. status
        # The user request example had cpg_status=1,2 etc.
        # We probably want ALL status to reconstruct the full account, 
        # or maybe just active? The requirement says "download the structure", usually implies everything.
        # But maybe safer to ask? The user said "不指定上層id 或是本層id 的話，就是帳戶下的當下層級全抓"
        # and provided examples with various filters.
        # For now, let's try fairly broad filters or no filters if API allows.
        # The example used: cpg_status=1%2C2 (1,2)
        # We might want to include stopped ones too? 
        # API defaults usually apply if not specified.
        # Let's specify order to be deterministic.
        params = {
            "order_key": "update_time", 
            "order": "desc",
            # User requested to fetch only status 1 (Active) and 2 (Paused).
            # Status 3 (Archived) is excluded.
            "cpg_status": [1, 2]
        }
        return self._fetch_all_pages("ad-campaigns", params=params)

    def get_ad_group(self, group_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch detailed ad group by ID.
        Endpoint: /api/v2/ad-groups/{id}
        """
        try:
            resp = self.session.get(
                f"{self.base_url}/ad-groups/{group_id}",
                headers=self._auth_headers(),
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            # Structure is: data -> cpg_id, group_id, etc. (Direct object, no inner list)
            return data.get("data")
        except Exception as e:
            if self.DEBUG_LOG:
                print(f"Error fetching ad group {group_id}: {e}")
            return None

    def fetch_all_ad_groups(self) -> List[Dict[str, Any]]:
        """
        Fetch ALL ad groups for the authenticated account.
        WARNING: This now performs N+1 requests to get full details (Conversion, Audience, etc.)
        required for the full Excel export.
        """
        # 1. Fetch list to get IDs (lightweight)
        # Using status filters to ensure we capture active/paused/etc.
        # But we really want *all*. Default might hide deleted? 
        # Let's rely on default list first.
        params = {
            "order_key": "update_time", 
            "order": "desc",
            # User requested to fetch only status 1 (Active) and 2 (Paused).
            "group_status": [1, 2]
        }
        
        basic_list = self._fetch_all_pages("ad-groups", params=params)
        
        detailed_list = []
        for item in basic_list:
            gid = item.get("group_id")
            if gid:
                detail = self.get_ad_group(gid)
                if detail:
                    detailed_list.append(detail)
                else:
                    # Fallback to basic info if detail fetch fails
                    detailed_list.append(item)
            else:
                detailed_list.append(item)
                
        return detailed_list

    def fetch_all_ad_creatives(self) -> List[Dict[str, Any]]:
        """
        Fetch ALL ad creatives for the authenticated account.
        """
        # Example used: cr_status=1
        # Example used: cr_status=1
        params = {
            "order_key": "update_time", 
            "order": "desc",
            # User requested to fetch only status 1 (Active) and 2 (Paused).
            "cr_status": [1, 2]
        }
        return self._fetch_all_pages("ad-creatives", params=params)

    def fetch_material(self, mt_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch extensive details for a specific material by ID.
        Endpoint: /api/v2/ad-materials
        
        Args:
            mt_id: The material ID.
            
        Returns:
            Dict containing material details or None if not found.
        """
        params = {
            "mt_id": mt_id,
            "start": 0,
            "end": 1  # We only need the specific one
        }
        
        # This endpoint returns a list in data->data, similar to others
        results = self.fetch_lookup("ad-materials", params=params)
        if results:
            return results[0]
        return None
