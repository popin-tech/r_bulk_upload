from __future__ import annotations
from typing import Dict, List, Any, Optional
import logging
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class CampaignResult:
    """單個 Campaign 處理結果"""
    campaign_index: int
    success: bool
    campaign_id: Optional[int] = None
    ad_group_results: List['AdGroupResult'] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    
    def __post_init__(self):
        if self.ad_group_results is None:
            self.ad_group_results = []

@dataclass  
class AdGroupResult:
    """單個 Ad Group 處理結果"""
    ad_group_index: int
    success: bool
    ad_group_id: Optional[int] = None
    ad_asset_results: List['AdAssetResult'] = None
    error_message: Optional[str] = None
    
    def __post_init__(self):
        if self.ad_asset_results is None:
            self.ad_asset_results = []

@dataclass
class AdAssetResult:
    """單個 Ad Asset 處理結果"""
    ad_asset_index: int
    success: bool
    creative_id: Optional[int] = None
    error_message: Optional[str] = None

class CampaignBulkProcessor:
    """
    Campaign 批量處理器
    
    只需要傳入：
    1. api_token: 已交換後的API token
    2. payload: 整理後的Excel資料
    """
    
    def __init__(self, client):
        # 直接接收已配置好的 BroadcielClient
        self.client = client
        self.max_retries = 2
        
    def _translate_error_msg(self, error_str: str) -> str:
        """
        將 API JSON 錯誤訊息轉譯為繁體中文
        Example input: '... Errors: {"audience_target": {"site": {"id": {"_errors": ["Required"]}}}}'
        """
        import json
        import re
        
        # 1. 嘗試提取 JSON 部分
        json_part = None
        prefix = ""
        
        # 常見模式: "Update failed: ... Errors: {...}"
        if "Errors: {" in error_str:
            parts = error_str.split("Errors: ", 1)
            prefix = parts[0] + "錯誤詳情："
            json_str = parts[1]
            try:
                # 嘗試解析 JSON (需處理可能截斷或格式問題，這裡假設是完整 JSON)
                json_part = json.loads(json_str)
            except:
                pass
        
        if not json_part:
            # 嘗試直接解析整個字串
            try:
                json_part = json.loads(error_str)
            except:
                return error_str # 無法解析，回傳原始字串

        # 2. 定義欄位中文化對照表
        FIELD_MAP = {
            "audience_target": "目標受眾",
            "site": "網站/APP篩選",
            "id": "ID",
            "type": "類型",
            "url": "網址",
            "device_type": "設備類型",
            "traffic_type": "流量類型",
            "platform": "操作系統",
            "os_version": "系統版本",
            "browser": "瀏覽器",
            "age": "年齡",
            "gender": "性別",
            "category": "投放興趣",
            "keywords": "AI語意擴充",
            "pixel_audience": "自定義受眾",
            "geo_location": "地理位置",
            "budget": "預算",
            "schedule": "排程",
            "cpg_name": "廣告活動名稱",
            "group_name": "廣告群組名稱", 
            "cr_name": "素材名稱",
            "Required": "是必填的",
            "The field is required": "此欄位必填",
            "Invalid value": "數值無效"
        }

        # 3. 遞迴查找錯誤
        errors_found = []
        
        def _traverse(node, path=[]):
            if isinstance(node, dict):
                # Check for _errors
                if "_errors" in node and isinstance(node["_errors"], list) and node["_errors"]:
                    # Found error leaf
                    msgs = node["_errors"]
                    # Translate path
                    path_str = ""
                    for p in path:
                        path_str += f"{FIELD_MAP.get(p, p)} > "
                    
                    # Translate messages
                    trans_msgs = [FIELD_MAP.get(m, m) for m in msgs]
                    errors_found.append(f"{path_str.rstrip(' > ')}: {', '.join(trans_msgs)}")
                
                # Continue traverse
                for k, v in node.items():
                    if k != "_errors":
                        _traverse(v, path + [k])
        
        _traverse(json_part)
        
        if errors_found:
            translated = prefix + "；".join(errors_found)
            return f"{translated} | Original: {error_str}"
            
        return error_str

    def process_bulk_campaigns(
        self,
        payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        批量處理 Campaign 創建
        
        Args:
            payload: Excel處理後的JSON格式資料 {"campaign": [...]}
            
        Returns:
            處理結果統計和詳細資訊
        """
        results = []
        
        campaigns = payload.get("campaign", [])
        for campaign_index, campaign_data in enumerate(campaigns):
            result = self._process_single_campaign_with_retry(
                campaign_index=campaign_index,
                campaign_data=campaign_data
            )
            results.append(result)
        
        return self._generate_summary(results, campaigns)
    
    def _process_single_campaign_with_retry(
        self,
        campaign_index: int,
        campaign_data: Dict[str, Any]
    ) -> CampaignResult:
        """處理單個 Campaign，包含重試邏輯"""
        
        result = CampaignResult(
            campaign_index=campaign_index,
            success=False
        )
        
        for attempt in range(self.max_retries + 1):
            created_campaign_id = None
            failed_step = ""
            
            try:
                result.retry_count = attempt
                
                # 步驟 1: 處理 Campaign (Create or Update)
                failed_step = "campaign"
                campaign_request_body = self._extract_campaign_data(campaign_data)
                
                # Check for existing ID to determine operation
                cpg_id = campaign_data.get("cpg_id")
                if cpg_id:
                    # Update existing campaign
                    print(f"=== 更新campaign (ID: {cpg_id}) ===")
                    # Ensure ID is in the request body (client needs it for URL)
                    campaign_request_body["cpg_id"] = cpg_id
                    success = self.client.update_campaign(campaign_request_body)
                    if not success:
                         raise Exception(f"Campaign update failed for ID {cpg_id}")
                    created_campaign_id = cpg_id
                else:
                    # Create new campaign
                    print("=== 建立campaign ===")
                    created_campaign_id = self.client.create_campaign(campaign_request_body)
                
                if not created_campaign_id:
                    raise Exception("Campaign creation/update returned empty ID")
                
                result.campaign_id = created_campaign_id
                
                # 步驟 2: 處理所有 Ad Groups
                ad_groups = campaign_data.get("ad_group", [])
                ad_group_success_count = 0
                
                for ad_group_index, ad_group_data in enumerate(ad_groups):
                    ad_group_result = self._process_single_ad_group(
                        ad_group_index=ad_group_index,
                        ad_group_data=ad_group_data,
                        campaign_id=created_campaign_id
                    )
                    result.ad_group_results.append(ad_group_result)
                    
                    if ad_group_result.success:
                        ad_group_success_count += 1
                    # 下層級失敗時保留上層級，不拋出錯誤
                
                # Campaign 成功（即使某些 Ad Group 失敗也保留 Campaign）
                result.success = True
                if ad_group_success_count == len(ad_groups):
                    logger.info(f"Successfully processed campaign {campaign_index} with all ad groups after {attempt} retries")
                else:
                    logger.info(f"Successfully processed campaign {campaign_index} with {ad_group_success_count}/{len(ad_groups)} ad groups after {attempt} retries")
                break
                
            except Exception as e:
                raw_err = str(e)
                trans_err = self._translate_error_msg(raw_err)
                error_msg = f"Attempt {attempt + 1} failed at {failed_step}: {trans_err}"
                logger.warning(f"Campaign {campaign_index} - {error_msg}")
                
                if attempt < self.max_retries:
                    time.sleep(1 * (attempt + 1))
                    # 清空之前的結果準備重試
                    result.ad_group_results = []
                    continue
                else:
                    raw_err = str(e)
                    trans_err = self._translate_error_msg(raw_err)
                    result.error_message = f"Failed after {self.max_retries + 1} attempts. Last error at {failed_step}: {trans_err}"
                    logger.error(f"Campaign {campaign_index} - {result.error_message}")
        
        return result
    
    def _process_single_ad_group(
        self,
        ad_group_index: int,
        ad_group_data: Dict[str, Any],
        campaign_id: int
    ) -> AdGroupResult:
        """處理單個 Ad Group"""
        
        result = AdGroupResult(
            ad_group_index=ad_group_index,
            success=False
        )
        
        created_ad_group_id = None
        
        try:
            # 處理 Ad Group (Create or Update)
            ad_group_request_body = self._extract_ad_group_data(ad_group_data, campaign_id)
            
            group_id = ad_group_data.get("group_id")
            if group_id:
                 # Update existing ad group
                 print(f"=== 更新 Ad Group (ID: {group_id}) ===")
                 ad_group_request_body["group_id"] = group_id
                 success = self.client.update_ad_group(ad_group_request_body)
                 if not success:
                      raise Exception(f"Ad Group update failed for ID {group_id}")
                 created_ad_group_id = group_id
            else:
                 # Create new ad group
                 created_ad_group_id = self.client.create_ad_group(ad_group_request_body)
            
            if not created_ad_group_id:
                raise Exception("Ad Group creation/update returned empty ID")
            
            result.ad_group_id = created_ad_group_id
            
            # 處理所有 Ad Assets
            ad_assets = ad_group_data.get("ad_asset", [])
            ad_asset_success_count = 0
            
            for ad_asset_index, ad_asset_data in enumerate(ad_assets):
                ad_asset_result = self._process_single_ad_asset(
                    ad_asset_index=ad_asset_index,
                    ad_asset_data=ad_asset_data,
                    ad_group_id=created_ad_group_id
                )
                result.ad_asset_results.append(ad_asset_result)
                
                if ad_asset_result.success:
                    ad_asset_success_count += 1
                # 下層級失敗時保留上層級，不拋出錯誤
            
            # Ad Group 成功（即使某些 Creative 失敗也保留 Ad Group）
            result.success = True
            if ad_asset_success_count < len(ad_assets):
                logger.info(f"Ad Group {ad_group_index} created with {ad_asset_success_count}/{len(ad_assets)} creatives")
            
        except Exception as e:
            raw_err = str(e)
            trans_err = self._translate_error_msg(raw_err)
            result.error_message = f"Ad Group Action failed: {trans_err}"
            logger.error(f"Ad Group {ad_group_index} - {result.error_message}")
    
        return result
    
    def _process_single_ad_asset(
        self,
        ad_asset_index: int,
        ad_asset_data: Dict[str, Any],
        ad_group_id: int
    ) -> AdAssetResult:
        """處理單個 Ad Asset"""
        
        result = AdAssetResult(
            ad_asset_index=ad_asset_index,
            success=False
        )
        
        try:
            creative_request_body = self._extract_creative_data(ad_asset_data, ad_group_id)
            
            cr_id = ad_asset_data.get("cr_id")
            if cr_id:
                # Update existing creative
                # Ensure ID is passed to extraction or manually added if extract doesn't handle it
                # Logic: extract_creative_data doesn't extract cr_id because create doesn't need it.
                # So we add it here manually.
                creative_request_body["cr_id"] = cr_id
                print(f"=== 更新 Creative (ID: {cr_id}) ===")
                success = self.client.update_creative(creative_request_body)
                if not success:
                     raise Exception(f"Creative update failed for ID {cr_id}")
                created_creative_id = cr_id
            else:
                created_creative_id = self.client.create_creative(creative_request_body)
            
            if not created_creative_id:
                raise Exception("Creative creation/update returned empty ID")
            
            result.creative_id = created_creative_id
            result.success = True
            
        except Exception as e:
            raw_err = str(e)
            trans_err = self._translate_error_msg(raw_err)
            result.error_message = f"Ad Asset Action failed: {trans_err}"
            logger.error(f"Ad Asset {ad_asset_index} - {result.error_message}")
        
        return result
    
    def _extract_campaign_data(self, campaign_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        從 campaign_data 中提取 Campaign 創建所需資料
        """
        campaign_request = {
            "cpg_name": campaign_data.get("cpg_name", ""),
            "day_budget": campaign_data.get("day_budget", 0.01),
            "ad_channel": campaign_data.get("ad_channel", 1),
            "adomain": campaign_data.get("adomain", "")
        }
        
        # 添加 sponsored 欄位（可選）
        if "sponsored" in campaign_data:
            campaign_request["sponsored"] = campaign_data["sponsored"]
        
        
        # 添加 app 物件（只有在 ad_channel=1 時）
        if campaign_data.get("ad_channel") == 1 and "app" in campaign_data:
            campaign_request["app"] = campaign_data["app"]
            
        # status
        # Update always requires status (HTTP 400 if missing).
        # Default to 1 (Active) if not in data.
        if "cpg_status" in campaign_data:
            campaign_request["cpg_status"] = campaign_data["cpg_status"]
        else:
            campaign_request["cpg_status"] = 1
        
        return campaign_request
    
    def _extract_ad_group_data(self, ad_group_data: Dict[str, Any], campaign_id: int) -> Dict[str, Any]:
        """
        從 ad_group_data 中提取 Ad Group 創建所需資料
        """
        ad_group_request = {
            "cpg_id": campaign_id,
            "group_name": ad_group_data.get("group_name", ""),
            "target_info": ad_group_data.get("target_info", ""),
            "click_url": ad_group_data.get("click_url", []),
            "impression_url": ad_group_data.get("impression_url", [])
        }
        
        # 添加 budget 物件
        if "budget" in ad_group_data:
            ad_group_request["budget"] = ad_group_data["budget"]
        
        # 添加 schedule 物件
        if "schedule" in ad_group_data:
            ad_group_request["schedule"] = ad_group_data["schedule"]
        
        # 添加 location 物件
        if "location" in ad_group_data:
            ad_group_request["location"] = ad_group_data["location"]
        
        # 添加 audience_target 物件
        if "audience_target" in ad_group_data:
            ad_group_request["audience_target"] = ad_group_data["audience_target"]
            
        # status
        # Same logic as campaign: Update might require status
        if "group_status" in ad_group_data:
            ad_group_request["group_status"] = ad_group_data["group_status"]
        else:
            ad_group_request["group_status"] = 1
        
        return ad_group_request
    
    def _extract_creative_data(self, ad_asset_data: Dict[str, Any], ad_group_id: int) -> Dict[str, Any]:
        """
        從 ad_asset_data 中提取 Creative 創建所需資料
        """
        creative_request = {
            "group_id": ad_group_id,
            "cr_name": ad_asset_data.get("cr_name", ""),
            "cr_title": ad_asset_data.get("cr_title", ""),
            "cr_desc": ad_asset_data.get("cr_desc", ""),
            "cr_btn_text": ad_asset_data.get("cr_btn_text", ""),
            "iab": ad_asset_data.get("iab", "IAB1"),
            "cr_mt_id": ad_asset_data.get("cr_mt_id", 0),
            "cr_icon_id": ad_asset_data.get("cr_icon_id", 0)
        }
        
        
        # status (using cr_status key for creation)
        if "cr_status" in ad_asset_data:
            creative_request["cr_status"] = ad_asset_data["cr_status"]
        else:
            creative_request["cr_status"] = 1
            
        return creative_request
    
    def _generate_detailed_results(self, results: List[CampaignResult], campaigns_data: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """生成包含名稱信息的詳細結果"""
        detailed_results = []
        
        for r in results:
            # 獲取 campaign 資料
            campaign_data = campaigns_data[r.campaign_index] if campaigns_data and r.campaign_index < len(campaigns_data) else {}
            campaign_name = campaign_data.get("cpg_name", "Unknown Campaign")
            
            campaign_detail = {
                "campaign_index": r.campaign_index,
                "campaign_name": campaign_name,
                "success": r.success,
                "campaign_id": r.campaign_id,
                "error_message": r.error_message,
                "retry_count": r.retry_count,
                "ad_groups": []
            }
            
            # 處理 Ad Groups
            ad_groups_data = campaign_data.get("ad_group", [])
            for ag in r.ad_group_results:
                # 獲取 ad group 資料
                ad_group_data = ad_groups_data[ag.ad_group_index] if ag.ad_group_index < len(ad_groups_data) else {}
                ad_group_name = ad_group_data.get("group_name", "Unknown Ad Group")
                
                ad_group_detail = {
                    "ad_group_index": ag.ad_group_index,
                    "ad_group_name": ad_group_name,
                    "success": ag.success,
                    "ad_group_id": ag.ad_group_id,
                    "error_message": ag.error_message,
                    "ad_assets": []
                }
                
                # 處理 Ad Assets
                ad_assets_data = ad_group_data.get("ad_asset", [])
                for aa in ag.ad_asset_results:
                    # 獲取 creative 資料
                    ad_asset_data = ad_assets_data[aa.ad_asset_index] if aa.ad_asset_index < len(ad_assets_data) else {}
                    creative_name = ad_asset_data.get("cr_name", "Unknown Creative")
                    
                    ad_asset_detail = {
                        "ad_asset_index": aa.ad_asset_index,
                        "creative_name": creative_name,
                        "success": aa.success,
                        "creative_id": aa.creative_id,
                        "error_message": aa.error_message
                    }
                    
                    ad_group_detail["ad_assets"].append(ad_asset_detail)
                
                campaign_detail["ad_groups"].append(ad_group_detail)
            
            detailed_results.append(campaign_detail)
        
        return detailed_results
    
    def _generate_error_summary(self, results: List[CampaignResult], campaigns_data: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """生成錯誤摘要，包含index和name信息"""
        errors = []
        
        for r in results:
            campaign_data = campaigns_data[r.campaign_index] if campaigns_data and r.campaign_index < len(campaigns_data) else {}
            campaign_name = campaign_data.get("cpg_name", "Unknown Campaign")
            
            # Campaign 層級錯誤
            if not r.success and r.error_message:
                errors.append({
                    "level": "campaign",
                    "index": r.campaign_index,
                    "name": campaign_name,
                    "error_message": r.error_message,
                    "retry_count": r.retry_count
                })
            
            # Ad Group 層級錯誤
            ad_groups_data = campaign_data.get("ad_group", [])
            for ag in r.ad_group_results:
                # 每次迴圈都獲取 ad_group_data，確保變數存在
                ad_group_data = ad_groups_data[ag.ad_group_index] if ag.ad_group_index < len(ad_groups_data) else {}
                ad_group_name = ad_group_data.get("group_name", "Unknown Ad Group")
                
                if not ag.success and ag.error_message:
                    errors.append({
                        "level": "ad_group",
                        "campaign_index": r.campaign_index,
                        "campaign_name": campaign_name,
                        "ad_group_index": ag.ad_group_index,
                        "ad_group_name": ad_group_name,
                        "error_message": ag.error_message
                    })
                
                # Creative 層級錯誤
                ad_assets_data = ad_group_data.get("ad_asset", [])
                for aa in ag.ad_asset_results:
                    if not aa.success and aa.error_message:
                        ad_asset_data = ad_assets_data[aa.ad_asset_index] if aa.ad_asset_index < len(ad_assets_data) else {}
                        creative_name = ad_asset_data.get("cr_name", "Unknown Creative")
                        
                        errors.append({
                            "level": "creative",
                            "campaign_index": r.campaign_index,
                            "campaign_name": campaign_name,
                            "ad_group_index": ag.ad_group_index,
                            "ad_group_name": ad_group_name,
                            "creative_index": aa.ad_asset_index,
                            "creative_name": creative_name,
                            "error_message": aa.error_message
                        })
        
        return errors
    
    def _generate_summary(self, results: List[CampaignResult], campaigns_data: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """生成處理結果統計，確保同一ID只計算一次成功，並區分新增與修改"""
        
        # 提取成功的 ID 集合 (Set for de-duplication)
        # Separate sets for Created and Modified to allow distinct counts
        # Note: An ID could potentially be created and then modified in the same batch (unlikely but possible).
        # We will count it as Created if it was created in this batch? 
        # Actually, if I have row 1: Create A, row 2: Update A (using ID from 1? No, Excel doesn't support that dynamic ref).
        # So typically:
        # - Rows with ID -> Update
        # - Rows without ID -> Create
        # We can just count based on the input data intent.
        
        created_campaign_ids = set()
        modified_campaign_ids = set()
        
        created_ad_group_ids = set()
        modified_ad_group_ids = set()
        
        created_creative_ids = set()
        modified_creative_ids = set()
        
        # 統計失敗次數 (Operation based)
        failed_campaigns = 0
        failed_ad_groups = 0
        failed_ad_assets = 0
        
        # 統計總操作數 (Operation based)
        total_campaigns_ops = len(results)
        total_ad_groups_ops = 0
        total_ad_assets_ops = 0
        
        for campaign_result in results:
            # Check intended action from source data
            cpg_input = campaigns_data[campaign_result.campaign_index] if campaigns_data else {}
            is_cpg_update = bool(cpg_input.get("cpg_id"))
            
            if campaign_result.success and campaign_result.campaign_id:
                if is_cpg_update:
                    modified_campaign_ids.add(campaign_result.campaign_id)
                else:
                    created_campaign_ids.add(campaign_result.campaign_id)
            else:
                failed_campaigns += 1
                
            # Ad Groups
            ad_groups_input = cpg_input.get("ad_group", [])
            
            for ad_group_result in campaign_result.ad_group_results:
                total_ad_groups_ops += 1
                
                ag_input = ad_groups_input[ad_group_result.ad_group_index] if ad_group_result.ad_group_index < len(ad_groups_input) else {}
                is_grp_update = bool(ag_input.get("group_id"))
                
                if ad_group_result.success and ad_group_result.ad_group_id:
                    if is_grp_update:
                        modified_ad_group_ids.add(ad_group_result.ad_group_id)
                    else:
                        created_ad_group_ids.add(ad_group_result.ad_group_id)
                else:
                    failed_ad_groups += 1
                
                # Ad Assets
                ad_assets_input = ag_input.get("ad_asset", [])
                
                for ad_asset_result in ad_group_result.ad_asset_results:
                    total_ad_assets_ops += 1
                    
                    aa_input = ad_assets_input[ad_asset_result.ad_asset_index] if ad_asset_result.ad_asset_index < len(ad_assets_input) else {}
                    is_cr_update = bool(aa_input.get("cr_id"))
                    
                    if ad_asset_result.success and ad_asset_result.creative_id:
                        if is_cr_update:
                            modified_creative_ids.add(ad_asset_result.creative_id)
                        else:
                            created_creative_ids.add(ad_asset_result.creative_id)
                    else:
                        failed_ad_assets += 1
        
        n_created_cpg = len(created_campaign_ids)
        n_modified_cpg = len(modified_campaign_ids)
        n_success_cpg = n_created_cpg + n_modified_cpg # Total unique successes
        
        n_created_grp = len(created_ad_group_ids)
        n_modified_grp = len(modified_ad_group_ids)
        n_success_grp = n_created_grp + n_modified_grp
        
        n_created_cr = len(created_creative_ids)
        n_modified_cr = len(modified_creative_ids)
        n_success_cr = n_created_cr + n_modified_cr
        
        return {
            "summary": {
                "total_campaigns": total_campaigns_ops,
                "successful_campaigns": n_success_cpg,
                "created_campaigns": n_created_cpg,
                "modified_campaigns": n_modified_cpg,
                "failed_campaigns": failed_campaigns,
                "campaign_success_rate": round(n_success_cpg / total_campaigns_ops * 100, 2) if total_campaigns_ops > 0 else 0,
                
                "total_ad_groups": total_ad_groups_ops,
                "successful_ad_groups": n_success_grp,
                "created_ad_groups": n_created_grp,
                "modified_ad_groups": n_modified_grp,
                "failed_ad_groups": failed_ad_groups,
                "ad_group_success_rate": round(n_success_grp / total_ad_groups_ops * 100, 2) if total_ad_groups_ops > 0 else 0,
                
                "total_ad_assets": total_ad_assets_ops,
                "successful_ad_assets": n_success_cr,
                "created_ad_assets": n_created_cr,
                "modified_ad_assets": n_modified_cr,
                "failed_ad_assets": failed_ad_assets,
                "creative_success_rate": round(n_success_cr / total_ad_assets_ops * 100, 2) if total_ad_assets_ops > 0 else 0
            },
            "successful_ids": {
                "campaign_ids": list(created_campaign_ids | modified_campaign_ids),
                "ad_group_ids": list(created_ad_group_ids | modified_ad_group_ids),
                "creative_ids": list(created_creative_ids | modified_creative_ids)
            },
            "details": self._generate_detailed_results(results, campaigns_data),
            "errors": self._generate_error_summary(results, campaigns_data)
        }