from __future__ import annotations
from typing import Dict, List, Any, Optional
import logging
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class CampaignResult:
    """單個 Campaign 處理結果"""
    row_index: int
    success: bool
    campaign_id: Optional[int] = None
    ad_group_id: Optional[int] = None
    creative_id: Optional[int] = None
    error_message: Optional[str] = None
    retry_count: int = 0

class CampaignBulkProcessor:
    """
    Campaign 批量處理器
    
    只需要傳入：
    1. api_token: 已交換後的API token
    2. payload: 整理後的Excel資料
    """
    
    def __init__(self, api_token: str):
        # 動態導入避免循環導入
        import app
        self.client = app._broadciel_client()
        self.api_token = api_token
        self.max_retries = 2
    
    def process_bulk_campaigns(
        self,
        payload: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        批量處理 Campaign 創建
        
        Args:
            payload: Excel處理後的JSON格式資料
            
        Returns:
            處理結果統計和詳細資訊
        """
        results = []
        
        for index, campaign_data in enumerate(payload):
            result = self._process_single_campaign_with_retry(
                row_index=index,
                campaign_data=campaign_data
            )
            results.append(result)
        
        return self._generate_summary(results)
    
    def _process_single_campaign_with_retry(
        self,
        row_index: int,
        campaign_data: Dict[str, Any]
    ) -> CampaignResult:
        """處理單個 Campaign，包含重試邏輯"""
        
        result = CampaignResult(
            row_index=row_index,
            success=False
        )
        
        for attempt in range(self.max_retries + 1):
            created_campaign_id = None
            created_ad_group_id = None
            created_creative_id = None
            failed_step = ""
            
            try:
                result.retry_count = attempt
                
                # 步驟 1: 創建 Campaign
                failed_step = "campaign"
                campaign_request_body = self._extract_campaign_data(campaign_data)
                created_campaign_id = self.client.create_campaign(campaign_request_body, self.api_token)
                
                if not created_campaign_id:
                    raise Exception("Campaign creation returned empty ID")
                
                # 步驟 2: 創建 Ad Group
                failed_step = "ad_group"
                ad_group_request_body = self._extract_ad_group_data(campaign_data, created_campaign_id)
                created_ad_group_id = self.client.create_ad_group(ad_group_request_body, self.api_token)
                
                if not created_ad_group_id:
                    raise Exception("Ad Group creation returned empty ID")
                
                # 步驟 3: 創建 Creative
                failed_step = "creative"
                creative_request_body = self._extract_creative_data(campaign_data, created_ad_group_id)
                created_creative_id = self.client.create_creative(creative_request_body, self.api_token)
                
                if not created_creative_id:
                    raise Exception("Creative creation returned empty ID")
                
                # 全部成功，記錄到 result
                result.campaign_id = created_campaign_id
                result.ad_group_id = created_ad_group_id
                result.creative_id = created_creative_id
                result.success = True
                logger.info(f"Successfully processed campaign at row {row_index} after {attempt} retries")
                break
                
            except Exception as e:
                # 發生錯誤時進行回滾
                self._rollback_created_resources(
                    created_campaign_id, 
                    created_ad_group_id, 
                    created_creative_id,
                    row_index
                )
                
                error_msg = f"Attempt {attempt + 1} failed at {failed_step}: {str(e)}"
                logger.warning(f"Row {row_index} - {error_msg}")
                
                if attempt < self.max_retries:
                    time.sleep(1 * (attempt + 1))
                    continue
                else:
                    result.error_message = f"Failed after {self.max_retries + 1} attempts. Last error at {failed_step}: {str(e)}"
                    logger.error(f"Row {row_index} - {result.error_message}")
        
        return result
    
    def _rollback_created_resources(
        self, 
        campaign_id: Optional[int], 
        ad_group_id: Optional[int], 
        creative_id: Optional[int],
        row_index: int
    ) -> None:
        """回滾已創建的資源"""
        
        # 刪除順序：Creative -> Ad Group -> Campaign
        if creative_id:
            try:
                if self.client.delete_creative(creative_id, self.api_token):
                    logger.info(f"Row {row_index} - Successfully deleted creative {creative_id}")
                else:
                    logger.warning(f"Row {row_index} - Failed to delete creative {creative_id}")
            except Exception as e:
                logger.error(f"Row {row_index} - Error deleting creative {creative_id}: {str(e)}")
        
        if ad_group_id:
            try:
                if self.client.delete_ad_group(ad_group_id, self.api_token):
                    logger.info(f"Row {row_index} - Successfully deleted ad group {ad_group_id}")
                else:
                    logger.warning(f"Row {row_index} - Failed to delete ad group {ad_group_id}")
            except Exception as e:
                logger.error(f"Row {row_index} - Error deleting ad group {ad_group_id}: {str(e)}")
        
        if campaign_id:
            try:
                if self.client.delete_campaign(campaign_id, self.api_token):
                    logger.info(f"Row {row_index} - Successfully deleted campaign {campaign_id}")
                else:
                    logger.warning(f"Row {row_index} - Failed to delete campaign {campaign_id}")
            except Exception as e:
                logger.error(f"Row {row_index} - Error deleting campaign {campaign_id}: {str(e)}")
    
    def _extract_campaign_data(self, campaign_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        從 payload 中提取 Campaign 創建所需資料
        TODO: 等 payload 格式確定後實作
        """
        return {
            # 暫時返回測試資料
            "name": campaign_data.get("campaign_name", "Test Campaign"),
            "status": "PAUSED"
        }
    
    def _extract_ad_group_data(self, campaign_data: Dict[str, Any], campaign_id: int) -> Dict[str, Any]:
        """
        從 payload 中提取 Ad Group 創建所需資料
        TODO: 等 payload 格式確定後實作
        """
        return {
            # 暫時返回測試資料
            "campaign_id": campaign_id,
            "name": campaign_data.get("ad_group_name", "Test Ad Group"),
            "status": "ENABLED"
        }
    
    def _extract_creative_data(self, campaign_data: Dict[str, Any], ad_group_id: int) -> Dict[str, Any]:
        """
        從 payload 中提取 Creative 創建所需資料
        TODO: 等 payload 格式確定後實作
        """
        return {
            # 暫時返回測試資料
            "ad_group_id": ad_group_id,
            "title": campaign_data.get("title", "Test Creative"),
            "image_url": campaign_data.get("image_url", "https://example.com/test.jpg"),
            "type": "IMAGE"
        }
    
    def _generate_summary(self, results: List[CampaignResult]) -> Dict[str, Any]:
        """生成處理結果統計"""
        total = len(results)
        successful = sum(1 for r in results if r.success)
        failed = total - successful
        
        return {
            "summary": {
                "total_campaigns": total,
                "successful": successful,
                "failed": failed,
                "success_rate": round(successful / total * 100, 2) if total > 0 else 0
            },
            "details": [
                {
                    "row_index": r.row_index,
                    "success": r.success,
                    "campaign_id": r.campaign_id,
                    "ad_group_id": r.ad_group_id,
                    "creative_id": r.creative_id,
                    "error_message": r.error_message,
                    "retry_count": r.retry_count
                }
                for r in results
            ]
        }