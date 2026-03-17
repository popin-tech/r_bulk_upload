import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from typing import Dict, Any, List
import logging
import os

logger = logging.getLogger(__name__)

class MediaDashboardService:
    def __init__(self, key_path: str = "config/service_account.json"):
        self.key_path = key_path
        self.scopes = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly"
        ]
        self.sheet_url = "https://docs.google.com/spreadsheets/d/1cdvNpZKMMaoDMjZ4CaKUkZXKGaYIWY_QDTdoHAYlmT0/edit"
        self.worksheet_id = 176593541
        self.gc = None
        self._authenticate()

    def _authenticate(self):
        try:
            import json
            credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
            if credentials_json:
                logger.info("Using service account from GOOGLE_CREDENTIALS_JSON environment variable.")
                try:
                    info = json.loads(credentials_json)
                    credentials = Credentials.from_service_account_info(info, scopes=self.scopes)
                except Exception as e:
                    logger.error(f"Failed to parse GOOGLE_CREDENTIALS_JSON: {e}")
                    raise
            elif os.path.exists(self.key_path):
                credentials = Credentials.from_service_account_file(self.key_path, scopes=self.scopes)
            else:
                import google.auth
                logger.info(f"Service account key not found at {self.key_path}, falling back to Application Default Credentials.")
                credentials, _ = google.auth.default(scopes=self.scopes)
                
            self.gc = gspread.authorize(credentials)
            logger.info("Successfully authenticated with Google Sheets API.")
        except Exception as e:
            logger.error(f"Failed to authenticate with Google Sheets: {e}")

    def fetch_raw_data(self) -> pd.DataFrame:
        """從 Google Sheet 取得所有原始資料並轉為 DataFrame"""
        if not self.gc:
            self._authenticate()
            if not self.gc:
                raise Exception("Google Sheets API client not initialized.")
        try:
            sh = self.gc.open_by_url(self.sheet_url)
            worksheet = sh.get_worksheet_by_id(self.worksheet_id)
            data = worksheet.get_all_records()
            return pd.DataFrame(data)
        except Exception as e:
            logger.error(f"Failed to fetch data from Google Sheet: {e}")
            raise Exception(f"Failed to fetch data: {e}")

    def get_dashboard_data(self, start_date_str: str = None, end_date_str: str = None) -> Dict[str, Any]:
        """
        取得 Native Dashboard 摘要數據：
        - 依據 `日期` 過濾 (預設為資料中最新日期的過去 30 天)
        - 計算 6 大 Overview 總和指標
        - 依據 廣告Imp 排序取 Top 5 媒體，並回傳時序圖表資料與該媒體各項指標總計/平均
        """
        df = self.fetch_raw_data()
        if df.empty:
             return {"overview": {}, "top_5_media": [], "available_dates": []}
             
        # 清除欄位名稱前後空白 (避免 ' 還原收益 ' 取不到)
        df.columns = df.columns.str.strip()

        # 型別轉換，確保欄位為數字
        numeric_cols = ['廣告Imp', '廣告點擊次數', '還原收益', '媒體收益', '博英利潤']
        for col in numeric_cols:
            if col in df.columns:
                # 移除可能的千分位逗號再轉數字
                if df[col].dtype == object:
                    df[col] = df[col].astype(str).str.replace(',', '')
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                
        # 日期處理
        if '日期' not in df.columns:
             return {"error": "找不到『日期』欄位"}
        df['日期'] = pd.to_datetime(df['日期'], format="%Y/%m/%d", errors='coerce')
        df = df.dropna(subset=['日期'])
        
        # 取得所有可用日期
        available_dates = sorted(df['日期'].dt.strftime('%Y-%m-%d').unique().tolist())
        latest_date = df['日期'].max()
        
        # 過濾區間
        if start_date_str and end_date_str:
            start_date = pd.to_datetime(start_date_str)
            end_date = pd.to_datetime(end_date_str)
        else:
            # 預設：最新日期的前 30 天 (包含最新日期，故 >= 最新日期 - 29 天)
            end_date = latest_date
            start_date = end_date - pd.Timedelta(days=29)

        mask = (df['日期'] >= start_date) & (df['日期'] <= end_date)
        filtered_df = df.loc[mask].copy()
        
        # 如果過濾後沒資料
        if filtered_df.empty:
            return {
                "overview": {
                   "imp": 0, "click": 0, "cpm": 0, "ctr": 0, "media_rev": 0, "popin_profit": 0,
                   "date_range": f"{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}"
                },
                "top_5_media": [],
                "available_dates": available_dates
            }

        # ---------------- 1. 總覽數據 (Overview) ----------------
        total_imp = float(filtered_df['廣告Imp'].sum())
        total_click = float(filtered_df['廣告點擊次數'].sum())
        total_restore_rev = float(filtered_df.get('還原收益', 0).sum())
        total_media_rev = float(filtered_df.get('媒體收益', 0).sum())
        total_popin_profit = float(filtered_df.get('博英利潤', 0).sum())
        
        overall_cpm = (total_restore_rev / total_imp) * 1000 if total_imp > 0 else 0
        overall_ctr = (total_click / total_imp) * 100 if total_imp > 0 else 0

        overview = {
            "imp": total_imp,
            "click": total_click,
            "cpm": round(overall_cpm, 2),
            "ctr": round(overall_ctr, 2),
            "media_rev": total_media_rev,
            "popin_profit": total_popin_profit,
            "date_range": f"{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}",
            "start_date": start_date.strftime('%Y-%m-%d'),
            "end_date": end_date.strftime('%Y-%m-%d')
        }

        # ---------------- 2. Top 5 媒體分析 ----------------
        group_key = '媒體' if '媒體' in filtered_df.columns else '域名'
        
        # 先計算各媒體加總以排序 Top 5
        media_agg = filtered_df.groupby(group_key).agg({
            '廣告Imp': 'sum',
            '廣告點擊次數': 'sum',
            '還原收益': 'sum',
            '媒體收益': 'sum',
            '博英利潤': 'sum'
        }).reset_index()
        
        # 排序取 Top 5
        top5_names = media_agg.sort_values(by='廣告Imp', ascending=False).head(5)[group_key].tolist()
        
        top_5_media_data = []
        # 針對這 Top 5 的媒體，取出每天的資料以供作圖
        for media_name in top5_names:
            media_df = filtered_df[filtered_df[group_key] == media_name].copy()
            
            # --- Daily Timeline Data ---
            daily_agg = media_df.groupby('日期').agg({
                '廣告Imp': 'sum',
                '廣告點擊次數': 'sum',
                '還原收益': 'sum'
            }).reset_index().sort_values('日期')
            
            timeline_dates = daily_agg['日期'].dt.strftime('%m-%d').tolist()
            timeline_imp = daily_agg['廣告Imp'].tolist()
            timeline_click = daily_agg['廣告點擊次數'].tolist()
            
            # 每日 CPM 與 CTR
            timeline_cpm = []
            timeline_ctr = []
            for _, row in daily_agg.iterrows():
                imp = row['廣告Imp']
                click = row['廣告點擊次數']
                rev = row['還原收益']
                
                day_cpm = (rev / imp * 1000) if imp > 0 else 0
                day_ctr = (click / imp * 100) if imp > 0 else 0
                
                timeline_cpm.append(round(day_cpm, 2))
                timeline_ctr.append(round(day_ctr, 2))
                
            # --- Summary Data ---
            m_imp = float(media_df['廣告Imp'].sum())
            m_click = float(media_df['廣告點擊次數'].sum())
            m_res_rev = float(media_df.get('還原收益', 0).sum())
            m_media_rev = float(media_df.get('媒體收益', 0).sum())
            m_popin_profit = float(media_df.get('博英利潤', 0).sum())
            
            m_cpm = (m_res_rev / m_imp) * 1000 if m_imp > 0 else 0
            m_ctr = (m_click / m_imp) * 100 if m_imp > 0 else 0
            
            top_5_media_data.append({
                "media_name": str(media_name),
                "summary": {
                    "imp": m_imp,
                    "click": m_click,
                    "cpm": round(m_cpm, 2),
                    "ctr": round(m_ctr, 2),
                    "media_rev": m_media_rev,
                    "popin_profit": m_popin_profit
                },
                "timeline": {
                    "labels": timeline_dates,
                    "imp": timeline_imp,
                    "click": timeline_click,
                    "cpm": timeline_cpm,
                    "ctr": timeline_ctr
                }
            })

        return {
            "overview": overview,
            "top_5_media": top_5_media_data,
            "available_dates": available_dates
        }
