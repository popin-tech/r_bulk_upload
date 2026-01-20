from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta


import pandas as pd


class UploadParsingError(RuntimeError):
    pass

def parse_excel_df(file_bytes: bytes) -> pd.DataFrame:
    try:
        df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl")
    except Exception as exc:  # pragma: no cover - defensive logging
        raise UploadParsingError("Unable to parse uploaded Excel file") from exc

    if df.empty:
        raise UploadParsingError("Sheet contains zero rows.")
    df = df.dropna(how="all")

    return df

def _validate_datetime_format(value: str, excel_row_num: int, field: str) -> str:
    if not value:
        return value

    parsed_dt = None

    # 若 Excel 讀入是 timestamp / datetime 物件
    if isinstance(value, (datetime, pd.Timestamp)):
        parsed_dt = value
    else:
        s = value.strip()
        # 正規 YYYY-MM-DD HH
        try:
            parsed_dt = datetime.strptime(s, "%Y-%m-%d %H")
        except Exception:
            pass

        if parsed_dt is None:
            # 若使用者輸入 yyyy/mm/dd hh, yyyy.mm.dd hh 也自動修正
            for sep in ["/", "."]:
                if sep in s:
                    s_fixed = s.replace(sep, "-")
                    try:
                        parsed_dt = datetime.strptime(s_fixed, "%Y-%m-%d %H")
                        break
                    except Exception:
                        pass
    
    # 成功解析後，進行時區扣減 (UTC+8 -> UTC)
    if parsed_dt:
        # Subtract 8 hours
        final_dt = parsed_dt - timedelta(hours=8)
        return final_dt.strftime("%Y-%m-%d %H")

    # 都不符合 → 拋錯
    raise UploadParsingError(
        f"Row {excel_row_num}: 欄位「{field}」格式錯誤，必須為 yyyy-mm-dd hh，例如：2025-12-10 08"
    )

def excel_to_campaign_json(df: pd.DataFrame, audience_name_map: Optional[Dict[str, int]] = None) -> Dict[str, object]:
    """
    Convert the uploaded Excel sheet into the nested campaign JSON structure
    expected by Broadciel Ads v2 bulk campaign API.

    The mapping below is based on the current template header row:
      廣告活動名稱, 廣告活動ID, 每日預算(NT$), 產品類型, APP名稱, 操作系統, 品牌名稱, 廣告群組名稱,
      廣告群組ID, 網站推廣連結, 第三方點擊追蹤連結(Grouped), 第三方曝光追蹤連結(Grouped),
      行銷目標, 計費模式, 固定出價, 每日預算, 深度轉換目標, 轉換價值, 轉化目標, 開始日期, 結束日期,
      投放星期數, 投放時間段, 地理位置, 國家, 設備類型, 流量類型, 操作系統, 最高系統版本, 瀏覽器,
      年齡, 性別, 投放興趣選項, 投放興趣受眾, 自定義受眾（包含）, 自定義受眾（不包含）,
      AI語意擴充選項, AI語意擴充關鍵字, 廣告文案名稱, 廣告文案ID, 廣告類型, 廣告標題,
      廣告內文, Call to Action, 廣告素材ID

    NOTE:
      - Only clearly mappable fields are populated; many enum / type codes are left
        for the API layer to fill or default.
      - This function is intentionally defensive: missing / unexpected columns are
        tolerated and simply omitted from the output.
    """

    from urllib.parse import urlparse

    def _get_str(row: pd.Series, col: str) -> str:
        if col not in row or pd.isna(row[col]):
            return ""
        return str(row[col]).strip()

    def _get_optional_str(row: pd.Series, col: str) -> Optional[str]:
        if col not in row:
            return None
        s = _get_str(row, col)
        return s or None

    def _to_float(val: Any) -> Optional[float]:
        if pd.isna(val):
            return None
        try:
            if isinstance(val, str):
                val = val.replace(",", "")
            return float(val)
        except Exception:
            return None

    def _split_list(val: Any) -> List[str]:
        if pd.isna(val):
            return []
        if isinstance(val, (list, tuple)):
            return [str(v).strip() for v in val if str(v).strip()]
        s = str(val).strip()
        if not s:
            return []
        for sep in ["\n", ";", "；", "|"]:
            s = s.replace(sep, ",")
        parts = [p.strip() for p in s.split(",")]
        return [p for p in parts if p]

    def _parse_int_list(val: Any) -> List[int]:
        items: List[int] = []
        for token in _split_list(val):
            try:
                items.append(int(token))
            except Exception:
                # ignore non-int tokens
                continue
        return items

    # Identify columns
    # App OS: "操作系統"
    # Target OS: "受眾操作系統"
    app_os_col: Optional[str] = "操作系統"
    target_os_col: Optional[str] = "受眾操作系統"
    
    # Check existence
    if app_os_col not in df.columns:
        app_os_col = None
    if target_os_col not in df.columns:
        target_os_col = None

    campaigns: Dict[str, Dict[str, Any]] = {}

    for idx, row in df.iterrows():
        excel_row_num = idx + 2  # header assumed row 1

        campaign_name = _get_str(row, "廣告活動名稱")
        if not campaign_name:
            # skip empty campaign rows
            continue

        product_type_raw = _get_str(row, "產品類型")
        product_type = product_type_raw.lower()
        app_name_raw = _get_optional_str(row, "APP名稱")
        app_os_raw = _get_optional_str(row, app_os_col) if app_os_col else None

        app_present = bool(app_name_raw)
        os_present = bool(app_os_raw)

        # 保留原本邏輯：web 活動不能填 APP / OS
        if product_type == "web" and (app_present or os_present):
            raise UploadParsingError(
                f"Row {excel_row_num}: Web campaign must not have APP名稱 or 操作系統 filled."
            )

        # 初始化或取得 campaign 物件
        if campaign_name not in campaigns:
            campaign: Dict[str, Any] = {
                # 廣告活動名稱 → cpg_name
                "cpg_name": campaign_name,
            }

            # 廣告活動ID → cpg_id
            cpg_id_val = _get_optional_str(row, "廣告活動ID")
            if cpg_id_val is not None:
                try:
                    campaign["cpg_id"] = int(float(cpg_id_val))
                except Exception:
                    pass

            # 活動層級預算
            cpg_budget_val = row.get("每日預算(NT$)")
            cpg_budget = _to_float(cpg_budget_val)
            if cpg_budget is not None:
                # 每日預算(NT$) → day_budget
                campaign["day_budget"] = cpg_budget

            # 品牌名稱 → sponsored
            brand = _get_optional_str(row, "品牌名稱")
            if brand:
                campaign["sponsored"] = brand

            # 主網域名稱 → adomain
            adomain = _get_optional_str(row, "主網域名稱")
            if adomain is not None:
                # 移除 http:// 或 https://
                cleaned_adomain = adomain.lower()
                for prefix in ["https://", "http://"]:
                    if cleaned_adomain.startswith(prefix):
                        cleaned_adomain = cleaned_adomain[len(prefix):]
                # 也要移除結尾的 / (如果有的話)
                cleaned_adomain = cleaned_adomain.rstrip("/")
                
                campaign["adomain"] = cleaned_adomain
            else:
                raise UploadParsingError(
                        f"Row {excel_row_num}: 主網域名稱必須填寫。"
                    )

            # 產品類型 → ad_channel (1=app, 2=web)
            if product_type == "app":
                campaign["ad_channel"] = 1
            elif product_type == "web":
                campaign["ad_channel"] = 2

            # app-only fields (APP名稱 / 操作系統)
            if product_type == "app":
                app_obj: Dict[str, Any] = {}
                if app_name_raw:
                    # APP名稱 → app.ad_target
                    app_obj["ad_target"] = app_name_raw

                # 操作系統 → app.ad_platform (1=iOS, 2=Android)
                if app_os_raw:
                    os_name = app_os_raw.lower()
                    if "ios" in os_name:
                        app_obj["ad_platform"] = 1
                    elif "android" in os_name:
                        app_obj["ad_platform"] = 2

                # 規則：ad_channel 為 1 (app) 時，ad_target / ad_platform 不可為空
                if not app_obj.get("ad_target") or not app_obj.get("ad_platform"):
                    raise UploadParsingError(
                        f"Row {excel_row_num}: APP 活動必須填寫 APP名稱 與 操作系統。"
                    )

                campaign["app"] = app_obj

            # 廣告活動狀態 -> cpg_status (1=開啟, 2=關閉)
            cpg_status_raw = _get_optional_str(row, "廣告活動狀態")
            if cpg_status_raw:
                 # Check for text or numeric representation
                 v = cpg_status_raw.strip()
                 if v == "開啟":
                      campaign["cpg_status"] = 1
                 elif v == "關閉":
                      campaign["cpg_status"] = 2
                 elif v in ("1", "1.0"):
                      campaign["cpg_status"] = 1
                 elif v in ("2", "2.0"):
                      campaign["cpg_status"] = 2
            
            campaigns[campaign_name] = campaign
        else:
            campaign = campaigns[campaign_name]

        #
        # 廣告群組 (ad_group) 層級
        #
        group_name = _get_str(row, "廣告群組名稱") or "Default Group"
        ad_groups: List[Dict[str, Any]] = campaign.setdefault("ad_group", [])

        # 找到 / 建立 group
        group: Optional[Dict[str, Any]] = None
        for g in ad_groups:
            if g.get("group_name") == group_name:
                group = g
                break
        if group is None:
            group = {
                # 廣告群組名稱 → group_name
                "group_name": group_name
            }
            # 廣告群組ID → group_id
            group_id_val = _get_optional_str(row, "廣告群組ID")
            if group_id_val is not None:
                try:
                    group["group_id"] = int(float(group_id_val))
                except Exception:
                    pass
            
            # 廣告群組狀態 -> group_status (1=開啟, 2=關閉)
            group_status_raw = _get_optional_str(row, "廣告群組狀態")
            if group_status_raw:
                 v = group_status_raw.strip()
                 if v == "開啟":
                      group["group_status"] = 1
                 elif v == "關閉":
                      group["group_status"] = 2
                 elif v in ("1", "1.0"):
                      group["group_status"] = 1
                 elif v in ("2", "2.0"):
                      group["group_status"] = 2

            ad_groups.append(group)

        #
        # URL / tracking
        #
        # 網站推廣連結 → target_info (主要推廣頁面)
        target_info_url = _get_optional_str(row, "網站推廣連結")
        if target_info_url:
            group["target_info"] = target_info_url

        # 第三方點擊追蹤連結(Grouped) → click_url, only https
        click_urls = [u.strip() for u in _split_list(row.get("第三方點擊追蹤連結(Grouped)")) if u and str(u).strip().startswith("https://")]
        if click_urls:
            group["click_url"] = click_urls

        # 第三方曝光追蹤連結(Grouped) → impression_url, only https and formatted as <img src="url">
        imp_urls = [u.strip() for u in _split_list(row.get("第三方曝光追蹤連結(Grouped)")) if u and str(u).strip().startswith("https://")]
        if imp_urls:
            group["impression_url"] = [{"type": 2, "value": f'<img src="{u}">'} for u in imp_urls]

        #
        # Budget block
        #
        budget: Dict[str, Any] = group.setdefault("budget", {})

        # 行銷目標 → market_goal
        marketing_goal = _get_optional_str(row, "行銷目標")
        if marketing_goal:
            market_target_map = {
                "品牌知名度": 1,
                "電商網上購買": 2,
                "增加網站流量": 3,
                "開發潛在客戶": 5,
                "網站互動": 6,
            }
            market_target = market_target_map.get(marketing_goal)
            if market_target is None:
                raise UploadParsingError(
                    f"Row {excel_row_num}: 不支援的行銷目標「{marketing_goal}」，請確認是否拼寫正確。"
                )
            budget["market_target"] = market_target

        # 計費模式 → rev_type
        billing_type = _get_optional_str(row, "計費模式")
        if billing_type:
            type_map = {"CPM": 2, "CPC": 3}
            budget["rev_type"] = type_map.get(billing_type, billing_type)

        price_val = row.get("固定出價")
        price = _to_float(price_val)
        if price is not None:
            # 固定出價 → price
            budget["price"] = price

        group_day_budget_val = row.get("每日預算")
        group_day_budget = _to_float(group_day_budget_val)
        if group_day_budget is not None:
            # 廣告群組「每日預算」 → budget.day_budget
            budget["day_budget"] = group_day_budget

        # 深度轉換目標 / 轉換價值 / 轉化目標 → conversion_goal 物件
        depth_goal = _get_optional_str(row, "深度轉換目標")  # → conversion_goal
        conv_value = _to_float(row.get("轉換價值"))  # → target_value
        conv_goal = _get_optional_str(row, "轉化目標")  # → convert_event

        conversion_goal: Dict[str, Any] = {}
        if depth_goal is not None:
            #conversion_goal["conversion_goal"] = depth_goal
            goal_type_map = {
                "帳戶預設設定": 0,
                "所有轉換": 1,
                "指定轉換目標": 2,
            }
            t = goal_type_map.get(depth_goal)
            if t is None:
                raise UploadParsingError(
                    f"Row {excel_row_num}: 不支援的深度轉換目標「{depth_goal}」，請確認是否拼寫正確。"
                )
            conversion_goal["type"] = t
            if t == 1 and conv_value is None:
                raise UploadParsingError(
                    f"Row {excel_row_num}: 深度轉換目標為「所有轉換」時，必須填寫『轉換價值』。"
                )
            if t == 2:
                missing = []
                if conv_value is None:
                    missing.append("轉換價值")
                if conv_goal is None:
                    missing.append("轉化目標")
                if missing:
                    raise UploadParsingError(
                        f"Row {excel_row_num}: 深度轉換目標為「指定轉換目標」時，必須填寫「{'、'.join(missing)}」。"
                    )

        # target_value：只有在 type != 0 時才輸出
        if conv_value is not None and t != 0:
            conversion_goal["target_value"] = conv_value

        # convert_event：只有在 type != 0 時才輸出
        if conv_goal is not None and t != 0:
            conv_goal_map = {
                "點擊數": 11,
                "網頁瀏覽": 13,
                "完成註冊": 6,
                "搜尋": 5,
                "收藏": 3,
                "加入購物車": 4,
                "開始結帳": 2,
                "完成結帳": 1,
            }
            cg = conv_goal_map.get(conv_goal)
            if cg is None:
                raise UploadParsingError(
                    f"Row {excel_row_num}: 不支援的轉換目標「{conv_goal}」，請確認是否拼寫正確。"
                )
            conversion_goal["convert_event"] = cg

        if conversion_goal:
            budget["conversion_goal"] = conversion_goal

        #
        # Schedule
        #
        schedule: Dict[str, Any] = group.setdefault("schedule", {})

        # 開始日期 / 結束日期
        start_date_raw = row.get("開始日期")
        end_date_raw = row.get("結束日期")

        if start_date_raw:
            if pd.isna(start_date_raw) or str(start_date_raw).strip() == "":
                schedule["start_date"] = ""
            else:
                start_date = _validate_datetime_format(str(start_date_raw), excel_row_num, "開始日期")
                schedule["start_date"] = start_date

        if end_date_raw:
            if pd.isna(end_date_raw) or str(end_date_raw).strip() == "":
                schedule["end_date"] = ""
            else:
                end_date = _validate_datetime_format(str(end_date_raw), excel_row_num, "結束日期")
                schedule["end_date"] = end_date

        # 投放星期數 → week_days
        week_days_raw = row.get("投放星期數")
        week_days = _parse_int_list(week_days_raw)
        if week_days:
            schedule["week_days"] = week_days

        # 投放時間段 → hours
        hours_raw = row.get("投放時間段")
        hours = _parse_int_list(hours_raw)
        if hours:
            # UTC+8 換成 UTC (local - 8)
            hours_utc = [(h - 8) % 24 for h in hours]
            schedule["hours"] = hours_utc

        #
        # Location
        #
        location: Dict[str, Any] = group.setdefault("location", {})
        # 地理位置 → country_type
        country_type_raw = _get_optional_str(row, "地理位置")
        if country_type_raw:
            country_type_map = {
                "包含": 1,
                "不包含": 2
            }
            country_type = country_type_map.get(country_type_raw)
            if country_type is None:
                raise UploadParsingError(
                    f"Row {excel_row_num}: 不支援的地理位置「{country_type_raw}」，請確認是否拼寫正確。"
                )
            location["country_type"] = country_type
        # 國家 → country
        country = _split_list(row.get("國家"))
        if country:
            location["country"] = country

        #
        # Audience targeting
        #
        audience: Dict[str, Any] = group.setdefault("audience_target", {})

        # 設備類型 → device_type
        device_types = _split_list(row.get("設備類型"))
        if device_types:
            device_types_int = []
            for d in device_types:
                try:
                    device_types_int.append(int(d))
                except Exception:
                    continue
            audience["device_type"] = device_types_int

        # 流量類型 → traffic_type, force int
        traffic_types = _split_list(row.get("流量類型"))
        if traffic_types:
            traffic_types_int = []
            for t in traffic_types:
                try:
                    traffic_types_int.append(int(t))
                except Exception:
                    continue
            audience["traffic_type"] = traffic_types_int

        # 受眾層級 OS / 平台: 操作系統(第二欄) → platform, force int
        target_os_raw = (
            _get_optional_str(row, target_os_col) if target_os_col else None
        )
        if target_os_raw:
            platforms = _split_list(target_os_raw)
            platforms_int = []
            for p in platforms:
                # Handle string inputs (iOS/Android/Others) or Ints
                p_lower = p.lower()
                if "ios" in p_lower:
                    platforms_int.append(1)
                elif "android" in p_lower:
                    platforms_int.append(2)
                elif "others" in p_lower:
                    platforms_int.append(3) # Assuming 3 for Others, or skip?
                else:
                    try:
                        platforms_int.append(int(p))
                    except Exception:
                        continue
            audience["platform"] = platforms_int

        # 最高系統版本 → os_version as {min, max}
        max_os_ver_raw = row.get("最高系統版本")
        min_version = 0
        max_version = 0
        audience["os_version"] = {"min": min_version, "max": max_version}

        # 瀏覽器 → browser, force int
        browsers = _split_list(row.get("瀏覽器"))
        if browsers:
            browsers_int = []
            for b in browsers:
                try:
                    browsers_int.append(int(b))
                except Exception:
                    continue
            audience["browser"] = browsers_int

        # 年齡 → age, force int
        ages = _split_list(row.get("年齡"))
        if ages:
            ages_int = []
            for a in ages:
                try:
                    ages_int.append(int(a))
                except Exception:
                    continue
            audience["age"] = ages_int

        # 性別 → gender, force int
        genders = _split_list(row.get("性別"))
        if genders:
            genders_int = []
            for g in genders:
                try:
                    genders_int.append(int(g))
                except Exception:
                    continue
            audience["gender"] = genders_int

        # 興趣 / IAB / 關鍵字類
        # 投放興趣選項 → category.type (包含=1, 不包含=2)
        # 投放興趣受眾 → category.value (IAB list)
        interest_opt = _get_optional_str(row, "投放興趣選項")
        interest_audience = _split_list(row.get("投放興趣受眾"))
        
        if interest_opt or interest_audience:
            category: Dict[str, Any] = {}
            
            # Parse type from 投放興趣選項 (包含 or 不包含)
            if interest_opt:
                if interest_opt == "包含":
                    category["type"] = 1
                elif interest_opt == "不包含":
                    category["type"] = 2
                else:
                    raise UploadParsingError(
                        f"Row {excel_row_num}: 投放興趣選項必須為「包含」或「不包含」。"
                    )
            
            # Use 投放興趣受眾 (IAB) as the value list
            if interest_audience:
                category["value"] = interest_audience
            
            if category:
                audience["category"] = category

        # AI語意擴充選項 / AI語意擴充關鍵字 → keywords: { type, value }
        # AI語意擴充選項 should be "1" or "2"
        ai_expand_opt_raw = row.get("AI語意擴充選項")
        ai_expand_keywords = _split_list(row.get("AI語意擴充關鍵字"))  # → keywords.value
        
        opt_str = ""
        if ai_expand_opt_raw is not None and not pd.isna(ai_expand_opt_raw):
            opt_str = str(ai_expand_opt_raw).strip().lower()

        # If both option and keywords are empty, skip silently
        if opt_str == "" and not ai_expand_keywords:
            pass
        else:
            kw_type: Optional[int] = None
            if opt_str == "" and ai_expand_keywords:
                # keywords provided but no type
                raise UploadParsingError(
                    f"Row {excel_row_num}: AI語意擴充選項必須為 1 或 2。"
                )
            elif opt_str:
                if opt_str in ("1", "1.0"):
                    kw_type = 1
                elif opt_str in ("2", "2.0"):
                    kw_type = 2
                else:
                    raise UploadParsingError(
                        f"Row {excel_row_num}: AI語意擴充選項必須為 1 或 2。"
                    )

            # Only add keywords if type is set
            if kw_type is not None:
                keywords: Dict[str, Any] = {
                    "type": kw_type,
                    "value": ai_expand_keywords  # Always set value, even if empty array
                }
                audience["keywords"] = keywords

        # 自定義受眾（包含）→ pixel_audience with type: 1
        # 自定義受眾（不包含）→ pixel_audience with type: 2
        pixel_audience_include = _split_list(row.get("自定義受眾（包含）"))
        pixel_audience_exclude = _split_list(row.get("自定義受眾（不包含）"))
        
        pixel_audience_list: List[Dict[str, int]] = []
        
        def _resolve_audience_id(val: str) -> Optional[int]:
            # 1. Try integer
            try:
                return int(val)
            except (ValueError, TypeError):
                pass
            
            # 2. Try map look up
            if audience_name_map:
                # remove whitespace just in case
                v_clean = val.strip()
                if v_clean in audience_name_map:
                    return audience_name_map[v_clean]
            
            return None

        # Add include audiences (type: 1)
        for val_str in pixel_audience_include:
            aid = _resolve_audience_id(val_str)
            if aid is not None:
                pixel_audience_list.append({"id": aid, "type": 1})
            else:
                # Optional: log warning or raise error if name not found?
                # For now, we just skip invalid ones as per original logic, 
                # but maybe logging would be better.
                pass
        
        # Add exclude audiences (type: 2)
        for val_str in pixel_audience_exclude:
            aid = _resolve_audience_id(val_str)
            if aid is not None:
                pixel_audience_list.append({"id": aid, "type": 2})
            else:
                pass
        
        if pixel_audience_list:
            audience["pixel_audience"] = pixel_audience_list

        #
        # Ad assets (creatives) per ad group
        #
        ad_assets: List[Dict[str, Any]] = group.setdefault("ad_asset", [])

        cr_name = _get_str(row, "廣告文案名稱")
        cr_title = _get_str(row, "廣告標題")
        cr_desc = _get_str(row, "廣告內文")
        cr_btn = _get_str(row, "Call to Action")
        cr_iab = _get_str(row, "廣告類型")  # per mapping request

        # 廣告文案狀態 (Creative Status)
        cr_status_raw = _get_optional_str(row, "廣告文案狀態")
        cr_status_val = None
        if cr_status_raw:
            v_cr = cr_status_raw.strip()
            if v_cr == "開啟":
                cr_status_val = 1
            elif v_cr == "關閉":
                cr_status_val = 2
            elif v_cr in ("1", "1.0"):
                cr_status_val = 1
            elif v_cr in ("2", "2.0"):
                cr_status_val = 2

        cr_mt_raw = row.get("廣告素材ID")
        cr_mt_id: Optional[int | str] = None
        if cr_mt_raw is not None and not pd.isna(cr_mt_raw):
            try:
                # Handle "101.0" or "101"
                cr_mt_id = int(float(str(cr_mt_raw).strip()))
            except Exception:
                # If parsing fails, do NOT fallback to string.
                # Valid ID must be int.
                cr_mt_id = None

        # Determine Creative ID (cr_id) for Update
        cr_id_raw = row.get("廣告文案ID")
        cr_id: Optional[int] = None
        if cr_id_raw is not None and not pd.isna(cr_id_raw):
             try:
                 # Handle "123.0"
                 cr_id = int(float(str(cr_id_raw).strip()))
             except:
                 pass

        # Add asset only if at least one meaningful field is present
        # Include cr_id in check
        if any([cr_name, cr_title, cr_desc, cr_btn, cr_iab, cr_mt_id, cr_id]):
            asset: Dict[str, Any] = {
                #"group_id": group.get("group_id"),
                "cr_name": cr_name,
                "cr_title": cr_title,
                "cr_desc": cr_desc,
                "cr_btn_text": cr_btn,
                "iab": cr_iab,
                "cr_mt_id": cr_mt_id if cr_mt_id is not None else 0,
                "cr_icon_id": 0,
            }
            if cr_id:
                asset["cr_id"] = cr_id
            if cr_status_val is not None:
                # Use ad_status generally, or whatever the API needs. 
                # Broadciel Client usually uses cr_status or ad_status depending on endpoint.
                # CampaignBulkProcessor will extract it.
                asset["cr_status"] = cr_status_val
                
            ad_assets.append(asset)

    return {"campaign": list(campaigns.values())}


def dataframe_preview(df: pd.DataFrame, limit: int = 50) -> Dict[str, object]:
    rows = df.fillna("").astype(str).head(limit).to_dict(orient="records")
    return {
        "columns": df.columns.tolist(),
        "rows": rows,
        "total_rows": len(df.index),
        "preview_count": len(rows),
    }


def parse_excel(file_bytes: bytes) -> Dict[str, object]:
    try:
        df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl")
    except Exception as exc:  # pragma: no cover - defensive logging
        raise UploadParsingError("Unable to parse uploaded Excel file") from exc

    if df.empty:
        raise UploadParsingError("Sheet contains zero rows.")

    return dataframe_preview(df)


def generate_excel_from_api_data(
    campaigns: List[Dict[str, Any]],
    ad_groups: List[Dict[str, Any]],
    ad_creatives: List[Dict[str, Any]]
) -> bytes:
    """
    Generate an Excel file (bytes) from the API data structures, 
    matching the upload format exactly (46+3 columns) with Data Validation.
    """
    from openpyxl.worksheet.datavalidation import DataValidation
    
    # helper for status check
    def _is_archived(obj, status_key):
        # 3 = Archived, skip
        val = obj.get(status_key)
        return val == 3

    # 1. Indexing for fast lookup
    # Normalize IDs to int to avoid hash mismatches (e.g. "123" vs 123)
    cpg_map = {}
    for c in campaigns:
        try:
             cid = int(c.get("cpg_id"))
             cpg_map[cid] = c
        except (ValueError, TypeError):
             continue
    
    layout: Dict[int, Any] = {}
    
    for cpg_id, c in cpg_map.items():
        if _is_archived(c, "cpg_status"):
            continue
        layout[cpg_id] = {"self": c, "groups": {}}
        
    for g in ad_groups:
        if _is_archived(g, "group_status"):
            continue
            
        try:
            cpg_id = int(g.get("cpg_id"))
            grp_id = int(g.get("group_id"))
        except (ValueError, TypeError):
            continue
            
        if cpg_id in layout:
            layout[cpg_id]["groups"][grp_id] = {"self": g, "creatives": []}
        else:
            pass
            
    # Track seen creatives to avoid duplicates (e.g. if API returns dupes)
    seen_creatives = set()

    for cr in ad_creatives:
        # Check both ad_status and cr_status, sometimes one is used
        status = cr.get("ad_status") or cr.get("cr_status")
        if status == 3:
            continue
        
        try:
            cpg_id = int(cr.get("cpg_id"))
            grp_id = int(cr.get("group_id"))
            cr_id = int(cr.get("cr_id")) if cr.get("cr_id") is not None else None
        except (ValueError, TypeError):
             continue
        
        # Deduplication check
        if cr_id and (grp_id, cr_id) in seen_creatives:
            continue
            
        if cpg_id in layout:
            if grp_id in layout[cpg_id]["groups"]:
                layout[cpg_id]["groups"][grp_id]["creatives"].append(cr)
                if cr_id:
                    seen_creatives.add((grp_id, cr_id))
                
            else:
                 pass
        else:
             pass
             
    # 2. Flatten to Rows
    rows = []
    
    # Track previous IDs for Sparse Writing (Smart Edit)
    last_cpg_id = None
    last_grp_id = None
    
    # Define Column Mappings (same as before)
    def _map_product_type(val):
        return "app" if val == 1 else "web" 
        
    def _map_os(val):
        if val == 1: return "iOS"
        if val == 2: return "Android"
        if val == 3: return "Others"
        return "" 

    def _map_status(val):
        if val == 1: return "開啟"
        if val == 2: return "關閉"
        return "" 

    def _map_market_goal(val):
        m = {1: "品牌知名度", 2: "電商網上購買", 3: "增加網站流量", 5: "開發潛在客戶", 6: "網站互動"}
        return m.get(val, "")

    def _map_billing(val):
        if val == 2: return "CPM"
        if val == 3: return "CPC"
        return ""
        
    def _map_conversion_goal_type(val):
        m = {0: "帳戶預設設定", 1: "所有轉換", 2: "指定轉換目標"}
        return m.get(val, "")
        
    def _map_convert_event(val):
        m = {11: "點擊數", 13: "網頁瀏覽", 6: "完成註冊", 5: "搜尋", 3: "收藏", 4: "加入購物車", 2: "開始結帳", 1: "完成結帳"}
        return m.get(val, "")

    def _map_country_type(val):
        if val == 1: return "包含"
        if val == 2: return "不包含"
        return ""
        
    def _map_list(lst):
        if not lst: return ""
        return ",".join(str(x) for x in lst)
        
    def _map_hours(utc_hours):
        if not utc_hours: return ""
        # User requested NO timezone adjustment (keep UTC)
        local_hours = [str(h) for h in utc_hours]
        return ",".join(local_hours)

    def _map_list_obj_value(lst):
        urls = []
        for x in lst:
            v = x.get("value", "")
            if v.startswith('<img src="') and v.endswith('">'):
                v = v[10:-2]
            elif v.startswith("<img src='") and v.endswith("'>"):
                v = v[10:-2]
            urls.append(v)
        return ",".join(urls)


    # Re-implementing the nested loop structure with correct sparse check placement
    
    for cpg_id, c_node in layout.items():
        c = c_node["self"]
        groups_node = c_node["groups"]
        
        # Prepare Campaign Values
        c_name = c.get("cpg_name", "")
        c_id = c.get("cpg_id", "")
        c_status = _map_status(c.get("cpg_status"))
        c_budget = c.get("day_budget", "")
        c_domain = c.get("adomain", "")
        c_prod_type = _map_product_type(c.get("ad_channel"))
        app_info = c.get("app", {})
        c_app_name = app_info.get("ad_target", "")
        c_os = _map_os(app_info.get("ad_platform"))
        c_brand = c.get("sponsored", "")
        
        c_cols_data = [
            c_name, c_id, c_status,
            c_budget, c_domain, c_prod_type, c_app_name, c_os, c_brand
        ]
        
        if not groups_node:
            # No groups, write campaign data only
            row = c_cols_data + [""] * 32 + [""] * 8
            rows.append(row)
            continue
            
        for grp_id, g_node in groups_node.items():
            g = g_node["self"]
            creatives = g_node["creatives"]
            
            # Prepare Group Values
            g_name = g.get("group_name", "")
            g_id = g.get("group_id", "")
            g_status = _map_status(g.get("group_status"))
            g_target = g.get("target_info", "")
            g_click = ",".join(g.get("click_url", []))
            g_imp = _map_list_obj_value(g.get("impression_url", []))
            b_obj = g.get("budget", {})
            g_market = _map_market_goal(b_obj.get("market_target"))
            g_rev = _map_billing(b_obj.get("rev_type"))
            g_price = b_obj.get("price", "")
            g_day_budget = b_obj.get("day_budget", "")
            cv_obj = b_obj.get("conversion_goal", {})
            g_depth = _map_conversion_goal_type(cv_obj.get("type"))
            g_cv_val = cv_obj.get("target_value", "")
            g_cv_event = _map_convert_event(cv_obj.get("convert_event"))
            sched = g.get("schedule", {})
            g_start = sched.get("start_date", "")
            g_end = sched.get("end_date", "")
            g_week = _map_list(sched.get("week_days"))
            g_hours = _map_hours(sched.get("hours"))
            loc = g.get("location", {})
            g_loc_type = _map_country_type(loc.get("country_type"))
            g_country = _map_list(loc.get("country"))
            aud = g.get("audience_target", {})
            a_device = _map_list(aud.get("device_type"))
            a_traffic = _map_list(aud.get("traffic_type"))
            a_platform = _map_list(aud.get("platform"))
            a_os_ver = "" 
            a_browser = _map_list(aud.get("browser"))
            a_age = _map_list(aud.get("age"))
            a_gender = _map_list(aud.get("gender"))
            cat = aud.get("category", {})
            a_cat_type = _map_country_type(cat.get("type"))
            a_cat_val = _map_list(cat.get("value"))
            if not a_cat_val:
                a_cat_type = ""
            pix_include = []
            pix_exclude = []
            for p in aud.get("pixel_audience", []):
                pid = p.get("id")
                ptype = p.get("type")
                if ptype == 1: pix_include.append(pid)
                elif ptype == 2: pix_exclude.append(pid)
            a_pix_inc = _map_list(pix_include)
            a_pix_exc = _map_list(pix_exclude)
            kw = aud.get("keywords", {})
            a_ai_type = kw.get("type", "")
            a_ai_val = _map_list(kw.get("value"))
            
            g_cols_data = [
                g_name, g_id, g_status,
                g_target, g_click, g_imp,
                g_market, g_rev, g_price, g_day_budget, g_depth, g_cv_val, g_cv_event, g_start, g_end,
                g_week, g_hours, g_loc_type, g_country, a_device, a_traffic, a_platform, a_os_ver, a_browser,
                a_age, a_gender, a_cat_type, a_cat_val, a_pix_inc, a_pix_exc,
                a_ai_type, a_ai_val
            ]
            
            if not creatives:
                c_cols = c_cols_data 
                g_cols = g_cols_data 
                     
                row = c_cols + g_cols + [""] * 8
                rows.append(row)
                continue
                
            for cr in creatives:
                cr_name = cr.get("cr_name", "")
                cr_id = cr.get("cr_id", "")
                cr_status = _map_status(cr.get("ad_status") or cr.get("cr_status"))
                cr_iab = cr.get("iab", "")
                cr_title = cr.get("cr_title", "")
                cr_desc = cr.get("cr_desc", "")
                cr_btn = cr.get("cr_btn_text", "")
                cr_mt = cr.get("cr_mt_id") or cr.get("cr_mt") or ""
                
                cr_cols = [
                    cr_name, cr_id, cr_status,
                    cr_iab, cr_title, cr_desc, cr_btn, cr_mt
                ]
                
                c_cols = c_cols_data
                g_cols = g_cols_data 
                    
                row = c_cols + g_cols + cr_cols
                rows.append(row)

    # 3. Create DataFrame
    # group_status inserted at index 11
    # creative_status inserted at index 43
    columns = [
        "廣告活動名稱", "廣告活動ID", "廣告活動狀態",
        "每日預算(NT$)", "主網域名稱", "產品類型", "APP名稱", "操作系統", "品牌名稱",
        "廣告群組名稱", "廣告群組ID", "廣告群組狀態", "網站推廣連結", "第三方點擊追蹤連結(Grouped)",
        "第三方曝光追蹤連結(Grouped)",
        "行銷目標", "計費模式", "固定出價", "每日預算", "深度轉換目標", "轉換價值", "轉化目標", "開始日期", "結束日期",
        "投放星期數", "投放時間段", "地理位置", "國家", "設備類型", "流量類型", "受眾操作系統", "最高系統版本", "瀏覽器",
        "年齡", "性別", "投放興趣選項", "投放興趣受眾", "自定義受眾（包含）", "自定義受眾（不包含）",
        "AI語意擴充選項", "AI語意擴充關鍵字", 
        "廣告文案名稱", "廣告文案ID", "廣告文案狀態",
        "廣告類型", "廣告標題", "廣告內文", "Call to Action", "廣告素材ID"
    ]
    
    df = pd.DataFrame(rows, columns=columns)
    
    # 4. Write to Bytes using OpenPyXL directly for advanced features
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        
        # --- Styles ---
        from openpyxl.styles import PatternFill, Border, Side, Font, Alignment
        
        # Fonts
        font_header = Font(bold=True, size=12)
        font_data = Font(size=12)
        
        # Colors (Light Pastel tones)
        # Campaign: Light Blue
        color_cpg = PatternFill(start_color="DDEBF7", end_color="DDEBF7", fill_type="solid")
        # Ad Group: Light Green
        color_grp = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid")
        # Creative: Light Yellow/Orange
        color_crt = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")

        # Border Styles
        # Data Gridline: Light Grey (Simulating default gridlines)
        grid_border = Border(
            left=Side(style='thin', color='D9D9D9'), 
            right=Side(style='thin', color='D9D9D9'), 
            top=Side(style='thin', color='D9D9D9'), 
            bottom=Side(style='thin', color='D9D9D9')
        )
        
        # Adjust Header Row Height (1.5x approx default 20 -> 30)
        worksheet.row_dimensions[1].height = 30
        
        # Alignment for Header
        align_center = Alignment(vertical='center', horizontal='center')

        # Apply Header Colors and Font
        # Helper to apply fill and font to header range
        def _style_header(start_col_idx, end_col_idx, fill):
            # openpyxl is 1-based indexing for rows/cols
            for c_idx in range(start_col_idx, end_col_idx + 1):
                cell = worksheet.cell(row=1, column=c_idx)
                cell.fill = fill
                cell.font = font_header
                # cell.border = header_border  # Removed per request
                cell.border = grid_border # Use same light grey border as data
                cell.alignment = align_center

        _style_header(1, 9, color_cpg)   # Campaign
        _style_header(10, 41, color_grp) # Ad Group
        _style_header(42, 49, color_crt) # Creative
        
        
        # Apply Font, Gridlines AND Background Colors to Data Range
        # Iterate over all rows from 2 to max_row
        
        # Determine strict data range
        max_r = len(df) + 1 # +1 for header
        max_c = len(columns)
        
        for r_idx in range(2, max_r + 1):
            for c_idx in range(1, max_c + 1):
                cell = worksheet.cell(row=r_idx, column=c_idx)
                cell.font = font_data
                # Apply light grey gridline
                cell.border = grid_border
                
                # Apply Background Color matching Header
                if 1 <= c_idx <= 9:
                    cell.fill = color_cpg
                elif 10 <= c_idx <= 41:
                    cell.fill = color_grp
                elif 42 <= c_idx <= 49:
                    cell.fill = color_crt
                
        # --- 2. Hide Default Gridlines (Requests 2 & 3) ---
        # "完全沒資料的row/col 把預設的框線也消除，全白的那種"
        worksheet.sheet_view.showGridLines = False

        # --- 1. Auto-fit Column Width (Request 1) ---
        # Heuristic: Iterate rows to find max length per column.
        # Since we have sparse data (blanks), we check valid cells.
        # openpyxl requires setting 'worksheet.column_dimensions[letter].width'
        
        # Initialize max_lengths with header lengths
        # columns is list of strings
        # headers are in row 1
        from openpyxl.utils import get_column_letter
        
        # Specific columns to widen (2x)
        double_width_cols = {
            "產品類型", "操作系統", "行銷目標", "計費模式", 
            "受眾操作系統", "最高系統版本", "深度轉換目標", "轉換價值", "轉化目標"
        }

        # We process header first
        column_widths = {}
        for i, col_name in enumerate(columns):
            # 1.5 factor for bold font and some padding
            width = len(str(col_name)) * 1.5 + 2
            
            # Check if this column needs doubling (based on name)
            # Handle duplicate names logic: The name in 'columns' list is what we check.
            if col_name in double_width_cols:
                # User asked for "column width add 2 times" (width * 2 presumably, or increase significantly)
                # I will store a multiplier.
                pass # Applied below
                
            column_widths[i+1] = width

        # Process data rows
        # Since dataframe can be large, iterating again might be slow but OK for this scale (~few k rows).
        # We can iterate the dataframe 'rows' list we constructed earlier 'rows' variable, which matches Excel except sparse logic.
        # Actually 'rows' variable in the loops above handles the sparse logic logic?
        # Yes, 'rows' list contains the actual data to be written.
        # Iterate 'rows' (which are lists of values)
        for r in rows:
            for i, val in enumerate(r):
                if val:
                    val_len = len(str(val))
                    # Adjust factor for chinese characters (width ~2) vs english (width ~1)
                    # Simple heuristic: len * 1.3 + padding
                    # Or count bytes?
                    # Let's use simple length * 1.8 for safety to fit font 12
                    curr_w = val_len * 1.8 
                    if curr_w > column_widths[i+1]:
                        column_widths[i+1] = curr_w
        
        # Set widths
        for col_idx, width in column_widths.items():
            # Apply doubling for specific columns
            # Get column name from our list (0-based)
            if col_idx <= len(columns):
                col_name = columns[col_idx-1]
                if col_name in double_width_cols:
                    width = width * 2
            
            # Cap width to avoid overly wide columns (e.g. long URL)
            final_width = min(width, 100) # Increased cap for doubled columns
            col_letter = get_column_letter(col_idx)
            worksheet.column_dimensions[col_letter].width = final_width
                    
        # --- Data Validation ---
        # Max row for validation
        dv_max = max_r + 100
        
        # Helper to add DV
        def _add_dv(col_letter, options_list, prompt_title=""):
            # Ensure options are strings and comma separated within double quotes
            quoted_opts = [f'{o}' for o in options_list]
            formula = f'"{",".join(quoted_opts)}"'
            
            dv = DataValidation(type="list", formula1=formula, allow_blank=True)
            dv.error = '請從下拉選單中選擇有效的值'
            dv.errorTitle = '輸入錯誤'
            dv.prompt = '請從選單中選擇'
            dv.promptTitle = prompt_title
            
            worksheet.add_data_validation(dv)
            dv.add(f'{col_letter}2:{col_letter}{dv_max}')

        # 1. Status Columns (C, L, AS)
        _add_dv("C", ["開啟", "關閉"], "廣告活動狀態")
        _add_dv("L", ["開啟", "關閉"], "廣告群組狀態")
        
        # Creative Status: 
        # Index 44 -> AR (0-based 44 -> 45th col -> 45-26=19 -> S)
        # Wait, let's re-verify Col 44.
        # A=1... Z=26. AA=27.
        # Col 1 = A.
        # Col 45. 45-26 = 19. 19th char is S. (A=1... S=19).
        # So AR is 18?
        # R is 18th letter. AR is 26+18 = 44.
        # So AR is column 44.
        # My Columns list has "廣告文案狀態" at index 44.
        # Python list index 44 is the 45th element.
        # So it is Column 45.
        # Column 45 is AS. (26+19=45). 
        # Previous logic: 40 -> AN. (26+14=40). 39 index is 40th col -> AN. Correct.
        # 44 index is 45th col -> AS. 
        # So Creative Status WAS "AS". My previous code had "AS" then I changed to "AR"?
        # Let's check listing again.
        # ... AP(41), AQ(42), AR(43), AS(44)?
        # Index 39=AN.
        # Index 40=AO.
        # Index 41=AP. (廣告文案名稱)
        # Index 42=AQ. (廣告文案ID)
        # Index 43=AR. (廣告文案狀態) -> THIS IS IT.
        # Wait, check columns list provided in code:
        # ... "AI語意擴充關鍵字"(40)
        # "廣告文案名稱"(41)
        # "廣告文案ID"(42)
        # "廣告文案狀態"(43)
        # So "廣告文案狀態" is index 43.
        # Index 43 is the 44th column.
        # 44th column -> 44-26 = 18 -> R. 
        # So AR is indeed correct for Index 43.
        # My apologies, I need to be super precise.
        
        # Col Index 43 (0-based) -> Excel Col 44 -> AR.
        _add_dv("AR", ["開啟", "關閉"], "廣告文案狀態")

        # 2. 產品類型 (Col 5 -> F)
        _add_dv("F", ["web", "app"], "產品類型")

        # 3. 操作系統 (APP) (Col 7 -> H)
        _add_dv("H", ["iOS", "Android", "Others"], "操作系統")
        
        # 4. 行銷目標 (Col 15 -> P) (Index 15 -> 16th col -> P)
        _add_dv("P", ["品牌知名度", "電商網上購買", "增加網站流量", "開發潛在客戶", "網站互動"], "行銷目標")

        # 5. 計費模式 (Col 16 -> Q) (Index 16 -> 17th col -> Q)
        _add_dv("Q", ["CPM", "CPC"], "計費模式")

        # 6. 深度轉換目標 (Col 19 -> T) (Index 19 -> 20th col -> T)
        _add_dv("T", ["帳戶預設設定", "所有轉換", "指定轉換目標"], "深度轉換目標")

        # 6.5 轉化目標 (Col 21 -> V)
        _add_dv("V", ["點擊數", "網頁瀏覽", "完成註冊", "搜尋", "收藏", "加入購物車", "開始結帳", "完成結帳"], "轉化目標")

        # 7. 地理位置 (Col 26 -> AA) (Index 26 -> 27th col -> AA)
        _add_dv("AA", ["包含", "不包含"], "地理位置")
        
        # 8. 受眾操作系統 (Col 30 -> AE)
        _add_dv("AE", ["iOS", "Android", "Others"], "受眾操作系統")
        
        # 9. 投放興趣選項 (Col 35 -> AJ) (Index 35 -> 36th col -> AJ)
        _add_dv("AJ", ["包含", "不包含"], "投放興趣選項")
        
        # 10. AI語意擴充選項 (Col 39 -> AN) (Index 39 -> 40th col -> AN)
        _add_dv("AN", ["1", "2"], "AI語意擴充選項")
        
    return output.getvalue()
