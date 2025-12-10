from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, List, Optional
from datetime import datetime


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

    # 若 Excel 讀入是 timestamp / datetime 物件
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.strftime("%Y-%m-%d %H")

    s = value.strip()

    # 正規 YYYY-MM-DD HH
    try:
        parsed = datetime.strptime(s, "%Y-%m-%d %H")
        return parsed.strftime("%Y-%m-%d %H")
    except Exception:
        pass

    # 若使用者輸入 yyyy/mm/dd hh, yyyy.mm.dd hh 也自動修正
    for sep in ["/", "."]:
        if sep in s:
            s_fixed = s.replace(sep, "-")
            try:
                parsed = datetime.strptime(s_fixed, "%Y-%m-%d %H")
                return parsed.strftime("%Y-%m-%d %H")
            except Exception:
                break

    # 都不符合 → 拋錯
    raise UploadParsingError(
        f"Row {excel_row_num}: 欄位「{field}」格式錯誤，必須為 yyyy-mm-dd hh，例如：2025-12-10 08"
    )

def excel_to_campaign_json(df: pd.DataFrame) -> Dict[str, object]:
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

    # Identify the two 操作系統 columns if they exist
    all_cols = list(df.columns)
    os_cols = [c for c in all_cols if str(c).startswith("操作系統")]
    app_os_col: Optional[str] = None
    target_os_col: Optional[str] = None
    if os_cols:
        app_os_col = os_cols[0]
        if len(os_cols) > 1:
            target_os_col = os_cols[1]

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
                    campaign["cpg_id"] = int(cpg_id_val)
                except Exception:
                    campaign["cpg_id"] = cpg_id_val

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
            group_id = _get_optional_str(row, "廣告群組ID")
            if group_id:
                try:
                    group["group_id"] = int(group_id)
                except Exception:
                    group["group_id"] = group_id
            ad_groups.append(group)

        #
        # URL / tracking
        #
        # 網站推廣連結 → target_info (主要推廣頁面)
        target_info_url = _get_optional_str(row, "網站推廣連結")
        if target_info_url:
            group["target_info"] = target_info_url

        # 第三方點擊追蹤連結(Grouped) → click_url
        click_urls = _split_list(row.get("第三方點擊追蹤連結(Grouped)"))
        if click_urls:
            group["click_url"] = click_urls

        # 第三方曝光追蹤連結(Grouped) → impression_url
        imp_urls = _split_list(row.get("第三方曝光追蹤連結(Grouped)"))
        if imp_urls:
            # type is not defined in sheet; default to 1
            group["impression_url"] = [{"type": 1, "value": u} for u in imp_urls]

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
            budget["rev_type"] = billing_type

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

        if conv_value is not None:
            conversion_goal["target_value"] = conv_value
        else:
            conversion_goal["target_value"] = 0
        if conv_goal is not None:
            conv_goal_map = {
            "點擊數": 11,
            "網頁瀏覽": 13,
            "完成註冊": 6,
            "搜尋": 5,
            "收藏": 3,
            "加入購物車": 4,
            "開始結帳": 2,
            "完成結帳": 1
            }
            cg = conv_goal_map.get(conv_goal)
            if cg is None:
                raise UploadParsingError(
                    f"Row {excel_row_num}: 不支援的轉換目標「{conv_goal}」，請確認是否拼寫正確。"
                )
            conversion_goal["convert_event"] = cg
        else:
            conversion_goal["convert_event"] = 0

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
            start_date = _validate_datetime_format(str(start_date_raw), excel_row_num, "開始日期")
            schedule["start_date"] = start_date

        if end_date_raw:
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
            schedule["hours"] = hours

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
            audience["device_type"] = device_types

        # 流量類型 → traffic_type
        traffic_types = _split_list(row.get("流量類型"))
        if traffic_types:
            audience["traffic_type"] = traffic_types

        # 受眾層級 OS / 平台: 操作系統(第二欄) → platform
        target_os_raw = (
            _get_optional_str(row, target_os_col) if target_os_col else None
        )
        if target_os_raw:
            audience["platform"] = _split_list(target_os_raw)

        # 最高系統版本 → os_version
        max_os_ver_raw = row.get("最高系統版本")
        if not pd.isna(max_os_ver_raw):
            try:
                max_ver = float(str(max_os_ver_raw).strip())
                audience["os_version"] = max_ver
            except Exception:
                # ignore parse error
                pass

        # 瀏覽器 → browser
        browsers = _split_list(row.get("瀏覽器"))
        if browsers:
            audience["browser"] = browsers

        # 年齡 → age
        ages = _split_list(row.get("年齡"))
        if ages:
            audience["age"] = ages

        # 性別 → gender
        genders = _split_list(row.get("性別"))
        if genders:
            audience["gender"] = genders

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
        
        # Add include audiences (type: 1)
        for id_str in pixel_audience_include:
            try:
                pixel_audience_list.append({"id": int(id_str), "type": 1})
            except (ValueError, TypeError):
                # Skip invalid IDs
                continue
        
        # Add exclude audiences (type: 2)
        for id_str in pixel_audience_exclude:
            try:
                pixel_audience_list.append({"id": int(id_str), "type": 2})
            except (ValueError, TypeError):
                # Skip invalid IDs
                continue
        
        if pixel_audience_list:
            audience["pixel_audience"] = pixel_audience_list

        #
        # Ad assets (creatives) per ad group
        #
        ad_assets: List[Dict[str, Any]] = group.setdefault("ad_asset", [])

        cr_name = _get_optional_str(row, "廣告文案名稱")
        cr_title = _get_optional_str(row, "廣告標題")
        cr_desc = _get_optional_str(row, "廣告內文")
        cr_btn = _get_optional_str(row, "Call to Action")
        cr_iab = _get_optional_str(row, "廣告類型")  # per mapping request

        cr_mt_raw = row.get("廣告素材ID")
        cr_mt_id: Optional[int | str] = None
        if cr_mt_raw is not None and not pd.isna(cr_mt_raw):
            try:
                cr_mt_id = int(str(cr_mt_raw).strip())
            except Exception:
                cr_mt_id = str(cr_mt_raw).strip()

        # Add asset only if at least one meaningful field is present
        if any([cr_name, cr_title, cr_desc, cr_btn, cr_iab, cr_mt_id]):
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

