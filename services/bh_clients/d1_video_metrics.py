# D1 影音報表的口徑（純函式，可離線驗證）。
#
# 公式全部照抄 D1 後台原始碼，不是自己推的：
#   popin-discovery-v2/app/Library/apiUtils.php 的 arrangeStats()
#
# ⚠️⚠️ 只算 mobile，PC 一律不計。arrangeStats 是寫死的
#      （$stats['video_imp'] = $dailyStats['mobile_video_imp']，完全沒碰 pc_video_*），
#      前端 lib6 的 video 也確實只出 mobile。Action4 有回 pc_video_*（實測約佔 1.7% 曝光），
#      但加進來就跟 AM 在 D1 後台看到的數字對不起來。
#
# ⚠️ _over（超投）欄位不計入，同樣是為了對齊後台。
#
# ⚠️ 金額的除數是 1000：Action4 的 charge.mobile_video_imp 存的是「CPM × 曝光」。
#    實測 campaign 6a8dbcf60b53275b884686e5（2026-08-28）：charge 407,664 ÷ 1000
#    = 407.664 元 ÷ 5,662 曝光 × 1000 = CPM 72.00 元整；直式那支 311,112 ÷ 4,321
#    也是 72.00 —— 兩支整數 CPM 反推證明除數正確。
#
# ⚠️ D1 影音沒有轉換追蹤，conversions 恆為 0（UI 顯示 — 的處理見 bh_service/bh.html）。

# 收費曝光欄位（橫式＋直式）
IMP_FIELDS = ('mobile_video_imp', 'mobile_video_vertical_imp')
# 點擊欄位
CLICK_FIELD = 'mobile_video_link'
# 金額欄位（charge 子表，值為 CPM × 曝光，取用要 ÷ 1000）
CHARGE_FIELDS = ('mobile_video_imp', 'mobile_video_vertical_imp')
CHARGE_DIVISOR = 1000


def _num(stats, field):
    """頂層數值欄位取值；缺欄／型別不對一律當 0（Action4 沒量的欄位就是整個不出現）。
    註：bool 要排除——isinstance(True, int) 是 True，不擋會把布林當 1。"""
    v = (stats or {}).get(field)
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else 0


def _charge(stats, field):
    """charge 子表取值。"""
    v = ((stats or {}).get('charge') or {}).get(field)
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else 0


def day_metrics(stats):
    """Action4 單日原始統計 → BH 統一格式 {spend, impressions, clicks, conversions}。"""
    return {
        'spend': sum(_charge(stats, f) for f in CHARGE_FIELDS) / CHARGE_DIVISOR,
        'impressions': int(sum(_num(stats, f) for f in IMP_FIELDS)),
        'clicks': int(_num(stats, CLICK_FIELD)),
        'conversions': 0,
    }
