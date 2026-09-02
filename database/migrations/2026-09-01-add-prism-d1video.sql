-- 2026-09-01-add-prism-d1video.sql
-- BH 新增 P（Prism / PAC platform）與 V（D1 影音 / Action4）兩個平台。
-- 只擴充 platform enum：
--   P 走一把全域 token（PRISM_API_TOKEN 環境變數），不需要 token 表。
--   V 走 Action4（公網免認證）＋ Firestore（D1_FIRESTORE_URI），也不需要 token 表。
-- 註：db.create_all() 不會 ALTER 既有 enum，必須手動執行本檔。
-- 註：DB 放行 != 功能開放。實際能不能建 V 帳戶由 bh_service 的上傳白名單控制。

ALTER TABLE bh_accounts
  MODIFY COLUMN platform ENUM('R','D','M','P','V') NOT NULL COMMENT '廣告平台: R/D/M/P/V';
