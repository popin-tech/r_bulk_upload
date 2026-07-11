-- 2026-07-11-add-mgid.sql
-- BH 新增 MGID(M) 平台：只擴充 platform enum。
-- 註：nexus.mgid_tokens 已由 TS 工具建立並灌帳號，此處不建表。

-- bh_accounts.platform 加入 'M'（db.create_all() 不會 ALTER 既有 enum，必須手動）
ALTER TABLE bh_accounts
  MODIFY COLUMN platform ENUM('R','D','M') NOT NULL COMMENT '廣告平台: R/D/M';
