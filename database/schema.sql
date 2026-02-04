-- 1. 帳戶設定表 (bh_accounts)
-- 紀錄 Excel 上傳的預算與走期設定
CREATE TABLE IF NOT EXISTS `bh_accounts` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `platform` ENUM('R', 'D') NOT NULL COMMENT '廣告平台: R/D',
  `account_id` VARCHAR(50) NOT NULL COMMENT '平台帳戶ID (D: Account ID, R: Account ID)',
  `account_name` VARCHAR(255) NOT NULL COMMENT '帳戶名稱',
  `budget` DECIMAL(15, 2) NOT NULL DEFAULT 0.00 COMMENT '總預算',
  `start_date` DATE NOT NULL COMMENT '走期開始日',
  `end_date` DATE NOT NULL COMMENT '走期結束日',
  `cpc_goal` DECIMAL(10, 2) DEFAULT NULL COMMENT '目標CPC',
  `cpa_goal` DECIMAL(10, 2) DEFAULT NULL COMMENT '目標CPA',
  `cv_definition` TEXT DEFAULT NULL COMMENT 'R平台的轉換定義 (逗號分隔字串，如 "CompleteCheckout,AddToCart")',
  `owner_email` VARCHAR(255) NOT NULL COMMENT '負責人Email (上傳者的Google Email)',
  `status` ENUM('active', 'archived') NOT NULL DEFAULT 'active' COMMENT '狀態',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX `idx_account_id` (`account_id`),
  INDEX `idx_owner_email` (`owner_email`),
  INDEX `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. 每日成效表 (bh_daily_stats)
-- 紀錄從 API 抓取的每日花費與成效 (不依賴 bh_accounts 的 PK，而是跟隨平台 account_id)
-- 確保每個帳戶(account_id) 每天(date) 只有一筆數據
CREATE TABLE IF NOT EXISTS `bh_daily_stats` (
  `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
  `account_id` VARCHAR(50) NOT NULL COMMENT '平台帳戶ID (與 bh_accounts.account_id 對應)',
  `date` DATE NOT NULL COMMENT '數據日期',
  `spend` DECIMAL(15, 2) NOT NULL DEFAULT 0.00 COMMENT '當日花費',
  `impressions` INT NOT NULL DEFAULT 0 COMMENT '曝光數',
  `clicks` INT NOT NULL DEFAULT 0 COMMENT '點擊數',
  `conversions` INT NOT NULL DEFAULT 0 COMMENT '總轉換數 (R依定義加總, D直接取值)',
  `raw_data` JSON DEFAULT NULL COMMENT 'API原始回應資料 (Debug用)',
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY `uniq_acc_date` (`account_id`, `date`),
  INDEX `idx_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
