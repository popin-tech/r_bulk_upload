-- ==========================================================
-- 1. 建立使用者權限表 (Users)
-- ==========================================================
CREATE TABLE `users` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(100) DEFAULT NULL,
  `email` VARCHAR(255) NOT NULL,
  `role` ENUM('admin', 'ae', 'viewer') NOT NULL DEFAULT 'viewer',
  `is_active` BOOLEAN NOT NULL DEFAULT TRUE COMMENT '是否允許登入系統',
  `access_modules` JSON DEFAULT NULL COMMENT 'JSON Array: ["cmp", "bh", "media"]',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_unique_email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ==========================================================
-- 2. 建立 Budget Hunter AE 多對多關聯表 (Account-AE Mapping)
-- ==========================================================
CREATE TABLE `bh_account_aes` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `bh_account_id` INT NOT NULL COMMENT '對應 bh_accounts.id',
  `ae_email` VARCHAR(255) NOT NULL COMMENT '對應 users.email',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_unique_mapping` (`bh_account_id`, `ae_email`),
  CONSTRAINT `fk_bh_account_link` FOREIGN KEY (`bh_account_id`) REFERENCES `bh_accounts` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_user_ae_link` FOREIGN KEY (`ae_email`) REFERENCES `users` (`email`) ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ==========================================================
-- 3. 匯入初始使用者名冊 (DML)
-- ==========================================================

-- A. 從舊 config/allowed_emails.json 轉換的名單 (預設為 viewer, 全模組權限)
INSERT INTO `users` (`email`, `role`, `access_modules`) VALUES
('fu.leopold@gmail.com', 'viewer', '["cmp", "bh", "media"]'),
('leo@popin.cc', 'viewer', '["cmp", "bh", "media"]'),
('spigflying@gmail.com', 'viewer', '["cmp", "bh", "media"]'),
('benson@popin.cc', 'admin', '["cmp", "bh", "media"]'), -- Benson 設為 Admin
('daniel@popin.cc', 'viewer', '["cmp", "bh", "media"]'),
('linzhongjyun@popin.cc', 'viewer', '["cmp", "bh", "media"]'),
('lulu@popin.cc', 'viewer', '["cmp", "bh", "media"]'),
('kalvin@broadciel.com', 'viewer', '["cmp", "bh", "media"]'),
('cindy@popin.cc', 'viewer', '["cmp", "bh", "media"]'),
('ted@popin.cc', 'viewer', '["cmp", "bh", "media"]'),
('tina@popin.cc', 'viewer', '["cmp", "bh", "media"]'),
('joyce@broadciel.com', 'viewer', '["cmp", "bh", "media"]'),
('crystal@popin.cc', 'viewer', '["cmp", "bh", "media"]'),
('teresa@broadciel.com', 'viewer', '["cmp", "bh", "media"]');

-- B. 新加的 AE 名單 (預設角色為 ae, 全模組權限)
INSERT INTO `users` (`name`, `email`, `role`, `access_modules`) VALUES
('jessica', 'jessica@popin.cc', 'ae', '["cmp", "bh", "media"]'),
('emma', 'emma@broadciel.com', 'ae', '["cmp", "bh", "media"]'),
('zoey', 'zoey@broadciel.com', 'ae', '["cmp", "bh", "media"]'),
('daniel', 'daniel@broadciel.com', 'ae', '["cmp", "bh", "media"]'),
('lisa', 'lisa@broadciel.com', 'ae', '["cmp", "bh", "media"]');
