from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Index, UniqueConstraint
from datetime import datetime

db = SQLAlchemy()

class BHAccount(db.Model):
    __tablename__ = 'bh_accounts'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    platform = db.Column(db.Enum('R', 'D'), nullable=False, comment='廣告平台: R/D')
    agent = db.Column(db.Integer, nullable=True, comment='R平台代理商(7168=4A, 7161=台客)') 
    account_id = db.Column(db.String(50), nullable=False, comment='平台帳戶ID')
    account_name = db.Column(db.String(255), nullable=False, comment='帳戶名稱')
    budget = db.Column(db.Numeric(15, 2), nullable=False, default=0.00, comment='總預算')
    start_date = db.Column(db.Date, nullable=False, comment='走期開始日')
    end_date = db.Column(db.Date, nullable=False, comment='走期結束日')
    cpc_goal = db.Column(db.Numeric(10, 2), nullable=True, comment='目標CPC')
    cpa_goal = db.Column(db.Numeric(10, 2), nullable=True, comment='目標CPA')
    ctr_goal = db.Column(db.Numeric(10, 4), nullable=True, comment='目標CTR')
    cv_definition = db.Column(db.Text, nullable=True, comment='R平台的轉換定義')
    owner_email = db.Column(db.String(255), nullable=False, comment='負責人Email')
    status = db.Column(db.Enum('active', 'archived'), nullable=False, default='active', comment='狀態')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_account_id', 'account_id'),
        Index('idx_owner_email', 'owner_email'),
        Index('idx_status', 'status'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'platform': self.platform,
            'agent': self.agent,
            'account_id': self.account_id,
            'account_name': self.account_name,
            'budget': float(self.budget) if self.budget else 0.0,
            'start_date': self.start_date.isoformat() if self.start_date else None,
            'end_date': self.end_date.isoformat() if self.end_date else None,
            'cpc_goal': float(self.cpc_goal) if self.cpc_goal else None,
            'cpa_goal': float(self.cpa_goal) if self.cpa_goal else None,
            'ctr_goal': float(self.ctr_goal) if self.ctr_goal else None,
            'cv_definition': self.cv_definition,
            'owner_email': self.owner_email,
            'status': self.status,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

class BHDailyStats(db.Model):
    __tablename__ = 'bh_daily_stats'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    account_id = db.Column(db.String(50), nullable=False, comment='平台帳戶ID')
    date = db.Column(db.Date, nullable=False, comment='數據日期')
    spend = db.Column(db.Numeric(15, 2), nullable=False, default=0.00, comment='當日花費')
    impressions = db.Column(db.Integer, nullable=False, default=0, comment='曝光數')
    clicks = db.Column(db.Integer, nullable=False, default=0, comment='點擊數')
    conversions = db.Column(db.Integer, nullable=False, default=0, comment='總轉換數')
    raw_data = db.Column(db.JSON, nullable=True, comment='API原始回應資料')
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('account_id', 'date', name='uniq_acc_date'),
        Index('idx_date', 'date'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'account_id': self.account_id,
            'date': self.date.isoformat() if self.date else None,
            'spend': float(self.spend) if self.spend else 0.0,
            'impressions': self.impressions,
            'clicks': self.clicks,
            'conversions': self.conversions,
            # raw_data usually skipped for list views to save bandwidth
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }


class BHDAccountToken(db.Model):
    __tablename__ = 'bh_d_account_token'

    account_id = db.Column(db.String(50), primary_key=True)
    account_name = db.Column(db.String(100))
    token = db.Column(db.Text)  # Token can be long
    created_time = db.Column(db.DateTime, default=datetime.utcnow)
    updated_time = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

def init_db(app):
    """Initializes the database context."""
    db.init_app(app)
    with app.app_context():
        # Create tables if they don't exist
        db.create_all()
