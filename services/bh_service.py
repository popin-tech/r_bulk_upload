import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import or_, func, case
from sqlalchemy.exc import IntegrityError
from database import db, BHAccount, BHDailyStats, BHDAccountToken
import logging

logger = logging.getLogger(__name__)

class BHService:
    def process_excel_upload(self, file_stream, owner_email: str) -> dict:
        """
        Parse Excel file and import accounts into bh_accounts.
        Format: Platform, AccID, Budget, StartDate, EndDate, CPCGoal, CPAGoal, CVDefinition, Token(Optional for D)
        """
        try:
            df = pd.read_excel(file_stream)
        except Exception as e:
            raise ValueError(f"Failed to read Excel file: {e}")

        # Normalize column names
        # User's example: 平台, AccID, Budget, StartDate, EndDate, CPCGoal, CPAGoal, R的cv定義
        # Map them to model fields
        col_map = {
            '平台': 'platform',
            'AccID': 'account_id',
            'Budget': 'budget',
            'StartDate': 'start_date',
            'EndDate': 'end_date',
            'CPCGoal': 'cpc_goal',
            'CPAGoal': 'cpa_goal',
            'R的cv定義': 'cv_definition'
            # 'Token': 'token' # Optional
        }
        
        # Check required columns (at least Platform, AccID, Budget, Start/End)
        required = ['平台', 'AccID', 'Budget', 'StartDate', 'EndDate']
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {', '.join(missing)}")

        results = {
            'total': 0,
            'inserted': 0,
            'errors': []
        }

        for index, row in df.iterrows():
            results['total'] += 1
            try:
                platform = str(row.get('平台', '')).strip().upper()
                acc_id = str(row.get('AccID', '')).strip()
                
                if platform not in ['R', 'D']:
                    results['errors'].append(f"Row {index+2}: Invalid Platform '{platform}'")
                    continue
                if not acc_id:
                    results['errors'].append(f"Row {index+2}: Missing Account ID")
                    continue

                # Parse Dates
                try:
                    s_date = pd.to_datetime(row['StartDate']).date()
                    e_date = pd.to_datetime(row['EndDate']).date()
                except Exception:
                    results['errors'].append(f"Row {index+2}: Invalid Date Format")
                    continue

                # Parse Goals
                try:
                    budget = float(row.get('Budget', 0))
                    cpc = float(row.get('CPCGoal', 0)) if pd.notna(row.get('CPCGoal')) else None
                    cpa = float(row.get('CPAGoal', 0)) if pd.notna(row.get('CPAGoal')) else None
                except ValueError:
                    results['errors'].append(f"Row {index+2}: Invalid Number Format")
                    continue

                cv_def = str(row.get('R的cv定義', '')) if pd.notna(row.get('R的cv定義')) else None

                # Create New Account (Always insert new tailored to user request: "Same ID not overwrite, treat as new entry")
                # Wait, if same ID same dates? User said "multple periods treat as multiple entries".
                # But we have Auto-Increment ID as PK. So it's fine.
                
                # Parse R Agent if Platform is R
                r_agent_id = None
                if platform == 'R':
                    agent_str = str(row.get('R代理', '')).strip()
                    if agent_str == '4A':
                        r_agent_id = 7168
                    elif agent_str == '台客':
                        r_agent_id = 7161
                
                account = BHAccount(
                    platform=platform,
                    account_id=acc_id,
                    account_name=f"{platform}_{acc_id}", # Placeholder name? Or fetch from API? Or user provided?
                    # User excel didn't have "Account Name" in the example columns!
                    # "上傳資料如下: 平台 AccID Budget ..."
                    # But in "平台顯示與數據規則": "帳戶清單欄位... 系統(r/d)，帳戶id，帳戶名..."
                    # Maybe the Excel *should* have Name? 
                    # Or we fetch it from API?
                    # implementation_plan says: "Table: bh_accounts ... account_name"
                    # Let's assume for now we use ID as Name or empty string, and update via sync later?
                    # Or better: check if user provided "Name" column in screenshot?
                    # Screenshot shows "名稱" column in UI.
                    # Excel sample text didn't list it.
                    # I will add optional 'Name' column support, default to ID.
                    budget=budget,
                    start_date=s_date,
                    end_date=e_date,
                    cpc_goal=cpc,
                    cpa_goal=cpa,
                    cv_definition=cv_def,
                    owner_email=owner_email,
                    status='active',
                    agent=r_agent_id
                )
                
                # If 'AccountName' or '名稱' exists
                if 'Name' in row:
                    account.account_name = str(row['Name'])
                elif '名稱' in row:
                    account.account_name = str(row['名稱'])
                elif 'AccountName' in row:
                    account.account_name = str(row['AccountName'])
                else:
                    # Logic 2) For BHDAccountToken, user request says "account_name" field exists.
                    # If user uploads Token, they likely upload Name too? 
                    pass
                
                db.session.add(account)
                
                # --- D Platform Token Logic ---
                if platform == 'D':
                    token_val = None
                    # Prioritize 'D Token', then fallback to 'Token' / 'token'
                    if 'D Token' in row and pd.notna(row['D Token']):
                         token_val = str(row['D Token']).strip()
                    elif 'Token' in row and pd.notna(row['Token']):
                        token_val = str(row['Token']).strip()
                    elif 'token' in row and pd.notna(row['token']):
                        token_val = str(row['token']).strip()
                        
                    if token_val:
                        # Check if exists (User Rule: "有這個帳戶的token 就不動作，沒有就insert" -> User ORIGINALLY said this??)
                        # Wait, User said "欄位 d token 目前有正常更新資料庫嗎？幫我確認"
                        # This implies they WANT it to update.
                        # My previous implementation was "insert only" based on some assumption or previous prompt.
                        # I will change it to UPSERT (Update if exists).
                        
                        existing_token = BHDAccountToken.query.filter_by(account_id=acc_id).first()
                        if existing_token:
                            # Update if changed
                            if existing_token.token != token_val:
                                existing_token.token = token_val
                                existing_token.updated_at = datetime.utcnow()
                                print(f"[BHService] Updated Token for D-Account {acc_id}")
                        else:
                            # Insert
                            new_token = BHDAccountToken(
                                account_id=acc_id,
                                account_name=account.account_name, # Use the name from account row
                                token=token_val
                            )
                            db.session.add(new_token)
                            print(f"[BHService] Inserted new Token for D-Account {acc_id}")
                # ------------------------------

                results['inserted'] += 1

            except Exception as e:
                results['errors'].append(f"Row {index+2}: {str(e)}")

        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            raise ValueError(f"Database Commit Failed: {e}")

        return results

    def get_accounts(self, owner_email: str = None, search_term: str = None) -> list[dict]:
        """
        Fetch accounts with optional filters.
        Returns list of dicts with calculation progress.
        """
        query = BHAccount.query.filter_by(status='active')
        
        if owner_email:
            query = query.filter(BHAccount.owner_email == owner_email)
        
        if search_term:
            # Simple fuzzy search
            term = f"%{search_term}%"
            query = query.filter(or_(
                BHAccount.account_name.ilike(term),
                BHAccount.account_id.ilike(term)
            ))
            
        # Debug ownership
        # print(f"DEBUG get_accounts: owner={owner_email}, count={query.count()}")
        
        accounts = query.order_by(BHAccount.created_at.desc()).all()
        
        # Calculate stats for each account
        # We need "Yesterday Spend", "Total Spend", "Days Remaining"
        # Fetch stats from BHDailyStats
        
        # Optimization: Fetch all stats for these accounts in one query?
        # Or just sum in DB?
        # Let's use a separate aggregation query for performance.
        
        acct_ids = [a.account_id for a in accounts] # Note: account_id is not PK.
        # bh_daily_stats links by account_id (String).
        
        if not acct_ids:
            return []

        # Aggregate Stats
        # sum(spend) as total_spend, sum(cv) as total_cv
        # also we need "yesterday spend". 
        
        # 1. Total Stats (Filtered by Date Range)
        # Fix: Only sum stats that are within the Account's Start/End Date
        # Group by BHAccount.id (Unique PK) instead of account_id string to Separate overlapping/same-id accounts
        account_primary_ids = [a.id for a in accounts]
        
        total_stats = db.session.query(
            BHAccount.id,
            func.sum(BHDailyStats.spend).label('total_spend'),
            func.sum(BHDailyStats.conversions).label('total_cv'),
            func.sum(BHDailyStats.clicks).label('total_clicks')
        ).join(
            BHAccount, BHAccount.account_id == BHDailyStats.account_id
        ).filter(
            BHAccount.id.in_(account_primary_ids),
            BHDailyStats.date >= BHAccount.start_date,
            BHDailyStats.date <= BHAccount.end_date
        ).group_by(BHAccount.id).all()
        
        stats_map = {r.id: {'spend': float(r.total_spend or 0), 'cv': int(r.total_cv or 0), 'clicks': int(r.total_clicks or 0)} for r in total_stats}
        
        # 2. Yesterday Stats
        yesterday = (datetime.utcnow() - timedelta(days=1)).date() 
        
        y_stats = db.session.query(
            BHDailyStats.account_id,
            BHDailyStats.spend
        ).filter(
            BHDailyStats.account_id.in_(acct_ids),
            BHDailyStats.date == yesterday
        ).all()
        
        y_map = {r.account_id: float(r.spend or 0) for r in y_stats}

        # 3. Fetch D-Tokens (Optimized)
        # Only for D accounts? Or all? fetching all is easier if list is small.
        # Filter where account_id in acct_ids
        tokens = BHDAccountToken.query.filter(
            BHDAccountToken.account_id.in_(acct_ids)
        ).all()
        token_map = {t.account_id: t.token for t in tokens}

        results = []
        today = datetime.utcnow().date() 

        for acc in accounts:
            data = acc.to_dict()
            
            # Match via PK ID
            s = stats_map.get(acc.id, {'spend': 0, 'cv': 0, 'clicks': 0})
            
            # Attach Token if D platform (or always, frontend can filter)
            if acc.platform == 'D':
                data['d_token'] = token_map.get(acc.account_id)
            
            # Yesterday Spend: Only valid if yesterday covers this account's period
            # Or at least check reasonable bounds if strictly required. 
            # For now, if same ID, physical spend is same. 
            # But if period is Jan and yesterday is Feb, Jan account shouldn't show yesterday spend.
            y_spend_val = y_map.get(acc.account_id, 0)
            if acc.start_date <= yesterday <= acc.end_date:
                data['yesterday_spend'] = y_spend_val
            else:
                data['yesterday_spend'] = 0
            
            data['total_spend'] = s['spend']
            data['total_cv'] = s['cv']
            data['total_clicks'] = s['clicks']
            
            # Calculations
            # 1. Budget Progress
            # "每日應花": (Budget - TotalSpend) / RemainingDays ? 
            # Or (Budget / TotalDays)? 
            # User said: "完整進度是每日應花，裡面的進度是昨日花費" -> Implies "Target Daily Spend" vs "Actual Yesterday Spend".
            # Usually: Daily Target = Total Budget / Total Days (Linear Pacing)
            # Or Remaining Budget / Remaining Days (Adaptive Pacing)
            # Let's simple Linear for "Goal":
            
            total_days = (acc.end_date - acc.start_date).days + 1
            if total_days < 1: total_days = 1
            
            # Dynamic Daily Budget Logic
            # User request: (Total Budget - Spend) / Remaining Days
            # remaining_days calculation needs to be before this or calculated here
            
            # Calculate Remaining Days (Inclusive of Today?) 
            # User example: 2/11~2/28 is 18 days. 28-11+1=18. So inclusive of today.
            if today < acc.start_date:
                rem_days = total_days
            elif today > acc.end_date:
                rem_days = 0
            else:
                rem_days = (acc.end_date - today).days + 1
            
            remaining_budget = float(acc.budget) - s['spend']
            
            if rem_days > 0:
                # If over budget, show negative to indicate issue as per user request
                daily_budget = remaining_budget / rem_days
            else:
                daily_budget = 0
                
            data['daily_budget'] = daily_budget
            
            # Days Remaining
            # today -> end_date
            # Assuming 'today' is inclusive or exclusive?
            # If today < start_date: all days remaining.
            # If today > end_date: 0
            
            if today < acc.start_date:
                data['remaining_days'] = total_days
            elif today > acc.end_date:
                data['remaining_days'] = 0
            else:
                data['remaining_days'] = (acc.end_date - today).days + 1
            
            # CPC / CPA
            # Current CPA = Total Spend / Total CV
            # Target CPA = acc.cpa_goal
            if s['cv'] > 0:
                data['current_cpa'] = s['spend'] / s['cv']
            else:
                data['current_cpa'] = 0
                
            if s['clicks'] > 0:
                data['current_cpc'] = s['spend'] / s['clicks']
            else:
                data['current_cpc'] = 0
            
            # Progress Color Logic (Dashboard requirement)
            # Percentage = Total Spend / Total Budget
            # Or Pacing %? (Actual / Expected at this point)
            # User requirement: "總花費，預算也是可以放一起看"
            # Color logic: < 80% Red, 80-90 Yellow, 90-100 Green, >100 Red.
            # Wait, this logic implies "We want to be close to 100% of *Budget*" ?
            # Usually for *Budget Pacing*, if we are at Day 50% but Spend 10%, that's "Behind" (Red).
            # If User means "Total Budget Utilization", then 10% is Red?
            # User said: "progress bar color... < 80% Red".
            # This makes sense if we compare "Actual Spend" to "Expected Spend (based on time)".
            # Pacing % = (Total Spend / Expected Spend to Date) * 100
            # Expected Spend to Date = Daily Budget * Days Passed.
            
            days_passed = (today - acc.start_date).days
            if days_passed < 0: days_passed = 0
            if days_passed > total_days: days_passed = total_days
            
            expected_spend = daily_budget * days_passed
            if expected_spend <= 0:
                pacing = 0 # Just started
            else:
                pacing = (s['spend'] / expected_spend) * 100
            
            data['pacing_percent'] = pacing
            
            # Overall Utilization (Total Spend / Total Budget)
            if float(acc.budget) > 0:
                data['budget_percent'] = (s['spend'] / float(acc.budget)) * 100
            else:
                data['budget_percent'] = 0

            results.append(data)

        return results

    def export_accounts_excel(self, owner_email: str = None, search_term: str = None) -> bytes:
        """
        Export filtered account list to Excel bytes.
        """
        accounts_data = self.get_accounts(owner_email, search_term)
        
        export_list = []
        for d in accounts_data:
            row = {
                '系統': d.get('platform'),
                'Account ID': d.get('account_id'),
                '名稱': d.get('account_name'),
                '總預算': d.get('budget'),
                '每日應花': d.get('daily_budget'),
                '昨日花費': d.get('yesterday_spend'),
                '總花費': d.get('total_spend'),
                '剩餘天數': d.get('remaining_days'),
                'CPC目標': d.get('cpc_goal'),
                '目前CPC': d.get('current_cpc'),
                'CPA目標': d.get('cpa_goal'),
                '目前CPA': d.get('current_cpa'),
                '走期開始': d.get('start_date'),
                '走期結束': d.get('end_date'),
                'CV定義': d.get('cv_definition')
            }
            export_list.append(row)
            
        df = pd.DataFrame(export_list)
        
        from io import BytesIO
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Accounts')
        
        return output.getvalue()

    def update_account(self, account_id: str, data: dict, user_email: str) -> bool:
        """
        Update account details.
        """
        acc = BHAccount.query.filter_by(account_id=account_id).first()
        if not acc:
            raise ValueError(f"Account {account_id} not found")
        
        # Update fields
        if 'budget' in data: acc.budget = data['budget']
        if 'start_date' in data: 
            try:
                if isinstance(data['start_date'], str):
                     acc.start_date = datetime.strptime(data['start_date'], '%Y-%m-%d').date()
            except: pass
        if 'end_date' in data:
            try:
                if isinstance(data['end_date'], str):
                    acc.end_date = datetime.strptime(data['end_date'], '%Y-%m-%d').date()
            except: pass
        if 'cpc_goal' in data: acc.cpc_goal = data['cpc_goal']
        if 'cpa_goal' in data: acc.cpa_goal = data['cpa_goal']
        
        # Update Agent
        if 'agent' in data:
            try:
                # Agent can be null or int
                val = data['agent']
                if val:
                    acc.agent = int(val)
                else:
                    acc.agent = None
            except: pass

        # Update D Token
        if 'd_token' in data and acc.platform == 'D':
            token_val = str(data['d_token']).strip()
            # Upsert BHDAccountToken
            d_token = BHDAccountToken.query.filter_by(account_id=account_id).first()
            if d_token:
                d_token.token = token_val
                d_token.updated_time = datetime.utcnow()
            else:
                new_token = BHDAccountToken(
                    account_id=account_id,
                    account_name=acc.account_name,
                    token=token_val
                )
                db.session.add(new_token)
        
        acc.updated_at = datetime.utcnow()
        db.session.commit()
        return True

    def update_accounts_status(self, account_ids: list[int], status: str) -> int:
        """
        Bulk update account status.
        Returns number of rows updated.
        """
        if not account_ids:
            return 0
            
        # status must be 'active' or 'archived' (or others if defined)
        if status not in ['active', 'archived']:
            raise ValueError(f"Invalid status: {status}")
            
        try:
            # Efficient Bulk Update
            # synchronize_session=False is faster for bulk updates but session objects might be stale 
            # (usually fine for this use case as we reload after)
            updated_count = BHAccount.query.filter(
                BHAccount.id.in_(account_ids)
            ).update({BHAccount.status: status}, synchronize_session=False)
            
            db.session.commit()
            return updated_count
        except Exception as e:
            db.session.rollback()
            raise e

    def get_account_daily_stats(self, account_pk: int) -> list[dict]:
        """
        Get daily stats for an account, filtered by its specific date range.
        """
        # 1. Get the Account to know the Date Range & Account ID
        acc = BHAccount.query.get(account_pk)
        if not acc:
            raise ValueError("Account not found")

        print(f"[DEBUG] Fetching Daily Stats for PK={account_pk}, AccID={acc.account_id}, Range={acc.start_date} ~ {acc.end_date}")

        # 2. Query Stats matching AccountID AND Date Range
        stats = BHDailyStats.query.filter(
            BHDailyStats.account_id == acc.account_id,
            BHDailyStats.date >= acc.start_date,
            BHDailyStats.date <= acc.end_date
        ).order_by(BHDailyStats.date.desc()).all()

        print(f"[DEBUG] Found {len(stats)} items.")
        results = []
        for s in stats:
            results.append({
                'date': s.date.strftime('%Y-%m-%d'),
                'spend': float(s.spend),
                'impressions': s.impressions,
                'clicks': s.clicks,
                'conversions': s.conversions,
                'cpc': float(s.spend) / s.clicks if s.clicks > 0 else 0,
                'cpa': float(s.spend) / s.conversions if s.conversions > 0 else 0
            })
        return results


