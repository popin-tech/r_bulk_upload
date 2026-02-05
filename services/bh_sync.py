import time
import json
from datetime import datetime, timedelta
from database import db, BHAccount, BHDailyStats, BHDAccountToken
from services.bh_clients.r_client import RixbeeClient
from services.bh_clients.d_client import DiscoveryClient
import logging

class BHSyncService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def sync_daily_stats(self, target_date: str = None, account_id: str = None):
        """
        Generator function that yields log messages for SSE.
        """
        yield f"data: {json.dumps({'msg': 'Starting Sync Process...'})}\n\n"
        
        try:
            # Determine Date
            if not target_date:
                # Default to Yesterday
                yesterday = datetime.utcnow() + timedelta(hours=8) - timedelta(days=1)
                target_date = yesterday.strftime('%Y-%m-%d')
            
            yield f"data: {json.dumps({'msg': f'Target Date: {target_date}'})}\n\n"

            # Fetch Accounts
            query = BHAccount.query.filter_by(status='active')
            if account_id:
                query = query.filter_by(account_id=account_id)
            
            accounts = query.all()
            total = len(accounts)
            yield f"data: {json.dumps({'msg': f'Found {total} active accounts.'})}\n\n"

            # Group by Platform
            r_accounts = [a for a in accounts if a.platform == 'R']
            d_accounts = [a for a in accounts if a.platform == 'D']
            
            # --- Process R Platform ---
            if r_accounts:
                yield f"data: {json.dumps({'msg': f'Processing {len(r_accounts)} R-Platform accounts...'})}\n\n"
                r_client = RixbeeClient()
                
                # R allows batch fetching, BUT the API response does NOT include 'user_id' in the items
                # if we don't request specific dimensions. To be safe and ensure correct mapping,
                # we process 1 by 1 and inject the user_id.
                batch_size = 1
                for i in range(0, len(r_accounts), batch_size):
                    batch = r_accounts[i:i+batch_size]
                    acc_ids = [a.account_id for a in batch]
                    acc_map = {a.account_id: a for a in batch}
                    
                    try:
                        yield f"data: {json.dumps({'msg': f'  Fetching batch {i+1}-{min(i+batch_size, len(r_accounts))}...'})}\n\n"
                        # Fetch Data
                        raw_data = r_client.get_report_data(acc_ids, target_date, target_date)
                        
                        # INJECT user_id because API doesn't return it
                        # Since batch_size=1, we know these items belong to acc_ids[0]
                        current_acc_id = acc_ids[0]
                        for item in raw_data:
                            item['user_id'] = current_acc_id

                        # Process Data (Group by Account)
                        # R client raw_data is a list of dicts. 
                        # We need to aggregate by account (and handle CV definitions).
                        
                        # We need to process each account individually because CV definition varies per account?
                        # RClient.process_daily_stats takes raw_data and one definition.
                        # If definitions differ, we can't batch process efficiently unless we group by definition?
                        # Or we process raw_data item by item and look up account definition.
                        
                        # Let's refine RClient logic. 
                        # Ideally, we pass raw_data to a helper, and helper maps items to account, then applies CV def.
                        
                        # Manual Aggregation here:
                        # 1. Group raw items by account_id
                        items_by_acc = {}
                        for item in raw_data:
                            # Item 'user_id' is account id
                            # Note: R client ensures 'user_id' is present (we added it to params)
                            aid = str(item.get('user_id', ''))
                            if not aid: continue
                            if aid not in items_by_acc: items_by_acc[aid] = []
                            items_by_acc[aid].append(item)
                            
                        # 2. Update DB for each account in batch
                        count_updated = 0
                        for acc in batch:
                            acc_items = items_by_acc.get(acc.account_id, [])
                            if not acc_items:
                                # No data for this account (Spend = 0?)
                                # Should we upsert 0? Yes, to show data is fresh.
                                stats = {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0}
                            else:
                                # Aggregate
                                stats = r_client.process_daily_stats(acc_items, acc.cv_definition)
                                # stats is {(acc_id, date): {metrics}}
                                # We only have 1 date here.
                                key = (acc.account_id, target_date)
                                stats = stats.get(key, {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})

                            # Upsert DB
                            self._upsert_stats(acc.account_id, target_date, stats)
                            
                            # Log for R Platform
                            print(f"[BH-R-Daily-Sync] Account {acc.account_id} Date {target_date}: {stats}")
                            print(f"[BH-R-Daily-Sync-SQL] Account {acc.account_id} Date {target_date} -> Upserted (Spend={stats.get('spend')})")
                            
                            # Detailed Log for SSE
                            log_msg = f"    [{acc.platform}] {acc.account_id}: Spend={stats.get('spend',0)}, Clicks={stats.get('clicks',0)}"
                            yield f"data: {json.dumps({'msg': log_msg})}\n\n"
                            
                            count_updated += 1

                        
                        yield f"data: {json.dumps({'msg': f'  Batch processed.'})}\n\n"
                        
                    except Exception as e:
                        yield f"data: {json.dumps({'msg': f'  Error in R batch: {str(e)}', 'type': 'error'})}\n\n"

            # --- Process D Platform ---
            if d_accounts:
                yield f"data: {json.dumps({'msg': f'Processing {len(d_accounts)} D-Platform accounts...'})}\n\n"
                
                # 1. Fetch Tokens for these accounts
                d_acc_ids = [a.account_id for a in d_accounts]
                tokens = BHDAccountToken.query.filter(BHDAccountToken.account_id.in_(d_acc_ids)).all()
                token_map = {t.account_id: t.token for t in tokens} # AccID -> Token
                
                # 2. Group Accounts by Token
                # Key: Token, Value: List[BHAccount]
                accs_by_token = {}
                for acc in d_accounts:
                    token = token_map.get(acc.account_id)
                    if not token:
                        yield f"data: {json.dumps({'msg': f'  [WARNING] No Token found for D-Account {acc.account_id}, skipping...'})}\n\n"
                        continue
                        
                    if token not in accs_by_token:
                        accs_by_token[token] = []
                    accs_by_token[token].append(acc)

                # 3. Process by Token Group
                for token, acc_batch in accs_by_token.items():
                    try:
                        yield f"data: {json.dumps({'msg': f'  Fetching D stats for {len(acc_batch)} accounts (Token: {token[:10]}...)'})}\n\n"
                        d_client = DiscoveryClient(token)
                        
                        # Pass the specific account IDs managed by this token
                        batch_ids = [str(a.account_id) for a in acc_batch]
                        d_stats_map = d_client.fetch_daily_stats(batch_ids, target_date, target_date)
                        
                        # DEBUG: Print all keys gathered from D Platform
                        # print(f"[DEBUG D-MAP KEYS] {list(d_stats_map.keys())}")

                        # Update DB for accounts in this batch
                        for acc in acc_batch:
                            # Look up stats
                            key = (acc.account_id, target_date)
                            stats = d_stats_map.get(key, {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                            
                            # DEBUG: detailed lookup log
                            if stats.get('spend', 0) > 0:
                                print(f"[DEBUG DB-UPSERT] Lookup Key: {key} -> Stats Found: {stats}")
                            
                            self._upsert_stats(acc.account_id, target_date, stats)
                            
                            # Log for D Platform
                            print(f"[BH-D-Daily-Sync-SQL] Account {acc.account_id} Date {target_date} -> Upserted (Spend={stats.get('spend')})")
                            
                            # Detailed Log
                            log_msg = f"    [{acc.platform}] {acc.account_id}: Spend={stats.get('spend',0)}, Clicks={stats.get('clicks',0)}"
                            yield f"data: {json.dumps({'msg': log_msg})}\n\n"
                        
                    except Exception as e:
                        yield f"data: {json.dumps({'msg': f'  Error in D Token Batch: {str(e)}', 'type': 'error'})}\n\n"

                yield f"data: {json.dumps({'msg': f'  D Platform processed.'})}\n\n"

            yield f"data: {json.dumps({'msg': 'Sync Completed Successfully!', 'done': True})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'msg': f'Critical Error: {str(e)}', 'type': 'error'})}\n\n"

    def _upsert_stats(self, account_id, date, stats):
        # Prepare object
        # Check existing
        existing = BHDailyStats.query.filter_by(account_id=account_id, date=date).first()
        if existing:
            # print(f"[DB] Found existing record for Account {account_id} Date {date}. UPDATING...")
            existing.spend = stats['spend']
            existing.impressions = stats['impressions']
            existing.clicks = stats['clicks']
            existing.conversions = stats['conversions']
            existing.updated_at = datetime.utcnow()
        else:
            # print(f"[DB] No record found for Account {account_id} Date {date}. INSERTING...")
            new_stat = BHDailyStats(
                account_id=account_id,
                date=date,
                spend=stats['spend'],
                impressions=stats['impressions'],
                clicks=stats['clicks'],
                conversions=stats['conversions'],
                updated_at=datetime.utcnow(),
                raw_data=None 
            )
            db.session.add(new_stat)
            
        try:
            db.session.commit()
            # print("[DB] Commit successful.")
        except Exception as e:
            db.session.rollback()
            # print(f"[DB ERROR] Commit failed: {e}")
            raise e

    def sync_consistency_check(self):
        yield f"data: {json.dumps({'msg': 'Starting Data Integrity Check...'})}\n\n"
        
        try:
            # TW Time for "Yesterday"
            yesterday = datetime.utcnow() + timedelta(hours=8) - timedelta(days=1)
            yesterday_date = yesterday.date()
            
            # Fetch Active Accounts
            accounts = BHAccount.query.filter_by(status='active').all()
            total = len(accounts)
            yield f"data: {json.dumps({'msg': f'Scanning {total} active accounts...'})}\n\n"

            r_client = RixbeeClient()
            # d_client init moved to inside loop because it needs dynamic token

            for acc in accounts:
                if not acc.start_date: continue
                
                # Check Range
                if acc.start_date > yesterday_date:
                    continue
                
                # Find all dates needed
                current = acc.start_date
                needed_dates = []
                while current <= yesterday_date:
                    needed_dates.append(current)
                    current += timedelta(days=1)
                
                if not needed_dates: continue
                
                range_str = f"{needed_dates[0].strftime('%Y-%m-%d')} ~ {needed_dates[-1].strftime('%Y-%m-%d')}"

                # Query DB for Existing Dates
                existing_stats = BHDailyStats.query.filter(
                    BHDailyStats.account_id == acc.account_id,
                    BHDailyStats.date.in_(needed_dates)
                ).all()
                
                existing_dates_set = {stat.date for stat in existing_stats}
                
                # Identify Missing
                missing_dates = [d for d in needed_dates if d not in existing_dates_set]
                
                # Log Status
                yield f"data: {json.dumps({'msg': f'[{acc.platform}] {acc.account_id} Range: {range_str}'})}\n\n"
                yield f"data: {json.dumps({'msg': f'  - DB has: {len(existing_dates_set)} days. Missing: {len(missing_dates)} days.'})}\n\n"
                
                if not missing_dates:
                    continue
                
                # Backfill
                for m_date in missing_dates:
                    target_str = m_date.strftime('%Y-%m-%d')
                    yield f"data: {json.dumps({'msg': f'  -> Fetching {target_str}...'})}\n\n"
                    
                    try:
                        stats = {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0}
                        
                        if acc.platform == 'R':
                            raw_data = r_client.get_report_data([acc.account_id], target_str, target_str)
                            for item in raw_data: item['user_id'] = acc.account_id
                            if raw_data:
                                stats = r_client.process_daily_stats(raw_data, acc.cv_definition)
                                key = (acc.account_id, target_str)
                                stats = stats.get(key, {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                                
                                # Log for R Platform (Intraday)
                                print(f"[BH-R-Intraday-Sync] Account {acc.account_id} Date {target_str}: {stats}")
                                print(f"[BH-R-Intraday-Sync-SQL] Account {acc.account_id} Date {target_str} -> Upserted (Spend={stats.get('spend')})")
                                
                        elif acc.platform == 'D':
                            # Fetch Token
                            d_token_row = BHDAccountToken.query.filter_by(account_id=acc.account_id).first()
                            if not d_token_row:
                                yield f"data: {json.dumps({'msg': f'     [WARNING] No Token for D-Account {acc.account_id}, skipping...'})}\n\n"
                                continue
                            
                            d_client = DiscoveryClient(d_token_row.token)
                            # Pass explicit account ID and Intraday Log Tag
                            d_map = d_client.fetch_daily_stats([str(acc.account_id)], target_str, target_str, log_tag='[BH-D-Intraday-Sync]')
                            key = (acc.account_id, target_str)
                            stats = d_map.get(key, {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                            
                            # Log for D Platform (Intraday)
                            print(f"[BH-D-Intraday-Sync-SQL] Account {acc.account_id} Date {target_str} -> Upserted (Spend={stats.get('spend')})")
                        
                        # Upsert
                        self._upsert_stats(acc.account_id, target_str, stats)
                        
                        yield f"data: {json.dumps({'msg': f'     Got: Spend={stats.get("spend")}, Clicks={stats.get("clicks")}. DB Write OK.'})}\n\n"
                        
                    except Exception as e:
                         yield f"data: {json.dumps({'msg': f'     Failed: {e}', 'type': 'error'})}\n\n"
            
            yield f"data: {json.dumps({'msg': 'Integrity Check Completed.', 'done': True})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'msg': f'Critical Error during Check: {str(e)}', 'type': 'error'})}\n\n"
