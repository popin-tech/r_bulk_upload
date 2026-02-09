import time
import json
from datetime import datetime, timedelta, date
from database import db, BHAccount, BHDailyStats, BHDAccountToken
from services.bh_clients.r_client import RixbeeClient
from services.bh_clients.d_client import DiscoveryClient
from flask import current_app
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
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
                        # Pass agent_id (Since batch_size=1, we can use batch[0].agent)
                        agent_id = batch[0].agent
                        raw_data = r_client.get_report_data(acc_ids, target_date, target_date, agent_id=agent_id)
                        
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
                            print(f"[BH-R-Daily-Sync] Account {acc.account_id} Date {target_date}: {stats}", flush=True)
                            print(f"[BH-R-Daily-Sync-SQL] Account {acc.account_id} Date {target_date} -> Upserted (Spend={stats.get('spend')})", flush=True)
                            
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
                            print(f"[BH-D-Daily-Sync-SQL] Account {acc.account_id} Date {target_date} -> Upserted (Spend={stats.get('spend')})", flush=True)
                            
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
        yield f"data: {json.dumps({'msg': 'Starting Data Integrity Check (Parallel)...'})}\n\n"
        
        try:
            yesterday = datetime.utcnow() + timedelta(hours=8) - timedelta(days=1)
            yesterday_date = yesterday.date()
            
            # Fetch Active Accounts
            accounts = BHAccount.query.filter_by(status='active').all()
            total = len(accounts)
            yield f"data: {json.dumps({'msg': f'Scanning {total} active accounts with 5 workers...'})}\n\n"

            # Prepare for Threading
            app = current_app._get_current_object()
            
            # Worker Function
            def _process_account(acc_id, platform, start_date, cv_def, agent_id=None):
                logs = []
                try:
                    with app.app_context():
                        # Re-calculate missing dates inside thread to ensure isolated logic
                        if not start_date or start_date > yesterday_date:
                            return logs

                        # Ensure start_date is reasonable (prevent 1970-01-01 fetching)
                        SAFE_START_DATE = date(2024, 1, 1)
                        if start_date < SAFE_START_DATE:
                            # print(f"[Worker-Log] Adjusting start_date from {start_date} to {SAFE_START_DATE}", flush=True)
                            start_date = SAFE_START_DATE

                        current = start_date
                        needed_dates = []
                        # Limit scan range? No, user wants full integrity check.
                        while current <= yesterday_date:
                            needed_dates.append(current)
                            current += timedelta(days=1)
                        
                        if not needed_dates: return logs
                        
                        # Check DB
                        existing_stats = BHDailyStats.query.filter(
                            BHDailyStats.account_id == acc_id,
                            BHDailyStats.date.in_(needed_dates)
                        ).all()
                        existing_dates_set = {stat.date for stat in existing_stats}
                        missing_dates = sorted([d for d in needed_dates if d not in existing_dates_set])

                        if not missing_dates:
                            return logs

                        range_str = f"{needed_dates[0].strftime('%Y-%m-%d')} ~ {needed_dates[-1].strftime('%Y-%m-%d')}"
                        log_msg = f"[{platform}] {acc_id} Missing {len(missing_dates)} days (Range: {range_str})"
                        logs.append(log_msg)
                        print(f"[Worker-Log] {log_msg}", flush=True)

                        # --- Platform Specific Logic ---
                        if platform == 'R':
                            r_client = RixbeeClient()
                            
                            # Group missing dates into continuous chunks of max 7 days
                            # Logic: Iterate sorted dates, group if contiguous and batch < 7
                            if missing_dates:
                                current_batch = [missing_dates[0]]
                                batches = []
                                
                                for d in missing_dates[1:]:
                                    last_d = current_batch[-1]
                                    if (d - last_d).days == 1 and len(current_batch) < 7:
                                        current_batch.append(d)
                                    else:
                                        batches.append(current_batch)
                                        current_batch = [d]
                                if current_batch:
                                    batches.append(current_batch)
                                    
                                for batch in batches:
                                    s_str = batch[0].strftime('%Y-%m-%d')
                                    e_str = batch[-1].strftime('%Y-%m-%d')
                                    # logs.append(f"  -> Fetching batch {s_str} to {e_str}...")
                                    print(f"[Worker-Log]   -> R-Batch Fetching {s_str} ~ {e_str}...", flush=True)
                                    
                                    try:
                                        # API Call
                                        raw_data = r_client.get_report_data([acc_id], s_str, e_str, agent_id=agent_id)
                                        
                                        # Process Data
                                        data_by_date = {}
                                        for item in raw_data:
                                            item['user_id'] = acc_id
                                            d_key = item.get('day')
                                            if d_key:
                                                if d_key not in data_by_date: data_by_date[d_key] = []
                                                data_by_date[d_key].append(item)
                                        
                                        # Write DB (Upsert 0 if empty)
                                        for target_date in batch:
                                            target_str = target_date.strftime('%Y-%m-%d')
                                            day_items = data_by_date.get(target_str, [])
                                            
                                            stats = {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0}
                                            if day_items:
                                                # Use existing helper logic if possible, or replicate behavior
                                                # RClient.process_daily_stats aggregates list
                                                batch_stats = r_client.process_daily_stats(day_items, cv_def)
                                                
                                                # Log keys for debug
                                                if str(acc_id) == '9573':
                                                    print(f"[DEBUG 9573] Keys in batch_stats: {list(batch_stats.keys())}", flush=True)
                                                    print(f"[DEBUG 9573] Looking for key: {(acc_id, target_str)}", flush=True)
                                                    # Try string key too
                                                    print(f"[DEBUG 9573] Looking for str key: {(str(acc_id), target_str)}", flush=True)
                                                    
                                                # Try both str and int keys
                                                key = (str(acc_id), target_str)
                                                if key not in batch_stats:
                                                    key = (int(acc_id) if str(acc_id).isdigit() else acc_id, target_str)
                                                    
                                                stats = batch_stats.get(key, stats)
                                                
                                                if str(acc_id) == '9573':
                                                    print(f"[DEBUG 9573] Final stats for {target_str}: {stats}", flush=True)
                                            
                                            self._upsert_stats(acc_id, target_str, stats)
                                            
                                        logs.append(f"     Batch {s_str}~{e_str} Saved ({len(batch)} days).")
                                        print(f"[Worker-Log]   -> R-Batch {s_str}~{e_str} Saved.", flush=True)
                                            
                                    except Exception as e:
                                        err_msg = f"     Batch {s_str}~{e_str} Failed: {e}"
                                        logs.append(err_msg)
                                        print(f"[Worker-Log] {err_msg}", flush=True)

                        elif platform == 'D':
                            d_token_row = BHDAccountToken.query.filter_by(account_id=acc_id).first()
                            if not d_token_row:
                                logs.append(f"     [WARNING] No Token for D-Account {acc_id}")
                            else:
                                d_client = DiscoveryClient(d_token_row.token)
                                # Fetch day by day
                                for m_date in missing_dates:
                                    target_str = m_date.strftime('%Y-%m-%d')
                                    print(f"[Worker-Log]   -> D-Fetch {acc_id} Date {target_str}...", flush=True)
                                    try:
                                        d_map = d_client.fetch_daily_stats([str(acc_id)], target_str, target_str, log_tag='[BH-D-Intra]')
                                        key = (acc_id, target_str)
                                        stats = d_map.get(key, {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                                        
                                        self._upsert_stats(acc_id, target_str, stats)
                                        
                                    except Exception as e:
                                        logs.append(f"     Failed {target_str}: {e}")
                                        print(f"[Worker-Log]   -> D-Fetch Details {target_str} Failed: {e}", flush=True)
                                        
                                logs.append(f"     D-Platform: Processed {len(missing_dates)} days.")
                                print(f"[Worker-Log]   -> D-Platform {acc_id} Finished.", flush=True)

                except Exception as e:
                    logs.append(f"Error processing {acc_id}: {str(e)}")
                
                return logs

            # Execute Parallel Jobs
            futures = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                for acc in accounts:
                    if not acc.start_date: continue
                    futures.append(executor.submit(
                        _process_account, 
                        acc.account_id, 
                        acc.platform, 
                        acc.start_date, 
                        acc.cv_definition,
                        acc.agent
                    ))
                
                for future in as_completed(futures):
                    for log in future.result():
                        yield f"data: {json.dumps({'msg': log})}\n\n"
            
            yield f"data: {json.dumps({'msg': 'Integrity Check Completed.', 'done': True})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'msg': f'Critical Error during Check: {str(e)}', 'type': 'error'})}\n\n"
