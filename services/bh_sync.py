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
    def sync_account_full_range(self, account_id, app):
        """
        Generator function for full range sync of a specific account.
        """
        yield f"data: {json.dumps({'msg': f'Starting Full Sync for Account {account_id}...'})}\n\n"
        
        # Use passed app object
        try:
            with app.app_context():
                # 1. Fetch Account Info (Must be Active)
                account = BHAccount.query.filter_by(account_id=account_id, status='active').first()
                if not account:
                    yield f"data: {json.dumps({'msg': f'Account not found or not active.', 'type': 'error'})}\n\n"
                    return
                    
                if not account.start_date:
                    yield f"data: {json.dumps({'msg': f'Account has no Start Date.', 'type': 'error'})}\n\n"
                    return
                    
                # Determine Date Range
                start_date = account.start_date
                end_date = account.end_date if account.end_date else date.today()
                # Cap end_date at yesterday (or today?)
                # Usually we sync up to yesterday for complete data, but some platforms might have today's data.
                # User said: "Effects like bh home sync data... directly from start to end"
                # Standard logic: sync up to yesterday to avoid partial day issues? 
                # Or allow today? Let's go with "Start Date ~ Today (or End Date)" but reliable stats usually usually start from yesterday.
                # Let's use max(end_date, yesterday) but capped at today.
                
                # Use Taiwan Time for Yesterday (Latest available data usually)
                taiwan_now = datetime.utcnow() + timedelta(hours=8)
                yesterday = (taiwan_now - timedelta(days=1)).date()
                
                # If end_date is in future or today, cap it at yesterday
                if end_date > yesterday: end_date = yesterday
                
                # Ensure safe start date
                SAFE_START_DATE = date(2024, 1, 1)
                if start_date < SAFE_START_DATE:
                     start_date = SAFE_START_DATE

                yield f"data: {json.dumps({'msg': f'Target Range: {start_date} ~ {end_date}'})}\n\n"
                
                current = start_date
                dates_to_sync = []
                while current <= end_date:
                    dates_to_sync.append(current)
                    current += timedelta(days=1)
                    
                total_days = len(dates_to_sync)
                yield f"data: {json.dumps({'msg': f'Total {total_days} days to be synced.'})}\n\n"
                
                # Platform Specific Logic
                if account.platform == 'R':
                    r_client = RixbeeClient()
                    # Batch by 7 days to be efficient?
                    batch_size = 7
                    for i in range(0, total_days, batch_size):
                        batch = dates_to_sync[i:i+batch_size]
                        s_str = batch[0].strftime('%Y-%m-%d')
                        e_str = batch[-1].strftime('%Y-%m-%d')
                        
                        yield f"data: {json.dumps({'msg': f'Fetching {s_str} ~ {e_str}...'})}\n\n"
                        
                        try:
                            raw_data = r_client.get_report_data([account_id], s_str, e_str, agent_id=account.agent)
                            
                            # Process & Save
                            data_by_date = {}
                            for item in raw_data:
                                item['user_id'] = account_id
                                d_key = item.get('day')
                                if d_key:
                                    if d_key not in data_by_date: data_by_date[d_key] = []
                                    data_by_date[d_key].append(item)
                                    
                            for target_date in batch:
                                target_str = target_date.strftime('%Y-%m-%d')
                                day_items = data_by_date.get(target_str, [])
                                
                                stats = {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0}
                                if day_items:
                                    batch_stats = r_client.process_daily_stats(day_items, account.cv_definition)
                                    key = (str(account_id), target_str)
                                    if key not in batch_stats:
                                         # Try int key
                                         key_int = (int(account_id) if str(account_id).isdigit() else account_id, target_str)
                                         stats = batch_stats.get(key_int, stats)
                                    else:
                                         stats = batch_stats.get(key, stats)

                                self._upsert_stats(account_id, target_str, stats, app=app)
                                
                                # Log daily stats
                                log_msg = f"  [{target_str}] Spend: {int(stats.get('spend', 0))} | Imp: {stats.get('impressions', 0)} | Click: {stats.get('clicks', 0)} | Conv: {stats.get('conversions', 0)}"
                                yield f"data: {json.dumps({'msg': log_msg})}\n\n"
                                
                            yield f"data: {json.dumps({'msg': f'  -> Saved.'})}\n\n"
                            
                        except Exception as e:
                            yield f"data: {json.dumps({'msg': f'  Error: {e}', 'type': 'error'})}\n\n"

                elif account.platform == 'D':
                    # Get Token
                    token_row = BHDAccountToken.query.filter_by(account_id=account_id).first()
                    if not token_row:
                        yield f"data: {json.dumps({'msg': f'No Token found for this account.', 'type': 'error'})}\n\n"
                        return
                    
                    d_client = DiscoveryClient(token_row.token)
                    
                    # D platform might not support long ranges smoothly, let's do day by day or small batches
                    # D Client 'fetch_daily_stats' takes list of IDs.
                    # It queries for a range.
                    
                    batch_size = 7
                    for i in range(0, total_days, batch_size):
                        batch = dates_to_sync[i:i+batch_size]
                        s_str = batch[0].strftime('%Y-%m-%d')
                        e_str = batch[-1].strftime('%Y-%m-%d')
                        
                        yield f"data: {json.dumps({'msg': f'Fetching {s_str} ~ {e_str}...'})}\n\n"
                        
                        try:
                            d_map = d_client.fetch_daily_stats([str(account_id)], s_str, e_str)
                            
                            for target_date in batch:
                                 target_str = target_date.strftime('%Y-%m-%d')
                                 key = (str(account_id), target_str)
                                 stats = d_map.get(key, {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                                 self._upsert_stats(account_id, target_str, stats, app=app)

                                 # Log daily stats
                                 log_msg = f"  [{target_str}] Spend: {int(stats.get('spend', 0))} | Imp: {stats.get('impressions', 0)} | Click: {stats.get('clicks', 0)} | Conv: {stats.get('conversions', 0)}"
                                 yield f"data: {json.dumps({'msg': log_msg})}\n\n"
                                 
                            yield f"data: {json.dumps({'msg': f'  -> Saved.'})}\n\n"
                            
                        except Exception as e:
                            yield f"data: {json.dumps({'msg': f'  Error: {e}', 'type': 'error'})}\n\n"
            
            yield f"data: {json.dumps({'msg': 'Full Sync Completed!', 'done': True})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'msg': f'Critical Error: {str(e)}', 'type': 'error'})}\n\n"

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def sync_daily_stats(self, target_date: str = None, account_id: str = None):
        """
        Generator function that yields log messages for SSE.
        Uses separate ThreadPoolExecutors for R and D parallel fetch operations.
        """
        yield f"data: {json.dumps({'msg': 'Starting Sync Process...'})}\n\n"
        
        start_time = time.time()
        
        # Separate executors for granular control
        r_executor = ThreadPoolExecutor(max_workers=5)
        d_executor = ThreadPoolExecutor(max_workers=10)
        
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
            
            # --- Process R Platform (Max 10) ---
            if r_accounts:
                yield f"data: {json.dumps({'msg': f'Processing {len(r_accounts)} R-Platform accounts (Parallel)...'})}\n\n"
                r_client = RixbeeClient()
                
                # Submit all R tasks
                future_to_acc = {}
                for acc in r_accounts:
                    # Submit job: Fetch data for 1 account
                    # We use batch size 1 to easily map result back to user_id
                    f = r_executor.submit(r_client.get_report_data, [acc.account_id], target_date, target_date, acc.agent)
                    future_to_acc[f] = acc

                # Process results as they complete
                processed_count = 0
                for future in as_completed(future_to_acc):
                    acc = future_to_acc[future]
                    try:
                        raw_data = future.result()
                        
                        # Process Data (Main Thread)
                        # Inject user_id (since we know it's for `acc` and API might strict it)
                        for item in raw_data:
                            item['user_id'] = acc.account_id
                        
                        # Aggregate
                        stats = {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0}
                        if raw_data:
                            # Aggregate using helper
                            # RClient.process_daily_stats expects logic to sum by account
                            # We can reuse it or just sum straightforwardly here since we have 1 acc
                            
                            # Using helper to ensure consistency with CV map
                            stats_map = r_client.process_daily_stats(raw_data, acc.cv_definition)
                            
                            # Get correct key
                            key_str = (str(acc.account_id), target_date)
                            key_int = (int(acc.account_id) if str(acc.account_id).isdigit() else acc.account_id, target_date)
                            
                            stats = stats_map.get(key_str) or stats_map.get(key_int) or stats

                        # Upsert DB
                        self._upsert_stats(acc.account_id, target_date, stats)
                        
                        # Log
                        log_msg = f"    [{acc.platform}] {acc.account_id}: Spend={int(stats.get('spend',0))}, Clicks={stats.get('clicks',0)}"
                        yield f"data: {json.dumps({'msg': log_msg})}\n\n"
                        pass

                    except Exception as e:
                        yield f"data: {json.dumps({'msg': f'  Error R-Acc {acc.account_id}: {str(e)}', 'type': 'error'})}\n\n"
                    
                    processed_count += 1
                    # Optional: Progress update every N items?
                
                yield f"data: {json.dumps({'msg': f'  R Platform processed ({len(r_accounts)} accounts).'})}\n\n"

            # --- Process D Platform (Max 3) ---
            if d_accounts:
                yield f"data: {json.dumps({'msg': f'Processing {len(d_accounts)} D-Platform accounts (Hybrid Parallel)...'})}\n\n"
                
                # 1. Fetch Tokens for these accounts
                d_acc_ids = [a.account_id for a in d_accounts]
                tokens = BHDAccountToken.query.filter(BHDAccountToken.account_id.in_(d_acc_ids)).all()
                token_map = {t.account_id: t.token for t in tokens} # AccID -> Token
                
                # 2. Group Accounts by Token
                accs_by_token = {}
                for acc in d_accounts:
                    token = token_map.get(acc.account_id)
                    if not token:
                        yield f"data: {json.dumps({'msg': f'  [WARNING] No Token found for D-Account {acc.account_id}, skipping...'})}\n\n"
                        continue
                        
                    if token not in accs_by_token:
                        accs_by_token[token] = []
                    accs_by_token[token].append(acc)

                # 3. Process by Token Group (Sequential Token, Parallel Internals)
                for token, acc_batch in accs_by_token.items():
                    try:
                        batch_ids = [str(a.account_id) for a in acc_batch]
                        yield f"data: {json.dumps({'msg': f'  Fetching D stats for Account(s): {batch_ids} (Token: {token[:6]}...)'})}\n\n"
                        
                        d_client = DiscoveryClient(token)
                        
                        # PASS D-SPECIFIC EXECUTOR (Max 3)
                        d_stats_map = d_client.fetch_daily_stats(batch_ids, target_date, target_date, executor=d_executor)
                        
                        # Update DB for accounts in this batch
                        for acc in acc_batch:
                            key = (acc.account_id, target_date)
                            stats = d_stats_map.get(key, {'spend': 0, 'impressions': 0, 'clicks': 0, 'conversions': 0})
                            
                            self._upsert_stats(acc.account_id, target_date, stats)
                            
                            log_msg = f"    [{acc.platform}] {acc.account_id}: Spend={int(stats.get('spend',0))}, Clicks={stats.get('clicks',0)}"
                            yield f"data: {json.dumps({'msg': log_msg})}\n\n"
                        
                    except Exception as e:
                        yield f"data: {json.dumps({'msg': f'  Error in D Token Batch: {str(e)}', 'type': 'error'})}\n\n"

                yield f"data: {json.dumps({'msg': f'  D Platform processed.'})}\n\n"

            elapsed = time.time() - start_time
            time_str = f"{int(elapsed // 60)}分{int(elapsed % 60)}秒({int(elapsed)}秒)"
            yield f"data: {json.dumps({'msg': f'Sync Completed Successfully! 總耗時時間 {time_str}', 'done': True})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'msg': f'Critical Error: {str(e)}', 'type': 'error'})}\n\n"
        finally:
            r_executor.shutdown(wait=False)
            d_executor.shutdown(wait=False)

    def _upsert_stats(self, account_id, date, stats, app=None):
        # Ensure context if app is passed
        ctx = app.app_context() if app else None
        if ctx: ctx.push()
        
        try:
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
        finally:
            if ctx: ctx.pop()

    def sync_consistency_check(self):
        yield f"data: {json.dumps({'msg': 'Starting Data Integrity Check (Parallel)...'})}\n\n"
        
        # Shared executor for D-Platform concurrent details fetching
        d_executor = ThreadPoolExecutor(max_workers=10)
        
        try:
            yesterday = datetime.utcnow() + timedelta(hours=8) - timedelta(days=1)
            yesterday_date = yesterday.date()
            
            # Fetch Active Accounts
            accounts = BHAccount.query.filter_by(status='active').all()
            total = len(accounts)
            yield f"data: {json.dumps({'msg': f'Scanning {total} active accounts with 2 workers...'})}\n\n"

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
                                        # Use Shared Executor
                                        d_map = d_client.fetch_daily_stats([str(acc_id)], target_str, target_str, log_tag='[BH-D-Intra]', executor=d_executor)
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
            with ThreadPoolExecutor(max_workers=2) as executor:
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
        finally:
            d_executor.shutdown(wait=False)
