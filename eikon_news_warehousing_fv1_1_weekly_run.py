# -*- coding: utf-8 -*-
"""
Created on Thu Jun 19 23:52:36 2025

@author: manishy
"""

import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from bs4 import BeautifulSoup
import time
import logging
import yaml
import pymongo
from refinitiv.data import eikon as ek
import urllib.request, json
from sqlalchemy import create_engine, text
import pytz


# --- PARAMETERS ---
SLEEP_BETWEEN_REQUESTS = 0.3
SLEEP_EVERY_N_REQUESTS = 200
LONG_SLEEP_SECONDS = 60
MAX_REQUESTS_PER_RUN = 6000
CHUNK_SIZE_DAYS = 1000
VERBOSE = True

# -- New: Sleep after every 5 minutes
LONG_SLEEP_AFTER_MINUTES = 5    # How often to long-sleep (minutes)
LONG_SLEEP_DURATION = 60        # How long to sleep (seconds)

# --- GLOBAL COUNTER ---
API_CALL_COUNT = 0

# --- SETUP LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='fetch.log'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)
logger = logging.getLogger(__name__)

def print_step_output(title, df, max_rows=5):
    print("\n" + "="*40)
    print(title)
    print("="*40)
    if df is not None and not df.empty:
        print(df.head(max_rows))
        print(f"\nTotal records: {len(df)}\n")
    else:
        print("No records found.\n")

def log_api_call(query, start_str, end_str, api_type="news_headlines"):
    global API_CALL_COUNT
    API_CALL_COUNT += 1
    if VERBOSE:
        print(f"[API CALL #{API_CALL_COUNT}] ({api_type}) Query: {query} | {start_str} to {end_str}")

# --- RATE LIMITER ---
class SafeLimiter:
    def __init__(self, max_per_run, sleep_between, block_n, long_sleep):
        self.max_per_run = max_per_run
        self.sleep_between = sleep_between
        self.block_n = block_n
        self.long_sleep = long_sleep
        self.request_count = 0

    def check(self):
        if self.request_count >= self.max_per_run:
            logger.warning(f"Request cap reached: {self.request_count}/{self.max_per_run}")
            raise RuntimeError("Max requests for this run reached. Aborting safely.")
        if self.request_count > 0 and self.request_count % self.block_n == 0:
            logger.info(f"Long sleep after {self.request_count} requests")
            time.sleep(self.long_sleep)
        else:
            time.sleep(self.sleep_between)
        self.request_count += 1

def load_config(config_file='C:\\smallcase\\Automatically\\config.yaml'):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def fetch_universe_from_db() -> pd.DataFrame:
    db = client.wm_research
    collection = db.universe
    data = collection.find({})
    l = []
    for i in data:
        l.append(i)
    df = pd.DataFrame.from_dict(l, orient='columns')
    df = df[df["is_active"] == 1]
    df = df[df["exchange"] == "NSE"]
    return df

def get_date_chunks(start_date, end_date, chunk_size_days=3):
    chunks = []
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_size_days - 1), end_date)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks

# --- GENERAL NEWS FETCH WITH GLOBAL SLEEP ---
def fetch_news_general(query, start_date, end_date, limiter, chunk_size_days=3, max_retries=3, retry_sleep=15, last_sleep_time_holder=None):
    all_news = []
    date_chunks = get_date_chunks(start_date, end_date, chunk_size_days)
    for chunk_start, chunk_end in date_chunks:
        start_str = chunk_start.strftime('%Y-%m-%dT00:00:00')
        end_str = chunk_end.strftime('%Y-%m-%dT23:59:59')
        limiter.check()

        # Global 5-min sleep logic
        if last_sleep_time_holder is not None:
            current_time = time.time()
            if current_time - last_sleep_time_holder[0] >= LONG_SLEEP_AFTER_MINUTES * 60:
                print(f"\n[SYSTEM] 5 minutes elapsed, sleeping for {LONG_SLEEP_DURATION} seconds...\n")
                logger.info("Sleeping for {} seconds after 5 minutes of execution.".format(LONG_SLEEP_DURATION))
                time.sleep(LONG_SLEEP_DURATION)
                last_sleep_time_holder[0] = time.time()

        attempt = 0
        while attempt < max_retries:
            try:
                log_api_call(query, start_str, end_str, api_type="news_headlines")
                df = ek.get_news_headlines(
                    query=query,
                    date_from=start_str, date_to=end_str, count=100
                )
                if 'df' not in locals():
                    raise RuntimeError("Output 'df' not defined by API call.")
                if not df.empty:
                    all_news.append(df)
                break  # Success
            except Exception as e:
                logger.error(f"[General][{query}][{start_str} to {end_str}] Attempt {attempt+1} failed: {e}")
                attempt += 1
                error_str = str(e).lower()
                if ("400 bad request" in error_str or "backend error" in error_str or "not defined" in error_str):
                    if attempt < max_retries:
                        print(f"Error on [{query}] ({start_str} to {end_str}): {e}\nRetrying in {retry_sleep} seconds... (Attempt {attempt}/{max_retries})")
                        time.sleep(retry_sleep)
                    else:
                        print(f"Skipping chunk [{query}] ({start_str} to {end_str}) after {max_retries} retries.")
                else:
                    break
    if all_news:
        combined = pd.concat(all_news, ignore_index=True)
        combined = combined.drop_duplicates(subset='storyId')
        return combined
    return pd.DataFrame()

def fetch_news_for_stock(instrument, date_chunks, limiter, max_retries=3, retry_sleep=15, last_sleep_time_holder=None):
    all_news = []
    for start_date, end_date in date_chunks:
        start_str = start_date.strftime('%Y-%m-%dT00:00:00')
        end_str = end_date.strftime('%Y-%m-%dT23:59:59')
        limiter.check()

        # Global 5-min sleep logic
        if last_sleep_time_holder is not None:
            current_time = time.time()
            if current_time - last_sleep_time_holder[0] >= LONG_SLEEP_AFTER_MINUTES * 60:
                print(f"\n[SYSTEM] 5 minutes elapsed, sleeping for {LONG_SLEEP_DURATION} seconds...\n")
                logger.info("Sleeping for {} seconds after 5 minutes of execution.".format(LONG_SLEEP_DURATION))
                time.sleep(LONG_SLEEP_DURATION)
                last_sleep_time_holder[0] = time.time()

        attempt = 0
        while attempt < max_retries:
            try:
                full_query = f"RIC:{instrument} AND Language:LEN"
                log_api_call(full_query, start_str, end_str, api_type="news_headlines")
                df = ek.get_news_headlines(
                    query=full_query,
                    date_from=start_str, date_to=end_str, count=100
                )
                if 'df' not in locals():
                    raise RuntimeError("Output 'df' not defined by API call.")
                if not df.empty:
                    df['RIC'] = instrument
                    all_news.append(df)
                break
            except Exception as e:
                logger.error(f"[Stock][{instrument}][{start_str} to {end_str}] Attempt {attempt+1} failed: {e}")
                attempt += 1
                error_str = str(e).lower()
                if ("400 bad request" in error_str or "backend error" in error_str or "not defined" in error_str):
                    if attempt < max_retries:
                        print(f"Error fetching {instrument} ({start_str} to {end_str}): {e}\nRetrying in {retry_sleep} seconds... (Attempt {attempt}/{max_retries})")
                        time.sleep(retry_sleep)
                    else:
                        print(f"Skipping chunk {instrument} ({start_str} to {end_str}) after {max_retries} retries.")
                else:
                    break
    if all_news:
        combined = pd.concat(all_news, ignore_index=True)
        combined = combined.drop_duplicates(subset='storyId')
        return combined
    return pd.DataFrame()

def fetch_news_for_multiple_stocks(rics, start_date, end_date, limiter, chunk_size_days=10, last_sleep_time_holder=None):
    all_news = []
    date_chunks = get_date_chunks(start_date, end_date, chunk_size_days)
    for ric in tqdm(rics, desc="RICs"):
        if limiter.request_count >= limiter.max_per_run:
            logger.info(f"Stopped at {limiter.request_count} requests (max per run reached).")
            break
        df = fetch_news_for_stock(ric, date_chunks, limiter, last_sleep_time_holder=last_sleep_time_holder)
        if df.empty:
            continue
        # --- Verbose preview ---
        if VERBOSE and "text" in df.columns:
            print(f"\n{'='*30}\nRIC: {ric}\n{'='*30}")
            print(df['text'].head(5).to_string(index=False))
            print(f"(Total headlines for {ric}: {len(df)})\n")
        all_news.append(df)
    if all_news:
        full_df = pd.concat(all_news, ignore_index=True)
        full_df = full_df.drop_duplicates(subset='storyId')
        return full_df
    return pd.DataFrame()

def create_news_table(engine, table_name):
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        version_created TIMESTAMPTZ,
        heading TEXT,
        story_id TEXT PRIMARY KEY,
        source_code TEXT,
        ric TEXT,
        news_text TEXT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
    print(f"Table '{table_name}' is ready (with PRIMARY KEY on story_id).")

def upsert_final_news(engine, table_name, final_news):
    # Ensure all required columns are present
    REQUIRED_COLS = ['version_created', 'heading', 'story_id', 'source_code', 'ric', 'news_text']
    final_news = final_news.reindex(columns=REQUIRED_COLS, fill_value=None)
    final_news = final_news.where(pd.notnull(final_news), None)  # pandas NaN -> None
    records = final_news.to_dict(orient='records')
    with engine.begin() as conn:
        for row in records:
            insert_stmt = f"""
            INSERT INTO {table_name} (version_created, heading, story_id, source_code, ric, news_text)
            VALUES (:version_created, :heading, :story_id, :source_code, :ric, :news_text)
            ON CONFLICT (story_id) DO UPDATE SET
                version_created = EXCLUDED.version_created,
                heading = EXCLUDED.heading,
                source_code = EXCLUDED.source_code,
                ric = EXCLUDED.ric,
                news_text = EXCLUDED.news_text;
            """
            conn.execute(text(insert_stmt), row)
    print(f"Upserted {len(records)} records to '{table_name}'.")

def truncate_table(engine, table_name):
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name};"))
    print(f"Table '{table_name}' truncated.")

def convert_to_ist(final_news):
    if final_news['version_created'].dtype == 'O':
        final_news['version_created'] = pd.to_datetime(final_news['version_created'], utc=True, errors='coerce')
    else:
        final_news['version_created'] = final_news['version_created'].dt.tz_convert('UTC')
    # Convert to IST
    ist = pytz.timezone('Asia/Kolkata')
    final_news['version_created'] = final_news['version_created'].dt.tz_convert(ist)
    return final_news

def overlapping_staggered_windows(today=None, lookback_months=15):
    if today is None:
        today = datetime.today()
    min_date = (today - relativedelta(months=lookback_months)).replace(hour=0, minute=0, second=0, microsecond=0)
    run_list = []
    cur_start = min_date
    run_id = 1

    while cur_start < today:
        # 1-month window
        end_1m = (cur_start + relativedelta(months=1)) - timedelta(seconds=1)
        if end_1m > today:
            end_1m = today
        run_list.append({
            'run_id': run_id,
            'start': cur_start,
            'end': end_1m,
            'window_type': '1_month'
        })
        run_id += 1

        # 1-quarter window from the same start
        end_qtr = (cur_start + relativedelta(months=3)) - timedelta(seconds=1)
        if end_qtr > today:
            end_qtr = today
        run_list.append({
            'run_id': run_id,
            'start': cur_start,
            'end': end_qtr,
            'window_type': 'quarter'
        })
        run_id += 1

        # Move start forward by 1 month
        cur_start = cur_start + relativedelta(months=1)
    
    return run_list

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    # --- Load config and setup DB ---
    config = load_config('C:\\smallcase\\Automatically\\config.yaml')  # Change path as needed
    ek.set_app_key(config['ekn']['jdn'])
    upass = urllib.parse.quote_plus(config['wm_prod']['password'])
    client = pymongo.MongoClient(
        f'mongodb://{config["wm_prod"]["user"]}:{upass}@{config["wm_prod"]["host"]}:{config["wm_prod"]["port"]}/?ssl=true'
        f'&ssl_ca_certs=C:\\smallcase\\Automatically\\global-bundle.pem&replicaSet=rs0&readPreference'
        f'=secondaryPreferred&retryWrites=false')
    CLIENT_TYPE = "PROD"

    # --- Fetch RIC universe ---
    universe_df = fetch_universe_from_db()
    ric_list = universe_df['RIC'].tolist()

    # --- Date Window ---
    today = datetime.today()
    windows = overlapping_staggered_windows(datetime(2026, 6, 20))

        # 2. Load last_run_id (or start fresh)
    try:
        with open('progress.txt', 'r') as f:
            last_run_id = int(f.read())
    except FileNotFoundError:
        last_run_id = 0

    # 3. Pick next pending window (or N if you want)
    pending_windows = [w for w in windows if w['run_id'] > last_run_id]
    if not pending_windows:
        print("All windows completed!")
        # Optionally exit or reset
        exit()

    # Pick the next window to use for this run
    next_window = pending_windows[0]   # (or loop through several if desired)
    start_dt = next_window['start']
    end_dt = next_window['end']

    print(f"Setting dates for Run {next_window['run_id']}: {start_dt} to {end_dt} ({next_window['window_type']})")

    # start_dt = (today - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
    # end_dt = (today + timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)

    # --- Rate limiter ---
    limiter = SafeLimiter(MAX_REQUESTS_PER_RUN, SLEEP_BETWEEN_REQUESTS, SLEEP_EVERY_N_REQUESTS, LONG_SLEEP_SECONDS)

    # --- Shared timer for 5-min sleep ---
    last_sleep_time_holder = [time.time()]

    # --- General Fetches ---
    news_sugg_in = fetch_news_general(
        query="SourcePackage:SUGG AND Language:LEN AND Topic:IN",
        start_date=start_dt,
        end_date=end_dt,
        limiter=limiter,
        chunk_size_days=CHUNK_SIZE_DAYS,
        last_sleep_time_holder=last_sleep_time_holder
    )
    if VERBOSE:
        print_step_output("Step 1: SUGG (India Topic, English)", news_sugg_in.text)

    news_mintne = fetch_news_general(
        query="Source:MINTNE",
        start_date=start_dt,
        end_date=end_dt,
        limiter=limiter,
        chunk_size_days=CHUNK_SIZE_DAYS,
        last_sleep_time_holder=last_sleep_time_holder
    )
    if VERBOSE:
        print_step_output("Step 2: MINTNE (All)", news_mintne.text)

    # --- Fetch News (Stockwise) ---
    try:
        news_df_all = fetch_news_for_multiple_stocks(
            rics=ric_list,
            start_date=start_dt,
            end_date=end_dt,
            limiter=limiter,
            chunk_size_days=CHUNK_SIZE_DAYS,
            last_sleep_time_holder=last_sleep_time_holder
        )
        if VERBOSE:
            print_step_output("Step 3: Stockwise News", news_df_all.text)
        logger.info(f"News fetch complete. {len(news_df_all)} records fetched.")
    except Exception as e:
        logger.error(f"Stopped execution due to: {e}")
        news_df_all = pd.DataFrame()

    # --- Final Combine ---
    all_news_dfs = []
    if news_sugg_in is not None and not news_sugg_in.empty:
        all_news_dfs.append(news_sugg_in)
    if news_mintne is not None and not news_mintne.empty:
        all_news_dfs.append(news_mintne)
    if news_df_all is not None and not news_df_all.empty:
        all_news_dfs.append(news_df_all)

    if all_news_dfs:
        final_news = pd.concat(all_news_dfs, ignore_index=True)
        final_news = final_news.drop_duplicates(subset='storyId')

        col_map = {
            'versionCreated': 'version_created',
            'text': 'heading',
            'storyId': 'story_id',
            'sourceCode': 'source_code',
            'RIC': 'ric'
        }
        final_news = final_news.rename(columns={k: v for k, v in col_map.items() if k in final_news.columns})

        if VERBOSE:
            print_step_output("Step 4: Final Combined News", final_news)
        final_news.to_csv("output1.csv", index=False)
        final_news.to_pickle("output1.pkl")
        logger.info(f"All news fetch complete. {len(final_news)} records saved.")
    else:
        print("No news found in any source.")

    print(f"\nTotal API calls made: {API_CALL_COUNT}\n")


    db_config = config['wm_sql']

    db_user = db_config['user']
    db_password = db_config['password']
    db_host = db_config['host']
    db_port = db_config['port']
    db_name = db_config['dbname']
    table_name = "news"

    # Replace with your real creds/config loader as needed
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    # truncate_table(engine, table_name)
    # create_news_table(engine, table_name)

    final_news2 = convert_to_ist(final_news)
    upsert_final_news(engine, table_name, final_news2)
    logger.info(f"upsert complete. {len(final_news2)} records updated.")

    # After successful run, update last_run_id
    last_run_id = next_window['run_id']
    with open('progress.txt', 'w') as f:
        f.write(str(last_run_id))

    logger.info(f"Dates for Run {next_window['run_id']}: {start_dt} to {end_dt} ({next_window['window_type']})")
    logger.info(f"run_id {str(last_run_id)} complete. {len(final_news2)} records updated.")