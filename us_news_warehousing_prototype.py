# -*- coding: utf-8 -*-
"""
US News Warehousing — Prototype
Fetches daily per-RIC news for the top 500 US equities (by market cap)
and upserts into PostgreSQL table `us_news`.

Universe source : PostgreSQL `us_universe` (NOT MongoDB)
Scope           : Daily news only — no backfill
Progress        : progress_us.txt tracks last completed RIC index within today's run
"""

import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import time
import logging
import yaml
import urllib.parse
from refinitiv.data import eikon as ek
from sqlalchemy import create_engine, text
import pytz

# ---------------------------------------------------------------------------
# PARAMETERS
# ---------------------------------------------------------------------------
TOP_N_BY_MKTCAP        = 500
LOOKBACK_DAYS          = 1        # yesterday 00:00 → today 23:59
CHUNK_SIZE_DAYS        = 1000     # single chunk for daily window
SLEEP_BETWEEN_REQUESTS = 0.3
SLEEP_EVERY_N_REQUESTS = 200
LONG_SLEEP_SECONDS     = 60
MAX_REQUESTS_PER_RUN   = 6000
VERBOSE                = True

LONG_SLEEP_AFTER_MINUTES = 5
LONG_SLEEP_DURATION      = 60

CONFIG_PATH    = 'C:\\smallcase\\Automatically\\config.yaml'
DB_CONFIG_PATH = 'db_config.yaml'
TABLE_NAME     = 'us_news'
PROGRESS_FILE  = 'progress_us.txt'

# ---------------------------------------------------------------------------
# GLOBAL COUNTER
# ---------------------------------------------------------------------------
API_CALL_COUNT = 0

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='us_news_fetch.log'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# RATE LIMITER  (matches India script exactly)
# ---------------------------------------------------------------------------
class SafeLimiter:
    def __init__(self, max_per_run, sleep_between, block_n, long_sleep):
        self.max_per_run   = max_per_run
        self.sleep_between = sleep_between
        self.block_n       = block_n
        self.long_sleep    = long_sleep
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

# ---------------------------------------------------------------------------
# CONFIG / DB
# ---------------------------------------------------------------------------
def load_config(path=CONFIG_PATH):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def load_db_config(path=DB_CONFIG_PATH):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def get_pg_engine(db_cfg):
    password = urllib.parse.quote_plus(db_cfg['password'])
    return create_engine(
        f"postgresql+psycopg2://{db_cfg['user']}:{password}@{db_cfg['host']}:{db_cfg['port']}/{db_cfg['dbname']}"
    )

# ---------------------------------------------------------------------------
# UNIVERSE — top N US equities by market cap from PostgreSQL
# ---------------------------------------------------------------------------
def fetch_top_us_equities(engine, top_n=TOP_N_BY_MKTCAP) -> tuple[list[str], pd.DataFrame]:
    query = text("""
        SELECT
            ric,
            ticker,
            full_name,
            instrument_class,
            primary_instrument_ric,
            as_mktcapcompanyusd,
            exchange_name,
            gics_sector_name
        FROM us_universe
        WHERE instrument_is_active_flag = 'true'
          AND primary_instrument_ric = ric
          AND instrument_class = 'ordinary'
          AND as_mktcapcompanyusd IS NOT NULL
        ORDER BY as_mktcapcompanyusd DESC NULLS LAST
        LIMIT :top_n
    """)
    with engine.connect() as conn:
        universe_df = pd.read_sql(query, conn, params={"top_n": top_n})
    rics = universe_df['ric'].tolist()
    logger.info(f"Fetched {len(rics)} RICs from us_universe (top {top_n} by market cap).")
    if VERBOSE:
        print(f"\nTop 10 by market cap:\n"
              f"{universe_df[['ric','ticker','full_name','as_mktcapcompanyusd']].head(10).to_string(index=False)}\n")
    return rics, universe_df

# ---------------------------------------------------------------------------
# DATE CHUNKING  (matches India script exactly)
# ---------------------------------------------------------------------------
def get_date_chunks(start_date, end_date, chunk_size_days=3):
    chunks = []
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_size_days - 1), end_date)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks

# ---------------------------------------------------------------------------
# NEWS FETCH — per RIC  (matches India script's fetch_news_for_stock pattern)
# ---------------------------------------------------------------------------
def fetch_news_for_stock(instrument, date_chunks, limiter, max_retries=3, retry_sleep=15,
                         last_sleep_time_holder=None):
    all_news = []
    for start_date, end_date in date_chunks:
        start_str = start_date.strftime('%Y-%m-%dT00:00:00')
        end_str   = end_date.strftime('%Y-%m-%dT23:59:59')
        limiter.check()

        # Global 5-min sleep logic
        if last_sleep_time_holder is not None:
            current_time = time.time()
            if current_time - last_sleep_time_holder[0] >= LONG_SLEEP_AFTER_MINUTES * 60:
                print(f"\n[SYSTEM] 5 minutes elapsed, sleeping for {LONG_SLEEP_DURATION} seconds...\n")
                logger.info(f"Sleeping for {LONG_SLEEP_DURATION} seconds after 5 minutes of execution.")
                time.sleep(LONG_SLEEP_DURATION)
                last_sleep_time_holder[0] = time.time()

        attempt = 0
        while attempt < max_retries:
            try:
                full_query = f"R:{instrument} AND Language:LEN"
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
                        print(f"Error fetching {instrument} ({start_str} to {end_str}): {e}"
                              f"\nRetrying in {retry_sleep}s... (Attempt {attempt}/{max_retries})")
                        time.sleep(retry_sleep)
                    else:
                        print(f"Skipping {instrument} ({start_str} to {end_str}) after {max_retries} retries.")
                else:
                    break
    if all_news:
        combined = pd.concat(all_news, ignore_index=True)
        combined = combined.drop_duplicates(subset='storyId')
        return combined
    return pd.DataFrame()

# ---------------------------------------------------------------------------
# NEWS FETCH — all RICs with progress resume
# ---------------------------------------------------------------------------
def fetch_news_for_multiple_stocks(rics, date_chunks, limiter, start_from_index=0,
                                   last_sleep_time_holder=None):
    all_news     = []
    last_saved_i = start_from_index - 1

    for i, ric in enumerate(tqdm(rics, desc="RICs")):
        if i < start_from_index:
            continue
        if limiter.request_count >= limiter.max_per_run:
            logger.info(f"Stopped at {limiter.request_count} requests (max per run reached). "
                        f"Last completed RIC index: {last_saved_i}")
            _save_progress(last_saved_i)
            break

        df = fetch_news_for_stock(ric, date_chunks, limiter,
                                  last_sleep_time_holder=last_sleep_time_holder)
        if not df.empty:
            if VERBOSE and "text" in df.columns:
                print(f"\n{'='*30}\nRIC: {ric}\n{'='*30}")
                print(df['text'].head(3).to_string(index=False))
                print(f"(Total headlines for {ric}: {len(df)})\n")
            all_news.append(df)

        last_saved_i = i

    if all_news:
        full_df = pd.concat(all_news, ignore_index=True)
        full_df = full_df.drop_duplicates(subset='storyId')
        return full_df, last_saved_i
    return pd.DataFrame(), last_saved_i

# ---------------------------------------------------------------------------
# PROGRESS FILE — stores "YYYY-MM-DD:ric_index"
# Resets automatically on a new calendar day.
# ---------------------------------------------------------------------------
def _save_progress(ric_index: int):
    today_str = datetime.today().strftime('%Y-%m-%d')
    with open(PROGRESS_FILE, 'w') as f:
        f.write(f"{today_str}:{ric_index}")

def _load_progress() -> int:
    """Returns the RIC index to resume from (0 = start fresh)."""
    try:
        with open(PROGRESS_FILE, 'r') as f:
            content = f.read().strip()
        saved_date, saved_index = content.split(':')
        if saved_date == datetime.today().strftime('%Y-%m-%d'):
            resume_from = int(saved_index) + 1
            logger.info(f"Resuming today's run from RIC index {resume_from}.")
            return resume_from
        else:
            logger.info("Progress file is from a previous day — starting fresh.")
            return 0
    except FileNotFoundError:
        return 0

# ---------------------------------------------------------------------------
# DB — table DDL and upsert
# ---------------------------------------------------------------------------
def create_us_news_table(engine):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        version_created TIMESTAMPTZ,
        heading         TEXT,
        story_id        TEXT PRIMARY KEY,
        source_code     TEXT,
        ric             TEXT,
        news_text       TEXT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    print(f"Table '{TABLE_NAME}' is ready (with PRIMARY KEY on story_id).")

def upsert_final_news(engine, table_name, final_news):
    REQUIRED_COLS = ['version_created', 'heading', 'story_id', 'source_code', 'ric', 'news_text']
    final_news = final_news.reindex(columns=REQUIRED_COLS, fill_value=None)
    final_news = final_news.where(pd.notnull(final_news), None)
    records = final_news.to_dict(orient='records')
    with engine.begin() as conn:
        for row in records:
            insert_stmt = f"""
            INSERT INTO {table_name} (version_created, heading, story_id, source_code, ric, news_text)
            VALUES (:version_created, :heading, :story_id, :source_code, :ric, :news_text)
            ON CONFLICT (story_id) DO UPDATE SET
                version_created = EXCLUDED.version_created,
                heading         = EXCLUDED.heading,
                source_code     = EXCLUDED.source_code,
                ric             = EXCLUDED.ric,
                news_text       = EXCLUDED.news_text;
            """
            conn.execute(text(insert_stmt), row)
    print(f"Upserted {len(records)} records to '{table_name}'.")

def convert_to_ist(final_news):
    if final_news['version_created'].dtype == 'O':
        final_news['version_created'] = pd.to_datetime(final_news['version_created'], utc=True, errors='coerce')
    else:
        final_news['version_created'] = final_news['version_created'].dt.tz_convert('UTC')
    ist = pytz.timezone('Asia/Kolkata')
    final_news['version_created'] = final_news['version_created'].dt.tz_convert(ist)
    return final_news

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
if __name__ == "__main__":

    config    = load_config(CONFIG_PATH)
    db_config = load_db_config(DB_CONFIG_PATH)

    ek.set_app_key(config['ekn']['jdn'])
    engine = get_pg_engine(db_config['wm_price_db'])
    create_us_news_table(engine)

    # --- Date window: yesterday 00:00 → today 23:59 ---
    today    = datetime.today()
    start_dt = (today - timedelta(days=LOOKBACK_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt   = today.replace(hour=23, minute=59, second=59)
    logger.info(f"Fetching US news | window: {start_dt} to {end_dt}")

    date_chunks = get_date_chunks(start_dt, end_dt, chunk_size_days=CHUNK_SIZE_DAYS)

    # --- Universe ---
    rics, universe_df = fetch_top_us_equities(engine, top_n=TOP_N_BY_MKTCAP)
    if not rics:
        logger.error("No RICs returned from us_universe. Exiting.")
        exit(1)

    # --- Progress resume ---
    start_from_index = _load_progress()

    # --- Rate limiter + shared 5-min timer ---
    limiter = SafeLimiter(MAX_REQUESTS_PER_RUN, SLEEP_BETWEEN_REQUESTS,
                          SLEEP_EVERY_N_REQUESTS, LONG_SLEEP_SECONDS)
    last_sleep_time_holder = [time.time()]

    # --- Fetch per-RIC news ---
    try:
        news_df_all, last_completed_index = fetch_news_for_multiple_stocks(
            rics=rics,
            date_chunks=date_chunks,
            limiter=limiter,
            start_from_index=start_from_index,
            last_sleep_time_holder=last_sleep_time_holder
        )
        if VERBOSE and not news_df_all.empty:
            print_step_output("Stock-wise News", news_df_all)
        logger.info(f"News fetch complete. {len(news_df_all)} records fetched.")
    except Exception as e:
        logger.error(f"Stopped execution due to: {e}")
        news_df_all = pd.DataFrame()
        last_completed_index = start_from_index - 1

    print(f"\nTotal API calls made: {API_CALL_COUNT}\n")

    if news_df_all.empty:
        logger.warning("No news fetched. Nothing to upsert.")
        exit(0)

    # --- Rename columns ---
    col_map = {
        'versionCreated': 'version_created',
        'text':           'heading',
        'storyId':        'story_id',
        'sourceCode':     'source_code',
        'RIC':            'ric'
    }
    news_df_all = news_df_all.rename(columns={k: v for k, v in col_map.items() if k in news_df_all.columns})

    if VERBOSE:
        print_step_output("Final Combined News", news_df_all)

    # --- Convert timestamps to IST ---
    news_df_all = convert_to_ist(news_df_all)

    # --- Upsert to PostgreSQL ---
    upsert_final_news(engine, TABLE_NAME, news_df_all)
    logger.info(f"Upsert complete. {len(news_df_all)} records updated.")

    # --- Save progress (only after successful upsert) ---
    _save_progress(last_completed_index)
    logger.info(f"Progress saved. Last completed RIC index: {last_completed_index} / {len(rics) - 1}")
