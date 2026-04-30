import os
import time
import random
import logging
from refinitiv.data import eikon as ek
import pandas as pd
import datetime
import certifi
import pymongo
import urllib.request, json
from os import path as pt
import numpy as np
from openpyxl import load_workbook
from mattermost_msg import send_msg_via_mm

path = r"G:\.shortcut-targets-by-id\0B7f_UMMZHM_JNXljMVVXY2VPcHM\Investment Research_N\Stock Meta\universe_stock_description.xlsx"

from typing import List, Union, Any
from pandas.tseries.offsets import BDay
import yaml

with open('../config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Anugrah's Eikon
ek.set_app_key(config['ekn']['jdn'])

today = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
today_10 = datetime.datetime.combine(datetime.date.today() - BDay(10), datetime.datetime.min.time())

data11 = ek.get_timeseries(['.NSEI'], start_date=today_10, end_date=today, fields='CLOSE', interval='daily', )

data11 = data11.reset_index()

# %%
today = data11.iloc[-2, 0]
today_1 = data11.iloc[-3, 0]
today_5 = data11.iloc[-7, 0]

if (today - today_1).days >= 3:
    print('Holdiday in last 1 day: ', str((today - today_1).days - 3))
else:
    print('Holdiday in last 1 day: ', str((today - today_1).days - 1))

# print('Holdiday in last 1 day: ',str((today-today_1).days-1))
print('Holdiday in last 5 days: ', str((today - today_5).days - 7))
# %%
dt0 = 'fo' + today.strftime('%d') + today.strftime('%b').upper() + today.strftime('%Y') + 'bhav.csv'
dt1 = 'fo' + (today_1).strftime('%d') + (today_1).strftime('%b').upper() + (today_1).strftime('%Y') + 'bhav.csv'
dt5 = 'fo' + (today_5).strftime('%d') + (today_5).strftime('%b').upper() + (today_5).strftime('%Y') + 'bhav.csv'

#
# upass = urllib.parse.quote_plus(config['wm_dev']['password'])
# client = pymongo.MongoClient(
#     f'mongodb://{config["wm_dev"]["user"]}:{upass}@{config["wm_dev"]["host"]}:{config["wm_dev"]["port"]}/?ssl=true'
#     f'&ssl_ca_certs=C:\smallcase\Automatically\\rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference'
#     f'=secondaryPreferred&retryWrites=false')
# CLIENT_TYPE = "DEV"

upass = urllib.parse.quote_plus(config['wm_prod']['password'])
client = pymongo.MongoClient(
    f'mongodb://{config["wm_prod"]["user"]}:{upass}@{config["wm_prod"]["host"]}:{config["wm_prod"]["port"]}/?ssl=true'
    f'&ssl_ca_certs=C:\smallcase\Automatically\\global-bundle.pem&replicaSet=rs0&readPreference'
    f'=secondaryPreferred&retryWrites=false')
CLIENT_TYPE = "PROD"

run_date = datetime.datetime.today().date()


def fetch_universe_from_db() -> pd.DataFrame:
    db = client.wm_research
    collection = db.universe
    data = collection.find({})
    l = []
    for i in data:
        l.append(i)

    df = pd.DataFrame.from_dict(l, orient='columns')
    # df.loc[:,"eventid_sc"]=df.loc[:,"_id"]
    return df


# Setting up logger

logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger('NSE_Sheet')
logger.setLevel(logging.INFO)


def find_new_stocks(universe_list: list):
    """A utility function to find new stocks listed on the exchange
        :parameter
        exchanges : str
            exchange MIC codes. NSE: XNSE and BSE: XBOM
        :returns
        List[str]
            a list of new scrips added on the bourse
    """
    # logger.info(f"Fetching Data for: {exchange}")
    # logger.info(f"Reading Data from: {exchange}.csv")

    # df = pd.read_csv(f'../Client/Data/{exchange}.csv', header=None)
    old_stocks = universe_list
    logger.info(f"{len(old_stocks)} RICS fetched from db")
    etf_list = pd.read_excel('etf_report.xlsx', sheet_name='Sample')
    etf_list = etf_list['RIC Code'].astype(str).to_list()
    etf_list = [x for x in etf_list if x != 'NULL']
    syntax = f'SCREEN(U(IN(Equity(active or inactive,public,primary))/*UNV:Public*/), IN(TR.ExchangeCountryCode,"IN"), IN(TR.ExchangeMarketIdCode,"XNSE" ,"XBOM"), NOT_IN(TR.InstrumentTypeCode,"CEF","OPF"),  CURN=INR)'
    # syntax = f'SCREEN(U(IN(Equity(active or inactive,public,primary))/*UNV:Public*/), IN(TR.ExchangeCountryCode,"IN"), IN(TR.InstrumentTypeCode,"FULLPAID","ORD","UNT","ETF","ETFB","ETFC","ETFE","ETFM"), IN(TR.ExchangeMarketIdCode,"XNSE"),NOT_IN(TR.InstrumentTypeCode,"CEF"),  CURN=INR)'
    logger.debug(f"Screener used for API call: {syntax}")
    df, e = ek.get_data(syntax, ['TR.Instrument', 'TR.AssetCategory', 'TR.InstrumentType'])
    complete_list = df['Instrument'].to_list() + etf_list
    logger.info(f"{len(df)} RICS fetched from Eikon")
    l: List[str] = (list(set(complete_list) - set(old_stocks)))
    logger.info(f"Length of new stock list returned:  {len(l)}")
    
    filtered = [
    t for t in l
    if t.endswith((".BO", ".NS")) and len(t.split(".")[0]) <= 9
]
    
    # df_append = pd.DataFrame(data=l)
    # df = pd.read_csv(f'../Client/Data/{exchange}.csv', header=None)
    # df = df.append(df_append, ignore_index=True)
    df.to_csv(f'../Client/Data/universe_screener_query_output.csv', index=False, header=None)
    print("*******")
    print(filtered)
    print("*******")
    return filtered


def calculate_avg_liquidity(scrips: List[str]) -> List[float]:  # vectorize this operation, makes too many api calls
    liquidity: List[float] = []
    # if is_empty(scrips):
    #     # logger.info("Empty list received. Returning empty List")

    # else:
    for company in scrips:
        logger.info(f"Calculating liquidity for {company}")
        turnover, e = ek.get_data(f'{company}', ['TR.TURNOVER(SDate=0,EDate=-89,Frq=D)'])
        liquidity.append((pow(10, 6) / turnover['Turnover'].mean()) * 100)
        logger.info(f"Liquidity data appended for {company}")
    return liquidity


def etf_liquidity_update():
    db = client.wm_research
    collection = db.universe
    data = collection.find({"$and": [{"is_active": {"$gt": 0}}, {"instrument_class": "etf"}]})
    l = []
    for i in data:
        l.append(i)

    df = pd.DataFrame.from_dict(l, orient='columns')
    # df = pd.read_csv('../Client/Data/ETF_static_data.csv')
    df['Liquidity (%)'] = calculate_avg_liquidity(df['RIC'].to_list())
    # df['Liquidity_label'] = calculate_avg_liquidity(df['RIC'].to_list())
    for idx, row in df.iterrows():
        row.fillna('', inplace=True)
        # print(row)
        collection.update_one({"_id": f"{row['RIC']}"}, {"$set":
                                                             {"updated_on": pd.to_datetime(run_date),
                                                              "liquidity_%": row['Liquidity (%)']
                                                              }})
    # new_df = get_etf_df(df['RIC'].to_list())
    # df.to_csv('ETF_static_data.csv')
    # new_etf = find_new_etfs()
    # new_df = get_etf_df(new_etf)


def update_noise(l: List[str], chunk_size=128):
    # df, e = ek.get_data(l, ['TR.ISIN'])
    noise = []
    for idx in range(len(l)):
        if idx % 2:
            noise.append(random.uniform(0.960000, 0.989999))
        else:
            noise.append(random.uniform(1.011000, 1.040000))
    noise_df = pd.DataFrame(zip(l, noise), columns = ['RIC', 'noise'])
    # return noise
    noise_df.to_csv('../Client/Data/noise_new.csv', index=False)


var_list = ["TRD_STATUS",
            "TR.CompanyName",
            "TR.BusinessSummary",
            "TR.ISIN",
            "TR.TickerSymbol",
            "TR.GICSSector",
            "TR.GICSIndustryGroup",
            "TR.GICSIndustry",
            "TR.GICSSubIndustry",
            "TR.FUNDTRACKINGERROR1YEAR",
            "TR.FundTotalNetAssets/10^7",
            "TR.PriceClose",
            "TR.PRICECLOSE.DATE",
            "TR.InstrumentType",
            "TR.CompanyIncorpDate",
            "TR.IPODate",
            "TR.MarketCapLocalCurn"]


def instrument_class(x):
    if  pd.isna(x):
        return str('unknown')
    if x.lower() in ["bond etfs", "commodity etfs", "equity etfs", "money market etfs"]:
        return str("etf")
    elif x.lower() in ["convertible preference shares", "differential voting rights sha", "partly paid ordinary shares",
                       "preference shares", "rights"]:
        return "special"
    elif x.lower() in ["fully paid ordinary shares", "ordinary shares"]:
        return "ordinary"
    elif x.lower() in ["unit"]:
        return "trust"
    else:
        return str('unknown')


def push_to_universe_db(df: pd.DataFrame):
    ### event id is _id
    db = client.wm_research
    collection = db.universe
    # for idx, row in df.iterrows():
    #     if pd.isna(df.at[idx, 'price_close_date']):
    #         df.at[idx, 'price_close_date']  = row['updated_on']
    #     if (df.at[idx, 'company_incorp_date']).isnull():
    #         df.at[idx, 'company_incorp_date']  = row['updated_on']
    #     if (df.at[idx, 'ipo_date']).isnull():
    #         df.at[idx, 'ipo_date']  = row['updated_on']
    #     if (df.at[idx, 'created_on']).isnull():
    #         df.at[idx, 'created_on']  = row['updated_on']
    #     if (df.at[idx, 'company_incorp_date']).isnull():
    #         df.at[idx, 'updated_on']  = row['updated_on']
    df['price_close_date'] = df['price_close_date'].fillna(run_date)
    df['company_incorp_date'] = df['company_incorp_date'].fillna(run_date)
    df['ipo_date'] = df['ipo_date'].fillna(run_date)
    df['created_on'] = df['created_on'].fillna(run_date)
    df['updated_on'] = df['updated_on'].fillna(run_date)

    df.to_csv('out.csv')

    # df['price_close_date'] = pd.to_datetime(df['price_close_date']).dt.date
    # df['company_incorp_date'] = pd.to_datetime(df['company_incorp_date']).dt.date
    # df['ipo_date'] = pd.to_datetime(df['ipo_date']).dt.date
    # df['created_on'] = pd.to_datetime(df['created_on']).dt.date
    # df['updated_on'] = pd.to_datetime(df['updated_on']).dt.date
    for col in df.select_dtypes(include=['datetime']):
        df[col] = df[col].dt.strftime('%Y-%m-%Yd')
    for idx, row in df.iterrows():
        # hardcoded edge case fix for GAUD.NS
        # we are getting two ISINs for GAUD.NS
        if row['RIC'] == 'GAUD.NS':
            if row['ISIN'] == 'INE0P8B01020':
                continue
            elif row['ISIN'] == 'INE0P8B01012':
                row['ISIN'] = 'INE0P8B01020'
        print(row['_id'])
        collection.update_one({'_id': f"{row['_id']}"},
            {'$set':{"_id": f"{row['_id']}",
             "RIC": f"{row['RIC']}",
             "is_active": row['is_active'],
             "Full Name": f"{row['Full Name']}",
             "Business Description_eikon": f"{row['Business Description_eikon']}",
             "ISIN": f"{row['ISIN']}",
             "Ticker Symbol": f"{row['Ticker Symbol']}",
             "GICS Sector_eikon": f"{row['GICS Sector_eikon']}",
             "GICS Industry Group_eikon": f"{row['GICS Industry Group_eikon']}",
             "GICS Industry_eikon": f"{row['GICS Industry_eikon']}",
             "GICS Sub industry_eikon": f"{row['GICS Sub industry_eikon']}",
             "fund_tracking_error_1year": row['fund_tracking_error_1year'],
             "fund_total_net_assets": row['fund_total_net_assets'],
             "price_close": row['price_close'],
             "price_close_date": f"{pd.to_datetime(row['price_close_date'])}",
             "instrument_type": f"{row['instrument_type']}",
             "company_incorp_date": f"{pd.to_datetime(row['company_incorp_date'])}",
             "ipo_date": f"{pd.to_datetime(row['ipo_date'])}",
             "Business Description": f"{row['Business Description']}",
             "Smallcase Prop Indicator": f"{row['Smallcase Prop Indicator']}",
             "instrument_class": f"{row['instrument_class']}",
             "created_on": pd.to_datetime(row['created_on']),
             "updated_on": pd.to_datetime(row['updated_on']),
             "GICS Sector": f"{row['GICS Sector']}",
             "GICS Industry": f"{row['GICS Industry']}",
             "noise": row['noise'],
             "market_cap": row['market_cap']
             }}, upsert=True)


def remove_from_universe_db(df: pd.DataFrame):
    ### event id is _id
    db = client.wm_research
    collection = db.universe
    for idx, row in df.iterrows():
        collection.delete_one({"_id": f"{row['_id']}"})


db_format = ['_id', 'RIC', 'is_active', 'Full Name', 'Business Description_eikon',
             'ISIN', 'Ticker Symbol', 'GICS Sector_eikon',
             'GICS Industry Group_eikon', 'GICS Industry_eikon',
             'GICS Sub industry_eikon', 'fund_tracking_error_1year',
             'fund_total_net_assets', 'price_close', 'price_close_date',
             'instrument_type', 'company_incorp_date', 'ipo_date',
             'Business Description', 'Smallcase Prop Indicator', 'instrument_class',
             'created_on', 'updated_on', 'GICS Sector', 'GICS Industry', 'noise', 'market_cap']


def update_universe_db(scrips: List[str]):  # can expand this funtion to cover other eikon fields too
    scrips = [x for x in scrips if x != 'None' or x != 'NAN' or x != 'NULL']
    count = int(len(scrips) / 700)
    offset = len(scrips) % 700
    print(offset)
    i = 0
    j = 0
    df = pd.DataFrame()
    while j != len(scrips):
        logger.info(f'Current Epoch: {count}')
        if count == 0:
            j = j + offset
        else:
            j = i + 700
        tmp, err = ek.get_data(
            instruments=scrips[i:j],
            fields=[
                'TR.RIC',
                'TRD_STATUS',
                'TR.PriceClose',
                'TR.CompanyName',
                'TR.PRICECLOSE.DATE',
                'TR.InstrumentType',
                "TR.BusinessSummary",
                "TR.GICSSector",
                "TR.GICSIndustryGroup",
                "TR.GICSIndustry",
                "TR.GICSSubIndustry",
                "TR.ISIN",
                "TR.TickerSymbol",
                'CF_EXCHNG',
                'TR.MarketCapLocalCurn'
            ]
        )
        print(tmp.columns)
        df = df.append(tmp)
        i = j
        count -= 1

    print(df.columns)
    df = df[df['Date'].notna()]
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    df['trading_active'] = df.apply(
        lambda row: 1 if row['Date'] > (datetime.date.today() - datetime.timedelta(7)) else 0, axis=1)
    df['ric_active'] = df.apply(lambda row: 1 if row['Instrument'] == row['RIC'] else 0, axis=1)
    df['status_active'] = df['TRD_STATUS'].apply(lambda x: 0 if str(x).strip() == "S" else 1)
    df['is_active'] = df.apply(
        lambda row: 1 if row['trading_active'] == 1 and row['ric_active'] == 1 and row['status_active'] == 1 else 0,
        axis=1)
    df.drop(columns=['RIC'], inplace=True)
    df = df.rename({'Instrument': 'RIC',
                    'Price Close': 'price_close',
                    'Date': 'price_close_date', 'Instrument Type': 'instrument_type',
                    'Business Description': 'Business Description_eikon',
                    'GICS Sector Name': 'GICS Sector_eikon',
                    'GICS Industry Group Name': 'GICS Industry Group_eikon',
                    'GICS Industry Name': 'GICS Industry_eikon',
                    'GICS Sub-Industry Name': 'GICS Sub industry_eikon',
                    'Company Market Cap': 'market_cap'
                    }, axis='columns')
    df.to_csv('out.csv')
    df['updated_on'] = pd.to_datetime(run_date)
    df['price_close_date'] = pd.to_datetime(df['price_close_date'])
    df['instrument_class'] = df['instrument_type'].apply(instrument_class)
    df['exchange'] = df.apply(lambda row: "NSE" if str(row['RIC']).__contains__(".NS") == True else "BSE", axis=1)

    # df = df.replace({np.nan: None})
    db = client.wm_research
    collection = db.universe
    for idx, row in df.iterrows():
        row.fillna('', inplace=True)
        # hardcoded edge case fix for GAUD.NS
        # we are getting two ISINs for GAUD.NS
        if row['RIC'] == 'GAUD.NS':
            if row['ISIN'] == 'INE0P8B01020':
                continue
            elif row['ISIN'] == 'INE0P8B01012':
                row['ISIN'] = 'INE0P8B01020'
        # print(row)
        collection.update_one({"_id": f"{row['RIC']}"}, {"$set":
                                                             {"updated_on": row['updated_on'],
                                                              "is_active": row['is_active'],
                                                              "Full Name": f"{row['Company Name']}",
                                                              "price_close": row['price_close'],
                                                              "price_close_date": row['price_close_date'],
                                                              "instrument_class": row['instrument_class'],
                                                              "exchange": row['exchange'],
                                                              "Business Description_eikon": row[
                                                                  'Business Description_eikon'],
                                                              "GICS Sector_eikon": row['GICS Sector_eikon'],
                                                              "GICS Industry Group_eikon": row[
                                                                  'GICS Industry Group_eikon'],
                                                              "GICS Industry_eikon": row['GICS Industry_eikon'],
                                                              "GICS Sub industry_eikon": row['GICS Sub industry_eikon'],
                                                              "ISIN": row['ISIN'],
                                                              "Ticker Symbol": row['Ticker Symbol'],
                                                              "market_cap": row['market_cap']
                                                              }})


def update_universe_db_business_desc(df: pd.DataFrame):
    ### event id is _id
    db = client.wm_research
    collection = db.universe
    for idx, row in df.iterrows():
        collection.update_one({"_id": f"{row['RIC']}"}, {"$set":
                                                             {"updated_on": pd.to_datetime(run_date),
                                                              "Smallcase Prop Indicator": f"{row['Smallcase Prop Indicator']}",
                                                              "Business Description": f"{row['Business Description']}",
                                                              "GICS Sector": f"{row['GICS Sector']}",
                                                              "GICS Industry": f"{row['GICS Industry']}",
                                                              "ai_generated": 0
                                                              }})


def update_etf_mktcap():
    df = pd.read_excel(r'C:\Users\windmill\Downloads\ETF_marketcap labels.xlsx', sheet_name='securities_universe_2023_10_23')
    db = client.wm_research
    collection = db.universe
    for idx, row  in df.iterrows():
        filter = {'_id': f'{row["RIC"]}'}
        data = {
            '$set':{
                "mkt_cap_label": f"{row['marketcap_category']}"
            }
        }
        result = collection.update_one(filter, data)

        # Print the number of documents updated
        print(result.modified_count, "documents updated.")


def main():
    
    send_msg_via_mm('initiating Universe_sheet.py job ')


    current_universe = fetch_universe_from_db()
    ric_list = current_universe["RIC"].to_list()
    new_stocks = find_new_stocks(ric_list)


    # new_stocks = ['LICI.NS','ICIW.NS','GOWW.NS','MRAI.NS','GROL.NS','ANGY.NS','NIPB.NS','GROO.NS','IICM.NS','GROD.NS','MRAD.NS','KTAK.NS','GNOW.NS','NIPR.NS','GROB.NS','SBIQ.NS','ZERN.NS','GROX.NS','NIPU.NS','EDET.NS','ANGG.NS','ZERI.NS']    
    # new_stocks = ['LICI.NS','GROT.NS','GROG.NS','ZERS.NS','SBIB.NS','KOTP.NS','ANGF.NS','ONER.NS']
    # new_stocks=["SBIS.BO", "KTMF.BO", "NIPI.BO", "SBIE.BO", "IFCS.BO", "SBIT.BO", "NIPN.BO"]
    # new_stocks = ["GAUD.NS"]
    # "ICCP.NS", "AXIP.NS", "MIRF.NS", "MIRY.NS", "UTIV.NS"]
    # new_stocks = ["ATYA.NS", "ADID.NS", "BAJN.NS", "BAJF.NS", "BAJI.NS", "BANP.BO", "BARS.NS", "BARY.NS", "DSPE.NS", "DSPX.NS", "DSPH.NS", "EDEI.NS", "EDES.NS", "HDFK.NS", "ICBS.NS", "IIPR.NS", "LICI.NS", "MRAA.NS", "MIRW.NS", "MRAE.NS", "MIRU.NS", "MIRM.NS", "MOTT.NS", "SBII.NS", "SBIY.NS", "SBIV.NS", "SHRQ.NS", "TATG.NS", "TATR.NS", "UTIX.NS", "UTIE.NS", "UTIT.NS", "UTIF.NS", "ZERD.NS", "ZERH.NS", "ZERO.NS", "ZERA.NS"]

    if len(new_stocks):
        df, e = ek.get_data(new_stocks, var_list)
        print(var_list)
        print(new_stocks)
        print(df)
        print(e)
        df['is_active'] = df['TRD_STATUS'].apply(lambda x: 0 if str(x).strip() == "S" else 1)
        df = df.rename({'Instrument': 'RIC', 'Company Name': 'Full Name',
                        'Business Description': 'Business Description_eikon',
                        'GICS Sector Name': 'GICS Sector_eikon',
                        'GICS Industry Group Name': 'GICS Industry Group_eikon',
                        'GICS Industry Name': 'GICS Industry_eikon',
                        'GICS Sub-Industry Name': 'GICS Sub industry_eikon',
                        'Tracking Error for 1 Year to Last Month End': 'fund_tracking_error_1year',
                        'TR.FUNDTOTALNETASSETS/10^7': 'fund_total_net_assets',
                        'Price Close': 'price_close',
                        'Date': 'price_close_date',
                        'Instrument Type': 'instrument_type',
                        'IPO Date': 'ipo_date',
                        'Date of Incorporation': 'company_incorp_date',
                        'Company Market Cap': 'market_cap'}, axis='columns')
        df['created_on'] = run_date
        df['updated_on'] = run_date
        df['Business Description'] = "-"
        df['Smallcase Prop Indicator'] = "-"
        df['GICS Sector'] = "-"
        df['GICS Industry'] = "-"
        # df['noise'] = update_noise(df)
        df['noise'] = 0
        df['instrument_class'] = df['instrument_type'].apply(instrument_class)
        df['_id'] = df['RIC']
        df_db = df.loc[:, db_format]

        df_db = df_db.replace({np.nan: None})
        push_to_universe_db(df_db)
        # remove_from_universe_db(df_db)
    
    #make updates to existing universe
    current_universe_upd = fetch_universe_from_db()
    ric_list = current_universe_upd["RIC"].to_list()
    #---dfunct now---#
    update_noise(ric_list)
    #----------------#
    ek.set_app_key(config['ekn']['jdn'])
    update_universe_db(ric_list)
    print('*******DB UPDATE DONE********')
    try:
        time.sleep(5)
        ek.set_app_key(config['ekn']['jdn'])
        update_universe_db(ric_list)
    except:
        try:
            print("update attempt 2")
            time.sleep(5)
            ek.set_app_key(config['ekn']['jdn'])
            update_universe_db(ric_list)
        except:
            print('universe update part failed')

            send_msg_via_mm('universe update part failed ')
    # # etf_liquidity_update()
    logger.info('universe update done !!!')
    update = pd.read_excel(path, sheet_name='update')
    if len(update):
        print('*******UPDATE*********')
        print(update)
        update_universe_db_business_desc(update)
    current_universe_upd = fetch_universe_from_db()

    ### accomodate nan/none/NaT/NaN
    prop_Sec_blank = current_universe_upd.loc[:, "Smallcase Prop Indicator"] == "-"
    business_desc_blank = current_universe_upd.loc[:, "Business Description"] == "-"
    ai_business_desc = current_universe_upd.loc[:, "ai_generated"] == 1

    missing_desc = current_universe_upd.loc[prop_Sec_blank | business_desc_blank | ai_business_desc, ['RIC', 'Business Description_eikon',
                                                                                   'GICS Sector_eikon',
                                                                                   'GICS Industry Group_eikon',
                                                                                   'GICS Industry_eikon',
                                                                                   'GICS Sub industry_eikon',
                                                                                   'Business Description',
                                                                                   'Smallcase Prop Indicator',
                                                                                   'instrument_class', 'created_on',
                                                                                   'GICS Sector', 'GICS Industry',
                                                                                   'is_active', 'exchange']]

    active_blank = missing_desc.loc[:, "is_active"] == 1
    active_blank_nse = missing_desc.loc[:, "exchange"] == "NSE"
    missing_desc = missing_desc.loc[active_blank & active_blank_nse, :]

    all_universe = current_universe_upd.loc[:, ['RIC', 'Business Description_eikon',
                                                'GICS Sector_eikon', 'GICS Industry Group_eikon', 'GICS Industry_eikon',
                                                'GICS Sub industry_eikon', 'Business Description',
                                                'Smallcase Prop Indicator',
                                                'instrument_class', 'created_on', 'GICS Sector', 'GICS Industry',
                                                'is_active', 'exchange']]
    missing_desc = missing_desc.sort_values("created_on", ascending=False)
    all_universe = all_universe.sort_values("created_on", ascending=False)
    book = load_workbook(path)
    writer = pd.ExcelWriter(path, engine='openpyxl')
    writer.book = book
    # change update sheet; put universe sheet in
    # stock meta folder
    # missing_desc.to_excel(writer, sheet_name='missing_latest', index=False)
    missing_desc.to_csv(r'G:\.shortcut-targets-by-id\0B7f_UMMZHM_JNXljMVVXY2VPcHM\Investment Research_N\Stock Meta\uni_desc_manu.csv')
    # all_universe.to_excel(writer, sheet_name='all_latest', index=False)
    writer.save()
    writer.close()


if __name__ == '__main__':
    # update_etf_mktcap()

    # exchanges: List[str] = ['XBOM', 'XNSE']
    # create_etf_df()

    count = 0

    while True:
        try:
            main()
            logging.info("Universe_sheet.py run complete !!!")
            send_msg_via_mm('Universe_sheet.py job complete')
            break
        except:
            logging.info("First run failed. Retrying")
            main()
            count += 1
            if count == 4:
                send_msg_via_mm('Universe_sheet.py job failed!!! Rerun manually...')
                break
            break
