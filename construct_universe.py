import pandas as pd
import refinitiv.data as rd
from typing import Literal
from time import sleep
from pathlib import Path
from wm_utils.utils.db_connections import DbConnections
from wm_utils.utils.misc_utils import MiscUtils
from wm_utils.eikon_data.rd_session import RDSession
from wm_utils.eikon_data.api_call_wrapper import RDApiCallWrapper


class ConstructUniverse:

    @staticmethod
    def create_us_universe_table():
        engine = DbConnections.get_postgresql_connection()
        query = """
        CREATE TABLE IF NOT EXISTS us_universe (
            ric TEXT NOT NULL,
            quote_id BIGINT,
            instrument_id BIGINT,
            ticker TEXT,
            full_name TEXT,
            isin TEXT,
            primary_quote_ric TEXT,
            primary_instrument_ric TEXT,
            primary_issue_ric TEXT,
            business_description TEXT,
            gics_sector_name TEXT,
            gics_industry_group_name TEXT,
            gics_industry_name TEXT,
            gics_sub_industry_name TEXT,
            trbc_economic_sector_name TEXT,
            trbc_business_sector_name TEXT,
            trbc_industry_group_name TEXT,
            trbc_industry_name TEXT,
            trbc_activity_name TEXT,
            price_close TEXT,
            price_close_date DATE,
            instrument_type TEXT,
            asset_category_description TEXT,
            date_of_incorporation DATE,
            ipo_date DATE,
            company_market_cap_local_currency NUMERIC(30, 4),
            instrument_is_active_flag TEXT,
            exchange_name TEXT,
            as_dtsubjectname TEXT,
            as_exchangename TEXT,
            as_issueisin TEXT,
            as_gics TEXT,
            as_assetstate TEXT,
            as_businessentity TEXT,
            as_pi TEXT,
            as_searchallcategoryv3 TEXT,
            as_searchallcategoryv2 TEXT,
            as_searchallcategory TEXT,
            as_rcsrbc2012leaf TEXT,
            as_rcsassetcategoryleaf TEXT,
            as_rcscurrencyleaf TEXT,
            as_exdividenddate TEXT,
            as_cusip TEXT,
            as_sedol TEXT,
            as_mktcapcompanyusd NUMERIC(30, 4),
            as_rcsexchangecountryleaf TEXT,
            as_rcsexchangecountry TEXT,
            as_cincusip TEXT,
            as_search_category TEXT,
            instrument_class TEXT,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

            PRIMARY KEY (ric)
        );
        """
        with engine.connect() as conn:
            conn.execute(query)
            print("table 'us_universe' created successfully")
        return None

    # example AS queries:
    # starts with B and exchange is NASDAQ Stock Exchange Capital Market
    """
    rd.discovery.search(
        view = rd.discovery.Views.EQUITY_QUOTES,
        top = 10,
        filter = "(AssetState ne 'DC' and SearchAllCategoryv2 eq 'Equities' and (RCSExchangeCountry xeq 'G:6J' and ExchangeName xeq 'NASDAQ Stock Exchange Capital Market' and (RIC eq 'B*')))",
        select = "DTSubjectName,ExchangeName,RIC,IssueISIN,Gics,AssetState,BusinessEntity,PI,SearchAllCategoryv3,SearchAllCategoryv2,SearchAllCategory,RCSTRBC2012Leaf,RCSAssetCategoryLeaf,RCSCurrencyLeaf,ExDividendDate,CUSIP,CinCUSIP,SEDOL,MktCapCompanyUsd,RCSExchangeCountryLeaf,RCSExchangeCountry",
        order_by = "ExDividendDate"
    )
    """

    # starts with B or C, exchange is NASDAQ Stock Exchange Capital Market
    """
    rd.discovery.search(
        view = rd.discovery.Views.EQUITY_QUOTES,
        top = 10,
        filter = "(AssetState ne 'DC' and SearchAllCategoryv2 eq 'Equities' and (((RIC eq 'C*') or (RIC eq 'B*')) and RCSExchangeCountry xeq 'G:6J' and ExchangeName xeq 'NASDAQ Stock Exchange Capital Market'))",
        select = "DTSubjectName,ExchangeName,RIC,IssueISIN,Gics,AssetState,BusinessEntity,PI,SearchAllCategoryv3,SearchAllCategoryv2,SearchAllCategory,RCSTRBC2012Leaf,RCSAssetCategoryLeaf,RCSCurrencyLeaf,ExDividendDate,CUSIP,CinCUSIP,SEDOL,MktCapCompanyUsd,RCSExchangeCountryLeaf,RCSExchangeCountry",
        order_by = "ExDividendDate"
    )
    """

    # start with any except A
    """
    rd.discovery.search(
        view = rd.discovery.Views.EQUITY_QUOTES,
        top = 10,
        filter = "(AssetState ne 'DC' and SearchAllCategoryv2 eq 'Equities' and (RCSExchangeCountry xeq 'G:6J' and ExchangeName xeq 'NASDAQ Stock Exchange Capital Market' and (RIC eq 'b*' or RIC eq 'c*' or RIC eq 'd*' or RIC eq 'e*' or RIC eq 'f*' or RIC eq 'g*' or RIC eq 'h*' or RIC eq 'i*' or RIC eq 'j*' or RIC eq 'j*' or RIC eq 'k*' or RIC eq 'l*' or RIC eq 'm*' or RIC eq 'n*' or RIC eq 'o*' or RIC eq 'p*' or RIC eq 'q*' or RIC eq 'r*' or RIC eq 's*' or RIC eq 't*' or RIC eq 'u*' or RIC eq 'v*' or RIC eq 'w*' or RIC eq 'x*' or RIC eq 'y*' or RIC eq 'z*' or RIC eq '1*' or RIC eq '2*' or RIC eq '3*' or RIC eq '4*' or RIC eq '5*' or RIC eq '6*' or RIC eq '7*' or RIC eq '8*' or RIC eq '9*' or RIC eq '0*' or RIC eq '.*')))",
        select = "DTSubjectName,ExchangeName,RIC,IssueISIN,Gics,AssetState,BusinessEntity,PI,SearchAllCategoryv3,SearchAllCategoryv2,SearchAllCategory,RCSTRBC2012Leaf,RCSAssetCategoryLeaf,RCSCurrencyLeaf,ExDividendDate,CUSIP,CinCUSIP,SEDOL,RCSExchangeCountryLeaf,RCSExchangeCountry"
    )
    """

    # for ETFS:
    # exchange is NASDAQ Stock Exchange Capital Market
    # note that no country filter here
    # AS offers Domicile here but we dont filter it becuase 
    # a etf listed in a US exhchange can have different domicile than US
    # so we just filter by exchange and ric
    """
    rd.discovery.search(
        view = rd.discovery.Views.FUND_QUOTES,
        top = 10,
        filter = "
        (
            AssetState ne 'DC' and 
            SearchAllCategoryv2 eq 'Funds' and 
            (
                ExchangeName xeq 'NASDAQ Stock Exchange Capital Market' and 
                (RIC eq 'ZJ*' or RIC eq 'ZK*' or RIC eq 'ZL*' or RIC eq ' ZM*' or RIC eq 'ZN*' or RIC eq 'ZO*' or RIC eq 'ZP*' or RIC eq 'ZQ*' or RIC eq 'zr*' or RIC eq 'zs*' or RIC eq 'zt*' or RIC eq 'zu*' or RIC eq 'zv*' or RIC eq 'zw*' or RIC eq 'zx*' or RIC eq 'zy*' or RIC eq ' zz*')
            )
        )",
        select = "DTSubjectName,RIC,BusinessEntity,PI,SearchAllCategoryv3,SearchAllCategoryv2,SearchAllCategory,IssueISIN,IssueLipperGlobalSchemeName,RCSAssetCategoryLeaf,RCSIssuerDomicileCountryLeaf,RCSIssueCountryRegisteredForSale,RCSCurrencyLeaf,ExchangeName,iNAVRIC"
    )
    """

    _AS_TOP_LIMIT = 10_000

    _ALPHABET = list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')


    exchanges_to_characters_map_equities = {
        # only NASDAQ Stock Exchange Capital Market 
        # requires splitting by the chracters and querying each sepera
        # this is because NASDAQ SECM has more than 10,000 rics starting with A
        # about 20,000 rics in NASDAQ SECM start with A
        # so we split the query into two parts
        # first we query all the rics starting with A
        # then we query all the rics starting with B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z, .
        # futher:
            # in a AC has more then 10,000 rics
            # so we split into two parts:
        'NASDAQ Stock Exchange Capital Market' : [

            # AC has more then 10,000 rics, so split into two parts
            ['ACA', 'ACB', 'ACC', 'ACD', 'ACE', 'ACF', 'ACG', 'ACH', 'ACI', 'ACJ', 'ACK', 'ACL', 'ACM'], 
            ['ACN', 'ACO', 'ACP', 'ACQ', 'ACR', 'ACS', 'ACT', 'ACU', 'ACV', 'ACW', 'ACX', 'ACY', 'ACZ'],
            
            # AB has ~9000 rics, so call it separately
            ['AB'],

            # all other characters starting with A
            ['AA', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI', 'AJ', 'AK', 'AL', 'AM', 'AN'], 
            ['AO', 'AP', 'AQ', 'AR', 'AS', 'AT', 'AU', 'AV', 'AW', 'AX', 'AY', 'AZ'],

            # all except A
            [
                'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 
                'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '.'
            ],

            # numbers 0 to 9
            ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'],
        ],

        # all other exchanges do not require splitting by characters
        # they can be queried in one go
        'NASDAQ Stock Exchange Global Market': 'full',
        'NASDAQ Stock Exchange Global Select Market': 'full',
        'New York Stock Exchange': 'full',
        'NYSE American': 'full',
        'NYSE Arca': 'full',
        'NYSE Texas Inc': 'full',
        'Cboe BZX Exchange': 'full'
    }

    exchanges_to_characters_map_etfs = {
        'NASDAQ Stock Exchange Capital Market': [

            # C has 8118 ricsm so call it separately
            ['C'],
            # f has 9917 rics, so call it separately
            ['F'],
            # r has 4166, so call it separately
            # this can be merged later
            ['R'],

            # this set has 7916 rics
            ['A', 'B', 'D', 'E'],
            # this has 9607
            [ 'G', 'H', 'I', 'J'], 
            ['K', 'L', 'M'],
            ['N', 'O', 'P', 'Q'],
            ['S', 'T', 'U', 'V', 'W', 'X', 'Y', '.'],

            # Z has ~34071 rics
            # ZA has >=10000 rics, so call it separately
            # so we split into two parts
            ['ZAA', 'ZAB', 'ZAC', 'ZAD', 'ZAE', 'ZAF', 'ZAG', 'ZAH', 'ZAI', 'ZAJ', 'ZAK', 'ZAL', 'ZAM'], 
            ['ZAN', 'ZAO', 'ZAP', 'ZAQ', 'ZAR', 'ZAS', 'ZAT', 'ZAU', 'ZAV', 'ZAW', 'ZAX', 'ZAY', 'ZAZ'],

            # ZE has 7932
            ['ZE'],
            # ZI has 7863
            ['ZI'],

            # this set has 1628 rics
            [
                'ZB', 'ZC', 'ZD', 'ZF', 'ZG', 'ZH', 'ZJ', 'ZK', 'ZL', 'ZM', 'ZN', 
                'ZO', 'ZP', 'ZQ', 'ZR', 'ZS', 'ZT', 'ZU', 'ZV', 'ZW', 'ZX', 'ZY', 'ZZ'
            ],

            # numbers 0 to 9
            ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'],
        ],

        # all other exchanges do not require splitting by characters
        # they can be queried in one go
        'NASDAQ Stock Exchange Global Market': 'full',
        'NASDAQ Stock Exchange Global Select Market': 'full',
        'New York Stock Exchange': 'full',
        'NYSE American': 'full',
        'NYSE Arca': 'full',
        'NYSE Texas Inc': 'full',
        'Cboe BZX Exchange': 'full'
    }

    # in english:
    # asset state is not 'DC' (dead code)
    # search all category v2 is 'Equities'
    # rcs exchange country is 'G:6J' (US, this is intenal to refinitiv)
    # exchange name is the exchange name
    # ric starts with the start_with character
    # !TODO:
        # when we ingest delisted, we will need to make
        # the AssetState filter eq 'DC' (active code)

    @staticmethod
    def filter_str_with_ric_and_exchange(exchange, ric_eq_str):
        return \
            f"""
            (
                AssetState ne 'DC' and 
                SearchAllCategoryv2 eq 'Equities' and 
                (
                    RCSExchangeCountry xeq 'G:6J' and 
                    ExchangeName xeq '{exchange}' and 
                    ({ric_eq_str})
                )
            )
            """

    @staticmethod
    def filter_str_with_exchange(exchange):
        filter_str = \
            f"""
            (
                AssetState ne 'DC' and 
                SearchAllCategoryv2 eq 'Equities' and
                (
                    RCSExchangeCountry xeq 'G:6J' and 
                    ExchangeName xeq '{exchange}'
                )
            )
            """

        filter_str = " ".join(filter_str.split())

        return filter_str

    @staticmethod
    def filter_str_with_ric_and_exchange_for_etfs(exchange, ric_eq_str):
        return \
            f"""
            (
                AssetState ne 'DC' and 
                SearchAllCategoryv2 eq 'Funds' and
                (
                    ExchangeName xeq '{exchange}' and
                    ({ric_eq_str})
                )
            )
            """

    @staticmethod
    def filter_str_with_exchange_for_etfs(exchange):
        filter_str = \
            f"""
            (
                AssetState ne 'DC' and 
                SearchAllCategoryv2 eq 'Funds' and
                (
                    ExchangeName xeq '{exchange}'
                )
            )
            """
        
        filter_str = " ".join(filter_str.split())

        return filter_str


    _SELECT_FIELDS = "DTSubjectName,ExchangeName,RIC,IssueISIN,Gics,AssetState,BusinessEntity,PI,SearchAllCategoryv3,SearchAllCategoryv2,SearchAllCategory,RCSTRBC2012Leaf,RCSAssetCategoryLeaf,RCSCurrencyLeaf,ExDividendDate,CUSIP,CinCUSIP,SEDOL,MktCapCompanyUsd,RCSExchangeCountryLeaf,RCSExchangeCountry"
    _SELECT_FIELDS_FOR_ETFS = "DTSubjectName,RIC,BusinessEntity,PI,SearchAllCategoryv3,SearchAllCategoryv2,SearchAllCategory,IssueISIN,IssueLipperGlobalSchemeName,RCSAssetCategoryLeaf,RCSIssuerDomicileCountryLeaf,RCSIssueCountryRegisteredForSale,RCSCurrencyLeaf,ExchangeName,iNAVRIC"

    _segment_to_func_map = {
        'equities': {
            'exchange_only': filter_str_with_exchange,
            'exchange_and_ric': filter_str_with_ric_and_exchange,
            'char_map': exchanges_to_characters_map_equities,
            'view': rd.discovery.Views.EQUITY_QUOTES,
            'select_fields': _SELECT_FIELDS,
        },
        'etfs': {
            'exchange_only': filter_str_with_exchange_for_etfs,
            'exchange_and_ric': filter_str_with_ric_and_exchange_for_etfs,
            'char_map': exchanges_to_characters_map_etfs,
            'view': rd.discovery.Views.FUND_QUOTES,
            'select_fields': _SELECT_FIELDS_FOR_ETFS,
        },
    }

    # meta data fields
    _META_DATA_FIELDS = [
        
        'TR.QuoteID',
        'TR.InstrumentID',
        'TR.TickerSymbol',
        'TR.CompanyName',
        'TR.ISINCode',

        'TR.PrimaryQuote',
        'TR.PrimaryInstrument',
        'TR.PrimaryRIC',

        'TR.BusinessSummary',        

        'TR.GICSSector',
        'TR.GICSIndustryGroup',
        'TR.GICSIndustry',
        'TR.GICSSubIndustry',
    
        'TR.TRBCEconomicSector',
        'TR.TRBCBusinessSector',
        'TR.TRBCIndustryGroup',
        'TR.TRBCIndustry',
        'TR.TRBCActivity',

        'TR.PriceClose',
        'TR.PriceClose.Date',
        
        'TR.InstrumentType',
        'TR.AssetCategory',
    
        'TR.CompanyIncorpDate',
        'TR.IPODate',
        'TR.MarketCapLocalCurn',
    
        'TR.InstrumentIsActive',

        # this has been rasing error repeatedly
        # not sure why, so commenting it out for now
        # 'TR.IsDelistedQuote',
        #'TR.InstrumentTradingStatus',

        'TR.ExchangeName'
        
    ]

    _always_add_rics = ['AAPL.OQ']


    def __init__(
        self,
        write_path: str | Path = './_data',
    ):
        if isinstance(write_path, str):
            write_path = Path(write_path)

        assert isinstance(write_path, Path), "write_path must be a pathlib Path object"

        self.write_path = write_path

        self.timestamp = pd.Timestamp.now().strftime('%Y-%m-%d-%H-%M-%S')

        self.write_path = self.write_path / self.timestamp
        MiscUtils.safe_create_folder(self.write_path, raise_error_if_exists=True)

        self.logger = MiscUtils.setup_logger(
            name=f"construct_universe_{self.timestamp}",
            file_location=f'{self.write_path}/construct_universe_log_{self.timestamp}.log'
        )

        self.logger.info(f"writing to: {self.write_path}")
        
        self.number_of_as_calls = 0

        self.universe = {}


    def get_entire_exchange_universe(
        self,
        segment: Literal['equities', 'etfs'],
        exchange: str,
        rd_session: RDSession
    ):
        assert segment in ['equities', 'etfs'], \
        "segment must be either equities or etfs"
        assert isinstance(exchange, str), "exchange must be a string"

        self.number_of_as_calls += 1

        _segment = self._segment_to_func_map[segment]
        _func = _segment['exchange_only']

        df = rd_session.discovery.search(
            view = _segment['view'],
            top = self._AS_TOP_LIMIT,
            filter = _func(exchange),
            select = _segment['select_fields'],
        )

        if len(df) >= self._AS_TOP_LIMIT:
            _text = MiscUtils.stripped_dedent(
                f"""
                @srivatsa.rao
                more than {self._AS_TOP_LIMIT} rows returned for exchange: {exchange}
                exiting the code, manual character break down is needed
                """
            )
            MiscUtils.send_msg_via_slack(_text)
            self.logger.warning(_text)
            raise ValueError(_text)

        return df


    def get_exchange_universe_starting_with(
        self,
        segment: Literal['equities', 'etfs'],
        exchange: str,
        start_with: list[str],
        rd_session: RDSession
    ):
        assert segment in ['equities', 'etfs'], \
        "segment must be either equities or etfs"
        assert isinstance(exchange, str), "exchange must be a string"
        assert isinstance(start_with, list), "start_with must be a list"
        assert all(isinstance(item, str) for item in start_with), "start_with must be a list of strings"

        self.number_of_as_calls += 1

        ric_eq_str = ''

        for idx, _start_with in enumerate(start_with):
            # append the ric eq string
            ric_eq_str += f"RIC eq '{_start_with}*'"

            # append the or string
            # only if it is not the last item
            if idx < len(start_with) - 1:
                ric_eq_str += ' or '
        
        self.logger.info(f"ric eq str: {ric_eq_str}")

        _segment = self._segment_to_func_map[segment]
        _func = _segment['exchange_and_ric']

        df = rd_session.discovery.search(
            view = _segment['view'],
            top = self._AS_TOP_LIMIT,
            filter = _func(exchange, ric_eq_str),
            select = _segment['select_fields'],
        )

        if len(df) >= self._AS_TOP_LIMIT:
            _text = MiscUtils.stripped_dedent(
                f"""
                more than {self._AS_TOP_LIMIT} rows returned for exchange: {exchange}
                and starting with: {start_with}
                exiting the code, manual character break down is needed
                """
            )
            MiscUtils.send_msg_via_slack(_text)
            self.logger.warning(_text)
            raise ValueError(_text)

        return df

    def construct_universe(
        self,
        segment: Literal['equities', 'etfs'],
    ):
        assert segment in ['equities', 'etfs'], \
        "segment must be either equities or etfs"

        self.logger.info(f"constructing universe")
        self.logger.info(f"exchanges to characters map: \n{self._segment_to_func_map[segment]['char_map']}")

        _char_map = self._segment_to_func_map[segment]['char_map']

        collect_df = []

        with RDSession() as rd_session:

            for exchange, characters in _char_map.items():
                self.logger.info(f"constructing universe for exchange: {exchange}")
                
                # if entire universe can be queried in one go
                if characters == 'full':
                    self.logger.info(f"entire universe can be queried in one go")

                    # get the entire universe
                    df = self.get_entire_exchange_universe(segment, exchange, rd_session)

                else:
                    self.logger.info(f"entire universe cannot be queried in one go")

                    # initialize a list to collect the dataframes
                    _collect_df = []

                    # iterate over the characters and 
                    # get the universe for each character
                    for start_with in characters:
                        self.logger.info(f"constructing universe for character: {start_with}")

                        # get the universe for the character
                        df = self.get_exchange_universe_starting_with(segment, exchange, start_with, rd_session)

                        # append to the collect_df
                        _collect_df.append(df.copy())
                    
                    # concat the dataframes
                    df = pd.concat(
                        _collect_df, 
                        axis=0, 
                        ignore_index=True, 
                        join='outer'
                    )

                # write to csv
                df.to_csv(f'{self.write_path}/{segment}_{exchange}_universe.csv', index=False)

                # append to collect_df
                collect_df.append(df.copy())

        # concat the collect_df
        collect_df = pd.concat(
            collect_df, 
            axis=0, 
            ignore_index=True, 
            join='outer'
        )

        # write to csv
        collect_df.to_csv(f'{self.write_path}/{segment}_universe_all_exchanges.csv', index=False)

        self.universe[segment] = collect_df.copy()

        return collect_df

    
    def construct_all_segments_universe(self):
        for segment in ['etfs']:
            self.construct_universe(segment)
            self.get_meta_data_for_universe(segment)


    def get_meta_data_for_universe(
        self,
        segment: Literal['equities', 'etfs'],
    ):
        assert segment in ['equities', 'etfs'], \
        "segment must be either equities or etfs"

        # create a temp write path for the meta data
        temp_write_path = self.write_path / 'temp_meta_data'
        MiscUtils.safe_create_folder(temp_write_path, raise_error_if_exists=True)

        _uni = self.universe[segment]
        #_uni = pd.read_csv('./_data/2026-04-28-16-32-20/equities_universe_all_exchanges.csv')

        # get the list of rics
        rics = _uni['RIC'].tolist()

        # create chunks of 9000
        # get_data has max number of rows limit of 10000
        # so we create chunks of 9000
        # then we sleep for 300s (5mins) to avoid rate limiting
        rics_chunks = MiscUtils.create_chunks(rics, 1500)

        collect_df = []

        # open rd session
        with RDSession() as rd_session:

             # iterate through the chunks and get the meta data
            for idx, rics_chunk in enumerate(rics_chunks):
                self.logger.info(f"getting meta data for chunk {idx+1} of {len(rics_chunks)}")

                fields_collect = []

                for _fields in self._META_DATA_FIELDS:
                    self.logger.info(f"getting meta data for fields: {_fields}")

                    _rics = list(set(rics_chunk + self._always_add_rics))

                    meta_data = RDApiCallWrapper.rd_get_data(
                        rd_session=rd_session,
                        universe=_rics,
                        fields=[_fields]
                    )

                    # set index on Instrument (ric)
                    meta_data = meta_data.set_index('Instrument')

                    fields_collect.append(meta_data.copy())

                # concat on axis=1
                meta_data = pd.concat(
                    fields_collect, 
                    axis=1,
                    ignore_index=False,
                    join='outer'
                ).reset_index(drop=False)

                # write to csv
                meta_data.to_csv(f'{temp_write_path}/meta_data_chunk_{idx+1}.csv', index=False)

                # append to collect_df
                collect_df.append(meta_data.copy())

                sleep(2)

        # concat the collect_df
        collect_df = pd.concat(
            collect_df,
            axis=0,
            ignore_index=True,
            join='outer'
        )

        # write to csv
        fname = f'{self.write_path}/{segment}_universe_meta_data.csv'
        collect_df.to_csv(fname, index=True)

        self.logger.info(f"meta data saved to: {fname}")

        self.meta_data_df = collect_df.copy()

        return collect_df


if __name__ == '__main__':

    construct_universe = ConstructUniverse()
    construct_universe.construct_all_segments_universe()

                
