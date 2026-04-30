import pandas as pd
from pathlib import Path
from wm_utils.eikon_data.rd_session import RDSession
from wm_utils.utils.misc_utils import MiscUtils


class USUniverseIngestion:

    # starts with AA
    """
    rd.discovery.search(
        view = rd.discovery.Views.EQUITY_QUOTES,
        top = 10,
        filter = "(AssetState ne 'DC' and SearchAllCategoryv2 eq 'Equities' and (RCSExchangeCountry xeq 'G:6J' and (RIC eq 'aa*')))",
        select = "DTSubjectName,ExchangeName,RIC,IssueISIN,Gics,AssetState,BusinessEntity,PI,SearchAllCategoryv3,SearchAllCategoryv2,SearchAllCategory,RCSTRBC2012Leaf,RCSAssetCategoryLeaf,RCSCurrencyLeaf,ExDividendDate,CUSIP,CinCUSIP,SEDOL,RCSExchangeCountryLeaf,RCSExchangeCountry",
    )
    """

    # starts with b
    """
    rd.discovery.search(
        view = rd.discovery.Views.EQUITY_QUOTES,
        top = 10,
        filter = "(AssetState ne 'DC' and SearchAllCategoryv2 eq 'Equities' and (RCSExchangeCountry xeq 'G:6J' and (RIC eq 'b*')))",
        select = "DTSubjectName,ExchangeName,RIC,IssueISIN,Gics,AssetState,BusinessEntity,PI,SearchAllCategoryv3,SearchAllCategoryv2,SearchAllCategory,RCSTRBC2012Leaf,RCSAssetCategoryLeaf,RCSCurrencyLeaf,ExDividendDate,CUSIP,CinCUSIP,SEDOL,RCSExchangeCountryLeaf,RCSExchangeCountry"
    )
    """
    
    # starts with C
    """
    rd.discovery.search(
        view = rd.discovery.Views.EQUITY_QUOTES,
        top = 10,
        filter = "(AssetState ne 'DC' and SearchAllCategoryv2 eq 'Equities' and (RCSExchangeCountry xeq 'G:6J' and (RIC eq 'C*')))",
        select = "DTSubjectName,ExchangeName,RIC,IssueISIN,Gics,AssetState,BusinessEntity,PI,SearchAllCategoryv3,SearchAllCategoryv2,SearchAllCategory,RCSTRBC2012Leaf,RCSAssetCategoryLeaf,RCSCurrencyLeaf,ExDividendDate,CUSIP,CinCUSIP,SEDOL,RCSExchangeCountryLeaf,RCSExchangeCountry"
    )
    """

    # start with 4
    """
    rd.discovery.search(
        view = rd.discovery.Views.EQUITY_QUOTES,
        top = 10,
        filter = "(AssetState ne 'DC' and SearchAllCategoryv2 eq 'Equities' and (RCSExchangeCountry xeq 'G:6J' and (RIC eq '4*')))",
        select = "DTSubjectName,ExchangeName,RIC,IssueISIN,Gics,AssetState,BusinessEntity,PI,SearchAllCategoryv3,SearchAllCategoryv2,SearchAllCategory,RCSTRBC2012Leaf,RCSAssetCategoryLeaf,RCSCurrencyLeaf,ExDividendDate,CUSIP,CinCUSIP,SEDOL,RCSExchangeCountryLeaf,RCSExchangeCountry"
    )
    """

    _AS_TOP_LIMIT = 10_000

    _ALPHABET = list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')

    @staticmethod
    def get_us_universe(
        start_with: str
    ):
        assert isinstance(start_with, str), "start_with must be a string"
        assert len(start_with) <= 2, "start_with must be less than or equal to 2 characters"
        assert start_with.isalpha() or start_with.isdigit(), "start_with must be a letter or a digit"

        with RDSession() as rd:
            df = rd.discovery.search(
                view = rd.discovery.Views.EQUITY_QUOTES,
                top = USUniverseIngestion._AS_TOP_LIMIT,
                filter = f"(AssetState ne 'DC' and SearchAllCategoryv2 eq 'Equities' and (RCSExchangeCountry xeq 'G:6J' and (RIC eq '{start_with}*')))",
                select = "DTSubjectName,ExchangeName,RIC,IssueISIN,Gics,AssetState,BusinessEntity,PI,SearchAllCategoryv3,SearchAllCategoryv2,SearchAllCategory,RCSTRBC2012Leaf,RCSAssetCategoryLeaf,RCSCurrencyLeaf,ExDividendDate,CUSIP,CinCUSIP,SEDOL,RCSExchangeCountryLeaf,RCSExchangeCountry"
            )

            return df

    

    @staticmethod
    def create_combination_of_start_with(
        write_path: str | Path = './_data/'
    ):
        if isinstance(write_path, str):
            write_path = Path(write_path)

        assert isinstance(write_path, Path), "write_path must be a pathlib Path object"

        timestamp = pd.Timestamp.now().strftime('%Y-%m-%d-%H-%M-%S')

        write_path = write_path / timestamp
        MiscUtils.safe_create_folder(write_path, raise_error_if_exists=True)

        # setup logger
        logger = MiscUtils.setup_logger(
            name=f"create_combination_of_start_with_{timestamp}",
            file_location=f'{write_path}/create_combination_of_start_with_log_{timestamp}.log'
        )

        logger.info(f"writing to: {write_path}")

        character_count = pd.read_csv('./ric_starting_count_2026-04-27.csv')

        # if the count is greater than the _AS_TOP_LIMIT
        # then we create a combination of two characters
        # the first letter is the start_with character, 
        # then the second letter is will be a combination of all the characters in the alphabet
        # example:
            # if A has count more than 10000
            # then we query AA, AB, AC, ..., AZ
        # if the count is less than the _AS_TOP_LIMIT
        # then we create a combination of one character
        # the character is the start_with character
        # example:
            # if F has count less than 10000
            # then we query just F
        
        # convert count to numeric int
        character_count['count'] = character_count['count'].astype(int)

        # convert character to uppercase
        character_count['character'] = character_count['character'].str.upper()

        number_of_calls = 0

        # iterate through the character_count dataframe
        for _, row in character_count.iterrows():
            char = row['character']
            count = row['count']

            logger.info(f"===================== char: {char}, count: {count} =====================")

            if count > USUniverseIngestion._AS_TOP_LIMIT:
                # create a combination of two characters
                combo = [char + other_char for other_char in USUniverseIngestion._ALPHABET]
                number_of_calls += len(combo)
                logger.info(f"dual character combination needed: \n{combo}")
            
            else:
                combo = [char]
                number_of_calls += len(combo)
                logger.info(f"single character combination needed: \n{combo}")

            for _combo in combo:
                logger.info(f"calling: {_combo}")

                # call the API
                df = USUniverseIngestion.get_us_universe(_combo)

                # write to csv
                df.to_csv(f'{write_path}/us_universe_ric_starts_with_{_combo}.csv', index=True)

        logger.info(f"total combinations: {number_of_calls}")

    @staticmethod
    def collect_all_data(
        read_path: str | Path
    ):
        if isinstance(read_path, str):
            read_path = Path(read_path)

        assert isinstance(read_path, Path), "read_path must be a pathlib Path object"

        from os import listdir
        files = listdir(read_path)

        collect_df = []

        for file in files:
            if not file.endswith('.csv'):
                continue

            print(f"reading: {file}")
        
            df = pd.read_csv(f'{read_path}/{file}')

            # drop Unnamed: 0 column
            df = df.drop(columns=['Unnamed: 0'])

            # append to collect_df
            collect_df.append(df.copy())

        # concatenate the collect_df
        collect_df = pd.concat(
            collect_df, 
            axis=0, 
            ignore_index=True, 
            join='outer'
        )

        collect_df.to_csv(f'{read_path}/us_universe_full_merged.csv', index=False)

            





if __name__ == '__main__':
    # USUniverseIngestion.create_combination_of_start_with()
    USUniverseIngestion.collect_all_data('./_data/2026-04-27-16-09-10')
                

        






