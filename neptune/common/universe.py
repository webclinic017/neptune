import logging
import pickle

import pandas as pd
import numpy as np
from finviz import Screener

from neptune.config import DATA_DIRECTORY


class SymbolUniverse:
    """Queries Finviz API and retrieves full market snapshot data.
    Writes retrieved data to local file for fast retrieval.
    """
    MULT_FACTORS = {'B': 1e9, 'M': 1e6, 'K': 1e3, '%': 0.01}
    TEXT_COLUMNS = ['Company', 'Sector', 'Industry', 'Country']
    CSV_FILEPATH = DATA_DIRECTORY / 'symbol_universe.csv'
    PKL_FILEPATH = DATA_DIRECTORY / 'symbol_universe.pkl'

    def __init__(self, reload=True):
        self.logger = logging.getLogger('SymbolUniverse')
        if self.PKL_FILEPATH.is_file() and reload:
            with open(str(self.PKL_FILEPATH), 'rb') as fp:
                self.data = pickle.load(fp)
        else:
            self.data = self._build_database()

    def _build_database(self) -> pd.DataFrame:
        """Rebuild the symbol database using a FinViz query
        @return: df containing symbol data
        """

        filters = {
            'USA': ['cap_smallover', 'geo_usa', 'ind_stocksonly', 'sh_avgvol_o500'],
            'RestOfWorld': ['cap_midover', 'geo_notusa', 'ind_stocksonly', 'sh_avgvol_o500'],
            # 'ETF': ['ind_exchangetradedfund', 'sh_avgvol_o2000']
        }

        # Query FinViz, request all columns
        self.logger.info("Collecting symbol data from FinViz...")
        columns = [str(i) for i in range(70)]

        df = pd.DataFrame()
        for category, filter_items in filters.items():
            self.logger.info("Querying {} data...".format(category))
            screener = Screener(filters=filter_items, table='Custom', custom=columns)
            data = pd.DataFrame(screener.data)
            data = data.apply(pd.to_numeric, errors='ignore')
            df = df.append(data)

        # Use symbol as index
        self.logger.info("Cleaning data...")
        df.drop(columns=['No.'], inplace=True, errors='ignore')
        df.set_index('Ticker', drop=True, inplace=True)
        df.index = df.index.map(str)  # Convert UnicodeElement to str

        # Clean up raw returned data
        df = df.replace('-', np.NaN)
        df['Volume'] = df['Volume'].str.replace(',', '').astype('int')
        process_columns = [col for col in df.columns if col not in SymbolUniverse.TEXT_COLUMNS]
        for col in process_columns:
            df[col] = pd.to_numeric(df[col], errors='ignore')
            for c, factor in SymbolUniverse.MULT_FACTORS.items():
                try:
                    idx = df[col].str.strip().str[-1].str.contains(c).fillna(value=False)
                    df.loc[idx, col] = df.loc[idx, col].str.strip().str.rstrip(c).astype('float',
                                                                                         errors='ignore') * factor
                except Exception as e:
                    self.logger.error("Exception caught while cleaning FinViz data: {}".format(str(e)))
                    pass

        for col in SymbolUniverse.TEXT_COLUMNS:
            df[col] = df[col].astype('str')

        # Save pickle for fast reload
        filepath = str(self.PKL_FILEPATH)
        self.logger.info("Writing data to {}".format(filepath))
        with open(filepath, "wb") as f:
            pickle.dump(df, f)
        df.to_csv(str(self.CSV_FILEPATH))

        return df