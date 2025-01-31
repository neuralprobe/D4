import pandas as pd
from datetime import datetime, timedelta
import math
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed
from ApiAccess.ApiAccess import ClientType, ClientManager
import Common.Common as Common
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv

class AssetFilter:
    """Base class for asset filtering with Alpaca API."""

    def __init__(self, filter_type, file_path):
        self.filter_type = filter_type
        self.file_path = file_path
        self.file_path = self.file_path.replace(":", "-")

    def read_existing_symbols(self):
        """Read existing symbols from file."""
        return Common.CSVHandler.read_to_list(self.file_path)

    def write_symbols_to_file(self, symbols):
        """Write filtered symbols to file."""
        (Common.CSVHandler.write_from_list(symbols, self.file_path))

class EquityFilter(AssetFilter):
    """Filter US equities based on trading volume and value."""

    def __init__(self, renew=False, asset_filter_num=250, russel_filter_num=250, start_timestamp=pd.Timestamp.now(), max_workers=1):
        super().__init__('US_EQUITY', f'{os.environ.get("D4")}/Data/Symbols/symbols_us_{start_timestamp}.csv')
        self.renew = renew
        self.start_timestamp = start_timestamp
        self.asset_filter_num = asset_filter_num
        self.russel_filter_num = russel_filter_num
        self.max_workers = max_workers

    @staticmethod
    def get_symbols():
        """Fetch tradable US equities from Alpaca API."""
        trading_client = ClientManager().get_client(ClientType.TRADE)
        search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
        assets = trading_client.get_all_assets(search_params)
        return pd.DataFrame([
            dict(symbol=asset.symbol, tradable=asset.tradable) for asset in assets if asset.tradable
            ]).set_index('symbol')

    def get_bars_slow(self, symbols):
        """Fetch 60-day stock bar data for given symbols."""
        stock_client = ClientManager().get_client(ClientType.STOCK_HISTORY)
        request_size = 1024
        df_daily_bars = pd.DataFrame()
        for i in range(math.ceil(len(symbols) / request_size)):
            batch = symbols[i * request_size: (i + 1) * request_size]
            request_params = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=TimeFrame.Day,
                start=self.start_timestamp - timedelta(days=10), #120
                end=self.start_timestamp,
                feed=DataFeed.SIP
            )
            daily_bars = stock_client.get_stock_bars(request_params).df
            df_daily_bars = pd.concat([df_daily_bars, daily_bars.groupby(level='symbol').tail(60)])
        return df_daily_bars

    # 병렬화된 get_bars_slow 함수
    def get_bars(self, symbols):
        """Fetch 60-day stock bar data for given symbols using parallel processing."""
        stock_client = ClientManager().get_client(ClientType.STOCK_HISTORY)
        request_size = 1024

        def fetch_batch(i):
            batch = symbols[i * request_size: (i + 1) * request_size]
            request_params = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=TimeFrame.Day,
                start=self.start_timestamp - timedelta(days=120),  # 120
                end=self.start_timestamp,
                feed=DataFeed.SIP
            )
            daily_bars = stock_client.get_stock_bars(request_params).df
            return daily_bars.groupby(level='symbol').tail(60)

        num_batches = math.ceil(len(symbols) / request_size)
        df_daily_bars = pd.DataFrame()

        # 병렬 실행
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(fetch_batch, i): i for i in range(num_batches)}

            for future in as_completed(futures):
                batch_index = futures[future]
                try:
                    batch_bars = future.result()
                    df_daily_bars = pd.concat([df_daily_bars, batch_bars])
                    print(f"Successfully fetched batch {batch_index}")
                except Exception as e:
                    print(f"Error fetching batch {batch_index}: {e}")

        return df_daily_bars


    def filter_symbols(self):
        """Filter US equities and return top 5% by trading value."""
        if not self.renew:
            return self.read_existing_symbols()

        tradable_assets = self.get_symbols()
        symbols = tradable_assets.index.to_list()
        daily_bars = self.get_bars(symbols)

        # Calculate trading value
        daily_bars['trading_value'] = daily_bars['volume'] * daily_bars['vwap']
        mean_trading_value = daily_bars.groupby(level='symbol')['trading_value'].mean()
        sorted_symbols = mean_trading_value.sort_values(ascending=False)

        # Select and write top-k assets
        threshold = self.asset_filter_num # int(len(sorted_symbols) * self.asset_filter_rate)
        top_symbols = sorted_symbols.head(threshold).index.tolist()

        # Do the same for Russel 2000
        folder_path = os.path.join(os.environ.get('D4'), 'Data/ExtData')
        russel = "IWM_holdings.csv"
        russel_csv = os.path.join(folder_path,russel)
        symbol_russel = self.get_tickers_from_csv(russel_csv)
        daily_bars_russel = self.get_bars(symbol_russel)
        daily_bars_russel['trading_value'] = daily_bars_russel['volume'] * daily_bars_russel['vwap']
        mean_trading_value_russel = daily_bars_russel.groupby(level='symbol')['trading_value'].mean()
        sorted_symbols_russel = mean_trading_value_russel.sort_values(ascending=False)
        threshold_russel = self.russel_filter_num # int(len(sorted_symbols_russel) * self.asset_filter_rate)
        top_symbols_russel = sorted_symbols_russel.head(threshold_russel).index.tolist()
        final_top_symbols = list(set(top_symbols + top_symbols_russel))

        self.write_symbols_to_file(final_top_symbols)
        return final_top_symbols

    def get_tickers_from_csv(self,file_path):
        """
        Reads the first column (Ticker) from a CSV file and returns a list of tickers.

        :param file_path: Path to the CSV file.
        :return: List of tickers.
        """
        tickers = []
        with open(file_path, 'r') as file:
            csv_reader = csv.reader(file)
            # Skip the header row
            next(csv_reader)
            for row in csv_reader:
                tickers.append(row[0])  # Append the first column (Ticker)
        return tickers


class CryptoFilter(AssetFilter):
    """Filter cryptocurrencies based on trading volume and value."""

    def __init__(self, renew=False, asset_filter_rate=0.20, start_timestamp=pd.Timestamp.now()):
        super().__init__('CRYPTO', f'{os.environ.get("D4")}/Data/Symbols/symbols_crypto_{start_timestamp}.csv')
        self.renew = renew
        self.start_timestamp = start_timestamp
        self.asset_filter_rate = asset_filter_rate

    @staticmethod
    def get_symbols():
        """Fetch tradable cryptocurrencies from Alpaca API."""
        trading_client = ClientManager().get_client(ClientType.TRADE)
        search_params = GetAssetsRequest(asset_class=AssetClass.CRYPTO)
        assets = trading_client.get_all_assets(search_params)
        return pd.DataFrame([
            dict(symbol=asset.symbol, tradable=asset.tradable) for asset in assets if asset.tradable and '/USD' in asset.symbol
        ]).set_index('symbol')

    def get_bars(self, symbols):
        """Fetch 60-day crypto bar data for given symbols."""
        crypto_client = ClientManager().get_client(ClientType.CRYPTO_HISTORY)
        request_params = CryptoBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame.Day,
            start=datetime.today() - timedelta(days=120),
            end=datetime.today()
        )
        daily_bars = crypto_client.get_crypto_bars(request_params).df
        return daily_bars.groupby(level='symbol').tail(60)

    def filter_symbols(self):
        """Filter cryptocurrencies and return top 20% by trading value."""
        if not self.renew:
            return self.read_existing_symbols()

        tradable_assets = self.get_symbols()
        symbols = tradable_assets.index.to_list()
        daily_bars = self.get_bars(symbols)

        # Calculate trading value
        daily_bars['trading_value'] = daily_bars['volume'] * daily_bars['vwap']
        mean_trading_value = daily_bars.groupby('symbol')['trading_value'].mean()
        threshold = mean_trading_value.quantile(1-self.asset_filter_rate)
        top_symbols = mean_trading_value[mean_trading_value > threshold].index.tolist()
        self.write_symbols_to_file(top_symbols)
        return top_symbols

import pytz

if __name__ == "__main__":
    symbols = EquityFilter(renew=True, asset_filter_num=250, russel_filter_num=250,
                                start_timestamp=pd.Timestamp('2024-11-01 09:31:00', tz=pytz.timezone('America/New_York')).replace(microsecond=0), max_workers=16).filter_symbols()
    print(symbols)
    print("Sym Len:", len(symbols))
