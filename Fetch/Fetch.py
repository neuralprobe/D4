from datetime import datetime, timedelta
from ApiAccess.ApiAccess import ClientType, ClientManager
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed, Adjustment
import pandas as pd
import os
import pytz


class Fetcher:
    """Combines API fetching, local data handling, and processing."""

    def __init__(self):
        self.api_fetcher = ApiDataFetcher()
        self.local_handler = LocalDataFetcher()
        self.processor = HistoryProcessor()

    def get_stock_history(self, symbols, start, end, time_frame, bar_window=1, min_num_bars=0, timezone='America/New_York', local_data=False):
        """Retrieve stock history from API or local data."""
        if local_data:
            history = self.local_handler.get_stock_history_from_local_data(symbols, start, end, time_frame, timezone)
        else:
            df_history = self.api_fetcher.get_stock_history(symbols, start, end, time_frame)
            if df_history.empty:
                return {}
            grouped = df_history.groupby(level='symbol')
            history = {symbol: group.reset_index(level='symbol', drop=True) for symbol, group in grouped}

        if bar_window > 1:
            history = {k: self.processor.merge_to_a_single_bar(v, bar_window) for k, v in history.items()}

        return self.processor.remove_symbols_with_small_num_bars(history, min_num_bars)


class ApiDataFetcher:
    """Handles fetching data from Alpaca APIs."""
    def __init__(self):
        self.client_manager = ClientManager()

    def get_stock_history(self, symbols, start, end, time_frame):
        stock_client = self.client_manager.get_client(ClientType.STOCK_HISTORY)
        time_margin = pd.Timedelta(seconds=10)
        request_params = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=time_frame,
            start=(start-time_margin).to_pydatetime(),
            end=end.to_pydatetime(),
            feed=DataFeed.SIP,
            adjustment=Adjustment.SPLIT
        )
        df_history = stock_client.get_stock_bars(request_params).df
        if not df_history.empty:
            df_history['trading_value'] = df_history['volume'] * df_history['vwap']
        return df_history



class LocalDataFetcher:
    """Handles local data management and caching."""

    def __init__(self, base_directory=f'{os.environ.get("D4")}/Data/Local'):
        self.base_directory = base_directory
        self.symbol_files_dict = {}
        self.cache = {}
        self.hourbar_it = {}
        self.minbar_it = {}

    def list_files_in_folder(self, folder_path):
        """List files in a folder with their timestamps."""
        return sorted([
            (*self.filename_to_timestamp(name), name)
            for name in os.listdir(folder_path)
            if os.path.isfile(os.path.join(folder_path, name))
        ])

    @staticmethod
    def filename_to_timestamp(name):
        """Convert a filename to timestamps."""
        parts = name.split('_')
        return pd.Timestamp(parts[4]), pd.Timestamp(parts[6].split('.')[0])

    def get_stock_history_from_local_data(self, symbols, start, end, time_frame, timezone='America/New_York'):
        """Retrieve stock history from locally stored data."""
        if not self.symbol_files_dict:
            self.symbol_files_dict = {
                symbol: self.list_files_in_folder(os.path.join(self.base_directory, symbol))
                for symbol in symbols
            }
        # Logic for retrieving hour/minute bars
        history = {}
        # Populate `history` with data retrieved from cache
        return history



class HistoryProcessor:
    """Processes and transforms financial data."""

    @staticmethod
    def merge_to_a_single_bar(df, bar_window):
        """Merge multiple rows into single bars based on the window size."""
        assert bar_window > 0
        result = {col: [] for col in df.columns}
        for i in range(len(df), 0, -bar_window):
            group = df.iloc[max(0, i - bar_window):i]
            result['open'].append(group['open'].iloc[0])
            result['high'].append(group['high'].max())
            result['low'].append(group['low'].min())
            result['close'].append(group['close'].iloc[-1])
            result['volume'].append(group['volume'].sum())
            result['trade_count'].append(group['trade_count'].sum())
            result['trading_value'].append(group['trading_value'].sum())
            result['vwap'].append(group['trading_value'].sum() / group['volume'].sum())
        return pd.DataFrame(result, columns=df.columns)

    @staticmethod
    def remove_symbols_with_small_num_bars(history, min_num_bars):
        """Remove symbols with fewer bars than the minimum required."""
        return {k: v for k, v in history.items() if len(v) >= min_num_bars}

