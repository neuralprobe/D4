import csv
from dataclasses import dataclass, field
import pandas as pd
import os


class CSVHandler:
    """Class for handling CSV file operations."""

    @staticmethod
    def read_to_list(file_path):
        """Read the first column of a CSV file into a list."""
        with open(file_path, 'r') as file:
            return [row[0] for row in csv.reader(file)]

    @staticmethod
    def write_from_list(str_list, file_path):
        """Write a list of strings to a CSV file."""
        print(f"TRYING TO WRITE at {file_path}")
        print(str_list)
        with open(file_path, 'w', newline='') as file:
            print(f"WRITING ENTER at {file_path}")
            writer = csv.writer(file)
            writer.writerows([[line] for line in str_list])
            print(f"WRITING DONE at {file_path}")


class DataFrameUtils:
    """Utility class for DataFrame operations."""

    @staticmethod
    def append_inplace(df1, df2):
        """Append rows of one DataFrame to another in-place."""
        if df1.empty:
            for col in df2.columns:
                df1[col] = []
        for _, row in df2.iterrows():
            df1.loc[len(df1)] = row


@dataclass
class Time:
    """Class for managing time-related data."""
    timezone: str = 'America/New_York'
    start: pd.Timestamp = field(default_factory=pd.Timestamp.now)
    end: pd.Timestamp = field(default_factory=pd.Timestamp.now)
    current: pd.Timestamp = field(default_factory=pd.Timestamp.now)


class Tee:
    """Class to replicate stdout/stderr into multiple files."""
    def __init__(self, *files):
        self.files = files

    def write(self, obj):
        for f in self.files:
            f.write(obj)

    def flush(self):
        for f in self.files:
            f.flush()


class SingletonMeta(type):
    """A metaclass for Singleton pattern."""
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

    @classmethod
    def is_instantiated(cls, target_cls):
        """Check if a class is already instantiated."""
        return target_cls in cls._instances


class Printer:
    @staticmethod
    def store_prophecy_history(prophecy_history, filename):
        prophecy_history.rename(columns={'time': '시간',
                                      'symbol': '종목',
                                      'touch_bb1_lower': 'bb1아래',
                                      'bullish_breakout_bb1_lower': 'bb1돌파1',
                                      'bullish_breakout_bb1_lower_margin': 'bb1돌파2',
                                      'touch_bb2_lower': 'bb1터치',
                                      'bullish_breakout_bb2_lower': 'bb2아래',
                                      'bullish_breakout_bb2_lower_margin': 'bb2돌파1',
                                      'PO_divergence': 'bb2돌파2',
                                      'RSI_check': 'RSI다이버',
                                      'SMA_align_strength': '정배열',
                                      'check_SMA_breakthrough': 'SMA돌파',
                                      'SMA_below_close': '종가밑SMA',
                                      'buy': '매수의견',
                                      'buy_reason': '매수근거',
                                      'buy_strength': '매수강도',
                                      'stop_loss': '손절가',
                                      'stop_loss_name': '손절근거',
                                      'current_close': '종가',
                                      'trading_value': '거래대금',
                                      'stoploss_downward_breakout': '손절가터치',
                                      'resistance_upward_breakout': '저항선터치',
                                      'new_stop_loss_candidate': '손절가후보',
                                      'new_stop_loss_name_candidate': '손절가후보근거',
                                      'top_resist_downward_break': '무저항매도',
                                      'sell': '매도의견',
                                      'sell_reason': '매도근거',
                                      'keep_profit': '보유의견',
                                      'hold': '보유여부',
                                      'qty': '갯수',
                                      'cost': '비용',
                                      'avg_price': '평균값',
                                      'buy_order': '매수주문',
                                      'sell_order': '매도주문'},
                             inplace=True)

        prophecy_history = prophecy_history.apply(
            lambda col: col.map(lambda x: round(x, 2) if isinstance(x, float) else x)
        )

        prophecy_history.to_csv(f"{os.environ.get('D4')}/Results/{filename}")

