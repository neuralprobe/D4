import pandas as pd
import pytz
from alpaca.data.timeframe import TimeFrame
from Fetch.Fetch import Fetcher
from Order.Order import BuyerLocal, SellerLocal
from Status.Status import AccountLocal
from Strategy.Maengja import Maengja
from Strategy.SymbolFilter import EquityFilter
import pandas_market_calendars as Calender

class TimeManager:

    def __init__(self, timezone='America/New_York'):
        self.timezone = pytz.timezone(timezone)
        self.start = None
        self.current = None
        self.end = None

    def set_period(self, start, end):
        self.start = pd.Timestamp(start, tz=self.timezone)
        self.current = pd.Timestamp(start, tz=self.timezone)
        self.end = pd.Timestamp(end, tz=self.timezone)

    def advance_time(self, minutes=1):
        self.current += pd.Timedelta(minutes=minutes)

    def is_within_period(self):
        return self.current <= self.end


class SymbolManager:

    def __init__(self, max_symbols=50, asset_filter_rate=0.05):
        self.symbols = []
        self.max_symbols = max_symbols
        self.asset_filter_rate = asset_filter_rate

    def initialize_symbols(self, start_timestamp):
        self.symbols = EquityFilter(renew=False, asset_filter_rate=self.asset_filter_rate, start_timestamp=start_timestamp).filter_symbols()[:self.max_symbols]
        return self.symbols


class DataManager:

    def __init__(self, history_param):
        self.fetcher = Fetcher()
        self.history_param = history_param
        self.history = {}
        self.recent = {}

    def fetch_history(self, symbols, current, timezone):
        self.history = self.fetcher.get_stock_history(
            symbols=symbols,
            start=current - pd.Timedelta(hours=self.history_param['period']),
            end=current,
            timezone=timezone,
            time_frame=TimeFrame.Hour,
            bar_window=self.history_param['bar_window'],
            min_num_bars=self.history_param['min_num_bars'],
            local_data=False
        )
        return self.history

    def update_recent_data(self, symbols, current, timezone):
        self.recent = self.fetcher.get_stock_history(
            symbols=symbols,
            start=current - pd.Timedelta(minutes=1),
            end=current,
            timezone=timezone,
            time_frame=TimeFrame.Minute,
            bar_window=self.history_param['bar_window'],
            min_num_bars=1,
            local_data=False
        )
        return self.recent

    def merge_recent_data_into_hourly(self):
        if not self.recent:
            return
        for symbol in self.recent:
            hbar_time = self.history[symbol].index[-1] if symbol in self.history else None
            mbar_time = self.recent[symbol].index[-1]
            if hbar_time is None or mbar_time > hbar_time:
                if self._needs_new_hour_bar(hbar_time, mbar_time):
                    self._create_new_hour_bar(symbol)
                else:
                    self._update_existing_hour_bar(symbol)

    @staticmethod
    def _needs_new_hour_bar(hbar_time, mbar_time):
        if hbar_time is None:
            return True
        return mbar_time.date() > hbar_time.date() or mbar_time.hour > hbar_time.hour

    def _create_new_hour_bar(self, symbol):
        new_row = self.recent[symbol].iloc[[-1]]
        if symbol in self.history:
            self.history[symbol].drop(self.history[symbol].index[:1], inplace=True)
        else:
            self.history[symbol] = pd.DataFrame()

        for index, row in new_row.iterrows():
            self.history[symbol].loc[index] = row
        self.history[symbol].sort_index(inplace=True)

    def _update_existing_hour_bar(self, symbol):
        new_row = self.recent[symbol].iloc[[-1]]
        last_index = self.history[symbol].index[-1]

        self.history[symbol].loc[last_index, 'high'] = max(self.history[symbol].loc[last_index, 'high'], new_row['high'].iloc[-1])
        self.history[symbol].loc[last_index, 'low'] = min(self.history[symbol].loc[last_index, 'low'], new_row['low'].iloc[-1])
        self.history[symbol].loc[last_index, 'close'] = new_row['close'].iloc[-1]
        self.history[symbol].loc[last_index, 'volume'] += new_row['volume'].iloc[-1]
        self.history[symbol].loc[last_index, 'trade_count'] += new_row['trade_count'].iloc[-1]
        self.history[symbol].loc[last_index, 'trading_value'] += new_row['trading_value'].iloc[-1]

        if self.history[symbol].loc[last_index, 'volume'] > 0:
            self.history[symbol].loc[last_index, 'vwap'] = (
                self.history[symbol].loc[last_index, 'trading_value'] / self.history[symbol].loc[last_index, 'volume']
            )


class StrategyManager:

    def __init__(self):
        self.sages = {}
        self.prophecy = pd.DataFrame()

    def initialize_strategies(self, symbols):
        self.sages = {symbol: Maengja(symbol) for symbol in symbols}

    def evaluate(self, history, recent):
        self.prophecy = pd.DataFrame()
        for symbol in recent.keys():
            note = self.sages[symbol].update(history[symbol], recent[symbol])
            recent_note = {key: [note[key][-1]] for key in note}
            self.prophecy = pd.concat([self.prophecy, pd.DataFrame(recent_note)], ignore_index=True)
        return self.prophecy


class OrderManager:

    def __init__(self, one_time_invest_ratio, max_buy_per_min, max_ratio_per_asset):
        self.trade_cfg = dict(one_time_invest_ratio=one_time_invest_ratio,
                              max_buy_per_min=max_buy_per_min,
                              max_ratio_per_asset=max_ratio_per_asset)
        self.buyer = BuyerLocal(self.trade_cfg)
        self.seller = SellerLocal()
        self.account = AccountLocal()

    def execute_orders(self, prophecy):
        sell_symbols = prophecy[prophecy['sell']]['symbol'].tolist()
        for symbol in sell_symbols:
            self.seller.sell(prophecy, symbol)
            print("sell, account:", self.account.get_total_value())

        buy_candidates = prophecy[prophecy['buy']]
        top_buy_candidates = buy_candidates.nlargest(self.trade_cfg['max_buy_per_min'], 'trading_value')
        for _, row in top_buy_candidates.iterrows():
            if self.is_affordable(row['symbol'], row['price']):
                self.buyer.buy(prophecy, row['symbol'])
                print("buy, account:", self.account.get_total_value())

    def is_affordable(self, symbol, price):
        if symbol in self.account.positions.assets:
            asset_ratio = self.account.positions.assets[symbol]['market_value'] / self.account.get_total_value()
            if asset_ratio > self.trade_cfg['max_ratio_per_asset']:
                return False
        return self.account.cash >= price * 2.0


class Trader:

    def __init__(self):
        self.time_manager = TimeManager()
        self.symbol_manager = SymbolManager(max_symbols=5, asset_filter_rate=0.05)
        self.data_manager = DataManager(history_param={'period': 2000, 'bar_window': 1, 'min_num_bars': 480})
        self.strategy_manager = StrategyManager()
        self.order_manager = OrderManager(one_time_invest_ratio=0.05, max_buy_per_min=2, max_ratio_per_asset=0.05)
        self.account = AccountLocal()
        self.account.set_cash(100000)

    def run(self, start, end):
        self.initialize(start, end)
        while self.time_manager.is_within_period():
            if self.is_market_open():
                recent = self.data_manager.update_recent_data(
                    self.symbol_manager.symbols, self.time_manager.current, self.time_manager.timezone
                )
                if recent:
                    prophecy = self.strategy_manager.evaluate(self.data_manager.history, recent)
                    self.order_manager.execute_orders(prophecy)
                    print("time:",self.time_manager.current)
            self.time_manager.advance_time()


    def initialize(self, start, end):
        self.time_manager.set_period(start, end)
        symbols = self.symbol_manager.initialize_symbols(self.time_manager.start)
        self.data_manager.fetch_history(symbols, self.time_manager.current, self.time_manager.timezone)
        self.strategy_manager.initialize_strategies(symbols)

    def is_market_open(self):
        nyse = Calender.get_calendar('NYSE')
        valid_days = nyse.valid_days(start_date=self.time_manager.start, end_date=self.time_manager.end,
                                     tz=self.time_manager.timezone)
        open_dates = [day.date() for day in valid_days]
        current_time = (self.time_manager.current.hour, self.time_manager.current.minute)
        return self.time_manager.current.date() in open_dates and (9, 30) <= current_time <= (16, 0)


if __name__ == "__main__":
    trader = Trader()
    trader.run('2024-07-01 09:30:00', '2024-07-31 16:00:00')