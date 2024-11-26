from datetime import datetime
import pytz
import pandas as pd
from Strategy.Symbol_Filter import filter_us_equity_static
from Fetch.Fetch import Fetcher
from alpaca.data.timeframe import TimeFrame
from Strategy.Decision import Maengja_trailing as Maengja
from Order.Order import Buyer, Seller
from Status.Status import Portfolio, Account
import pandas_market_calendars as mcal
from Common.Common import append_dataframe_inplace, Time, Tee


class HistoryManager:
    def __init__(self, fetcher, symbols, history_param):
        self.fetcher = fetcher
        self.symbols = symbols
        self.history_param = history_param
        self.history = {}
        self.recent = {}

    def initialize_history(self, start, current_time, timezone):
        self.history = self.fetcher.get_stock_history(
            symbols=self.symbols,
            start=current_time - pd.Timedelta(hours=self.history_param['period']),
            end=current_time,
            timezone=timezone,
            time_frame=TimeFrame.Hour,
            bar_window=self.history_param['bar_window'],
            min_num_bars=self.history_param['min_num_bars'],
            local_data=False
        )
        self.symbols = [symbol for symbol in self.history]  # 유효한 심볼만 필터링.

    def update_history(self, current_time, timezone):
        self.recent = self.fetcher.get_stock_history(
            symbols=self.symbols,
            start=current_time - pd.Timedelta(minutes=1),
            end=current_time,
            timezone=timezone,
            time_frame=TimeFrame.Minute,
            bar_window=self.history_param['bar_window'],
            min_num_bars=1,
            local_data=False
        )
        return self.recent


class ProphecyManager:
    def __init__(self, symbols):
        self.symbols = symbols
        self.sages = {symbol: Maengja(symbol) for symbol in symbols}
        self.current_prophecy = pd.DataFrame()

    def update_sages(self, history, recent):
        if not recent:
            return
        self.current_prophecy = pd.DataFrame()
        for symbol in recent:
            note = self.sages[symbol].update(history[symbol], recent[symbol])
            current_note = {key: [note[key][-1]] for key in note}
            if current_note['buy'][-1] or current_note['sell'][-1] or current_note['keep_profit'][-1]:
                current_note_df = pd.DataFrame(current_note)
                self.current_prophecy = pd.concat([self.current_prophecy, current_note_df], ignore_index=True)


class OrderManager:
    def __init__(self, buyer, seller, max_buy_per_min, max_ratio_per_asset, portfolio, account):
        self.buyer = buyer
        self.seller = seller
        self.max_buy_per_min = max_buy_per_min
        self.max_ratio_per_asset = max_ratio_per_asset
        self.portfolio = portfolio
        self.account = account

    def execute_orders(self, prophecy):
        sell_symbols = prophecy[prophecy['sell']]['symbol'].tolist()
        for sell_symbol in sell_symbols:
            self.seller.sell(prophecy, sell_symbol)

        buy_symbols = prophecy[prophecy['buy']]['symbol'].tolist()
        for buy_symbol in buy_symbols[:self.max_buy_per_min]:
            self.buyer.buy(prophecy, buy_symbol)


class MarketCalendar:
    def __init__(self, timezone):
        self.timezone = timezone
        self.all_open_days_list = []

    def is_market_open(self, current_time):
        nyse = mcal.get_calendar('NYSE')
        valid_days = nyse.valid_days(start_date="2020-01-01", end_date=datetime.now().strftime('%Y-%m-%d'))
        open_dates = [date.date() for date in valid_days]
        return current_time.date() in open_dates and 9 <= current_time.hour < 16


class Trader:
    def __init__(self):
        self.time = Time()
        self.history_param = {'period': 2000, 'bar_window': 1, 'min_num_bars': 480}
        self.symbols = []
        self.fetcher = Fetcher()
        self.history_manager = HistoryManager(self.fetcher, self.symbols, self.history_param)
        self.prophecy_manager = None
        self.market_calendar = MarketCalendar(self.time.timezone)
        self.buyer = Buyer()
        self.seller = Seller()
        self.account = Account()
        self.portfolio = Portfolio()
        self.order_manager = OrderManager(self.buyer, self.seller, 2, 0.05, self.portfolio, self.account)

    def initialize(self, start, end):
        self.time.start = pd.Timestamp(start, tz=pytz.timezone(self.time.timezone))
        self.time.end = pd.Timestamp(end, tz=pytz.timezone(self.time.timezone))
        self.time.current = self.time.start
        self.symbols = filter_us_equity_static(renew=True, start_timestamp=self.time.start)[:50]
        self.history_manager.initialize_history(self.time.start, self.time.current, self.time.timezone)
        self.prophecy_manager = ProphecyManager(self.symbols)

    def simulate_trade(self, start, end):
        self.initialize(start, end)
        while self.time.current <= self.time.end:
            if self.market_calendar.is_market_open(self.time.current):
                recent = self.history_manager.update_history(self.time.current, self.time.timezone)
                self.prophecy_manager.update_sages(self.history_manager.history, recent)
                self.order_manager.execute_orders(self.prophecy_manager.current_prophecy)
            self.time.current += pd.Timedelta(minutes=1)


if __name__ == "__main__":
    trader = Trader()
    trader.simulate_trade("2024-07-01 09:30:00", "2024-07-31 16:00:00")
