import pandas as pd
import pytz
from alpaca.data.timeframe import TimeFrame

from Common.Common import DataFrameUtils
from Fetch.Fetch import Fetcher
from Order.Order import BuyerLocal, BuyerLive, SellerLocal, SellerLive
from Status.Status import AccountLocal, AccountLive, OrderList
from Strategy.Maengja import Maengja
from Strategy.SymbolFilter import EquityFilter
import pandas_market_calendars as Calender
from concurrent.futures import ThreadPoolExecutor, as_completed
from Common.Logger import Logger


class TimeManager:

    def __init__(self, timezone='America/New_York'):
        self.timezone = pytz.timezone(timezone)
        self.start = None
        self.current = None
        self.end = None
        self.open_dates = None
        self.open_time = (9, 30)
        self.close_time = (16, 0)

    def set_period(self, start, end):
        self.start = pd.Timestamp(start, tz=self.timezone).replace(microsecond=0)
        self.current = pd.Timestamp(start, tz=self.timezone).replace(microsecond=0)
        self.end = pd.Timestamp(end, tz=self.timezone).replace(microsecond=0)

    def advance_current(self, minutes=1):
        self.current += pd.Timedelta(minutes=minutes).replace(microsecond=0)

    def sync_current(self):
        self.current = pd.Timestamp.now(tz=self.timezone).replace(microsecond=0)

    def before_end(self):
        return self.current <= self.end

    def initialize_open_dates(self):
        nyse = Calender.get_calendar('NYSE')
        valid_days = nyse.valid_days(start_date=self.start, end_date=self.end,
                                     tz=self.timezone)
        self.open_dates = [day.date() for day in valid_days]

    def is_market_open(self):
        if not self.open_dates:
            self.initialize_open_dates()
        current_time = (self.current.hour, self.current.minute)
        return self.current.date() in self.open_dates and self.open_time <= current_time <= self.close_time

class SymbolManager:

    def __init__(self, max_symbols=50, asset_filter_rate=0.05, renew_symbol=False, max_workers=1):
        self.symbols = []
        self.max_symbols = max_symbols
        self.asset_filter_rate = asset_filter_rate
        self.renew_symbol = renew_symbol
        self.max_workers = max_workers

    def initialize_symbols(self, start_timestamp):
        self.symbols = EquityFilter(renew=self.renew_symbol, asset_filter_rate=self.asset_filter_rate, start_timestamp=start_timestamp, max_workers=self.max_workers).filter_symbols()[:self.max_symbols]
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

class DataManagerFast(DataManager):

    def __init__(self, history_param, max_workers):
        super().__init__(history_param)
        self.max_workers = max_workers  # 병렬 처리에 사용할 최대 스레드 수

    def fetch_history(self, symbols, current, timezone):
        def fetch_symbol_history(symbol):
            return symbol, self.fetcher.get_stock_history(
                symbols=[symbol],
                start=current - pd.Timedelta(hours=self.history_param['period']),
                end=current,
                timezone=timezone,
                time_frame=TimeFrame.Hour,
                bar_window=self.history_param['bar_window'],
                min_num_bars=self.history_param['min_num_bars'],
                local_data=False
            )

        # 병렬 실행
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_symbol = {executor.submit(fetch_symbol_history, symbol): symbol for symbol in symbols}

            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    symbol, data_symbol = future.result()
                    data = data_symbol[symbol]
                    if data is not None and not data.empty:
                        self.history[symbol] = data
                        print(f"Successful data fetching frm {symbol}")
                    else:
                        print(f"Warning: No data returned for symbol {symbol}")
                except Exception as e:
                    print(f"Error fetching data for symbol {symbol}: {e}")


        return self.history


class StrategyManager:

    def __init__(self):
        self.sages = {}
        self.prophecy = pd.DataFrame()

    def initialize_strategies(self, symbols):
        self.sages = {symbol: Maengja(symbol) for symbol in symbols}

    def evaluate(self, history, recent):
        self.prophecy = pd.DataFrame()
        for symbol in recent.keys():
            if symbol not in history:
                continue
            note = self.sages[symbol].update(history[symbol], recent[symbol])
            recent_note = {key: [note[key][-1]] for key in note}
            self.prophecy = pd.concat([self.prophecy, pd.DataFrame(recent_note)], ignore_index=True)
        return self.prophecy


class StrategyManagerFast:

    def __init__(self):
        self.sages = {}
        self.prophecy = pd.DataFrame()
        self.max_cpus = 30

    def initialize_strategies(self, symbols):
        self.sages = {symbol: Maengja(symbol) for symbol in symbols}

    def evaluate(self, history, recent):
        self.prophecy = pd.DataFrame()
        max_threads = min(self.max_cpus, len(recent))  # 심볼의 수보다 많으면 len(recent)로 조정
        recent_symbols = recent.keys()
        hist_symbols = []
        for sym in recent_symbols:
            if sym not in history:
                continue
            hist_symbols.append(sym)
        with ThreadPoolExecutor(max_threads) as executor:
            futures = {
                executor.submit(self._evaluate_symbol, symbol, history, recent): symbol
                for symbol in hist_symbols
            }
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    result = future.result()
                    self.prophecy = pd.concat([self.prophecy, result], ignore_index=True)
                except Exception as e:
                    print(f"Error evaluating symbol {symbol}: {e}")
        return self.prophecy

    def _evaluate_symbol(self, symbol, history, recent):
        note = self.sages[symbol].update(history[symbol], recent[symbol])
        recent_note = {key: [note[key][-1]] for key in note}
        return pd.DataFrame(recent_note)

class OrderManager:

    def __init__(self, live, one_time_invest_ratio, max_buy_per_min, max_ratio_per_asset, logfile, time_manager):
        self.live = live
        self.trade_cfg = dict(one_time_invest_ratio=one_time_invest_ratio,
                              max_buy_per_min=max_buy_per_min,
                              max_ratio_per_asset=max_ratio_per_asset)
        self.logfile = logfile
        self.logger = Logger(self.logfile)
        self.time_manager = time_manager
        if live:
            self.buyer = BuyerLive(self.trade_cfg, self.logger, self.time_manager)
            self.seller = SellerLive(self.logger, self.time_manager)
            self.account = AccountLive()
            self.order_list = OrderList(live)
        else:
            self.buyer = BuyerLocal(self.trade_cfg, self.logger, self.time_manager)
            self.seller = SellerLocal(self.logger, self.time_manager)
            self.account = AccountLocal()
            self.order_list = OrderList(live)

    def execute_orders(self, prophecy, prophecy_history):
        try:
            sell_symbols = prophecy[prophecy['sell']]['symbol'].tolist()
            self.order_list.update()
            for symbol in sell_symbols:
                if self.live:
                    if symbol in self.order_list.orders:
                        continue
                sold = self.seller.sell(prophecy, symbol, self.order_list)
                if sold:
                    DataFrameUtils.append_inplace(prophecy_history, prophecy[prophecy['symbol'] == symbol])
        except Exception as e:
            print(f"Sell execution error executing orders: {e}")

        try:
            buy_hubos = prophecy[prophecy['buy']]
            sorted_buy_hubos = buy_hubos.sort_values(by='trading_value', ascending=False)
            buy_count = 0
            self.order_list.update()
            for _, row in sorted_buy_hubos.iterrows():
                if self.is_affordable(row['symbol'], row['price']) and (row['symbol'] not in sell_symbols):
                    if self.live:
                        if row['symbol'] in self.order_list.orders:
                            continue
                    bought = self.buyer.buy(prophecy, row['symbol'], self.order_list)
                    if bought:
                        DataFrameUtils.append_inplace(prophecy_history, prophecy[prophecy['symbol'] == row['symbol']])
                        buy_count += 1
                    if buy_count >= self.trade_cfg['max_buy_per_min']:
                        break
        except Exception as e:
            print(f"Buy execution error executing orders: {e}")

    def is_affordable(self, symbol, price):
        if symbol in self.account.positions.assets:
            asset_ratio = self.account.positions.assets[symbol]['market_value'] / self.account.get_total_value()
            if asset_ratio > self.trade_cfg['max_ratio_per_asset']:
                return False
        return self.account.cash >= price * 2.0