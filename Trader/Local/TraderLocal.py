import pandas as pd
import pytz
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
from Common.Common import DataFrameUtils, Printer
from Fetch.Fetch import Fetcher
from Order.Order import BuyerLocal, SellerLocal
from Status.Status import AccountLocal
from Strategy.Maengja import Maengja
from Strategy.SymbolFilter import EquityFilter
import pandas_market_calendars as Calender
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        self.start = pd.Timestamp(start, tz=self.timezone)
        self.current = pd.Timestamp(start, tz=self.timezone)
        self.end = pd.Timestamp(end, tz=self.timezone)

    def advance_time(self, minutes=1):
        self.current += pd.Timedelta(minutes=minutes)

    def is_within_period(self):
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

class DataManagerFast(DataManager):

    def __init__(self, history_param):
        super().__init__(history_param)
        self.max_workers = 24  # 병렬 처리에 사용할 최대 스레드 수

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
        self.max_cpus = 24

    def initialize_strategies(self, symbols):
        self.sages = {symbol: Maengja(symbol) for symbol in symbols}

    def _evaluate_symbol(self, symbol, history, recent):
        note = self.sages[symbol].update(history[symbol], recent[symbol])
        recent_note = {key: [note[key][-1]] for key in note}
        return pd.DataFrame(recent_note)

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

    # def evaluate(self, history, recent):
    #     self.prophecy = pd.DataFrame()
    #     max_threads = min(self.max_cpus, len(recent))
    #     hist_symbols = [sym for sym in recent.keys() if sym in history]
    #     with ThreadPoolExecutor(max_threads) as executor:
    #         results = list(executor.map(
    #             lambda symbol: self._evaluate_symbol(symbol, history, recent),
    #             hist_symbols
    #         ))
    #     self.prophecy = pd.concat(results, ignore_index=True)
    #     return self.prophecy

class OrderManager:

    def __init__(self, one_time_invest_ratio, max_buy_per_min, max_ratio_per_asset):
        self.trade_cfg = dict(one_time_invest_ratio=one_time_invest_ratio,
                              max_buy_per_min=max_buy_per_min,
                              max_ratio_per_asset=max_ratio_per_asset)
        self.buyer = BuyerLocal(self.trade_cfg)
        self.seller = SellerLocal()
        self.account = AccountLocal()

    def execute_orders(self, prophecy, prophecy_history):
        sell_symbols = prophecy[prophecy['sell']]['symbol'].tolist()
        for symbol in sell_symbols:
            sold = self.seller.sell(prophecy, symbol)
            if sold:
               DataFrameUtils.append_inplace(prophecy_history, prophecy[prophecy['symbol'] == symbol])

        buy_candidates = prophecy[prophecy['buy']]
        top_buy_candidates = buy_candidates.nlargest(self.trade_cfg['max_buy_per_min'], 'trading_value')
        sorted_buy_candidates = buy_candidates.sort_values(by='trading_value', ascending=False)
        buy_count = 0
        for _, row in sorted_buy_candidates.iterrows():
            if self.is_affordable(row['symbol'], row['price']):
                bought = self.buyer.buy(prophecy, row['symbol'])
                if bought:
                    DataFrameUtils.append_inplace(prophecy_history, prophecy[prophecy['symbol'] == row['symbol']])
                    buy_count += 1
                if buy_count >= self.trade_cfg['max_buy_per_min']:
                    break

    def is_affordable(self, symbol, price):
        if symbol in self.account.positions.assets:
            asset_ratio = self.account.positions.assets[symbol]['market_value'] / self.account.get_total_value()
            if asset_ratio > self.trade_cfg['max_ratio_per_asset']:
                return False
        return self.account.cash >= price * 2.0


class Trader:

    def __init__(self):
        self.time_manager = TimeManager()
        self.symbol_manager = SymbolManager(max_symbols=-1, asset_filter_rate=0.05)
        self.data_manager = DataManagerFast(history_param={'period': 2000, 'bar_window': 1, 'min_num_bars': 480})
        self.strategy_manager = StrategyManagerFast()
        self.order_manager = OrderManager(one_time_invest_ratio=0.05, max_buy_per_min=2, max_ratio_per_asset=0.10)
        self.account = AccountLocal()
        self.account.set_cash(100000.00)
        self.prophecy_history = pd.DataFrame()
        self.file_name = None

    def run(self, start, end, file_name):
        self.initialize(start, end, file_name)

        while self.time_manager.is_within_period():
            if self.time_manager.is_market_open():
                recent = self.data_manager.update_recent_data(
                    self.symbol_manager.symbols, self.time_manager.current, self.time_manager.timezone
                )
                for symbol in self.account.positions.assets:
                    if symbol in recent:
                        self.account.positions.update_price(symbol, round(recent[symbol]['close'].iloc[-1],2))
                print(f"업데이트시간: {self.time_manager.current}")
                print(f"총평가가치: ${self.account.get_total_value()}, 현금: ${self.account.cash}, 보유종목_평가금액: ${self.account.positions.value}")
                if recent:
                    prophecy = self.strategy_manager.evaluate(self.data_manager.history, recent)
                    buy_list = prophecy[prophecy['buy']]['symbol'].tolist()
                    sell_list = prophecy[prophecy['sell']]['symbol'].tolist()
                    keep_list = prophecy[prophecy['keep_profit']]['symbol'].tolist()
                    if buy_list or sell_list or keep_list:
                        print(f"매수의견: {buy_list}, 매도의견: {sell_list}, 보유의견: {keep_list}")
                    self.order_manager.execute_orders(prophecy, self.prophecy_history)
                    print("time:",self.time_manager.current)
            if self.time_manager.current.minute >= 0:
                self.account.positions.print_positions()
            self.time_manager.advance_time()
        Printer.store_prophecy_history(self.prophecy_history, self.file_name)


    def initialize(self, start, end, file_name):
        self.time_manager.set_period(start, end)
        symbols = self.symbol_manager.initialize_symbols(self.time_manager.start)
        self.data_manager.fetch_history(symbols, self.time_manager.current, self.time_manager.timezone)
        self.strategy_manager.initialize_strategies(symbols)
        self.file_name = file_name + f"_{start}_{end}_{datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')}.csv"

if __name__ == "__main__":
    trader = Trader()
    file_name = "trader_local_maengja"
    trader.run('2024-07-01 09:30:00', '2024-07-15 16:00:00', file_name)