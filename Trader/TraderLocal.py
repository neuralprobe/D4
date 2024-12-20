import pandas as pd
import pytz
from datetime import datetime
from Common.Common import  Printer, setup_logging, r2
from Status.Status import AccountLocal
from Trader.Managers import TimeManager, SymbolManager, DataManagerFast, StrategyManagerFast, OrderManager

class TraderLocal:

    def __init__(self):
        self.time_manager = TimeManager()
        self.symbol_manager = SymbolManager(max_symbols=-1, asset_filter_rate=0.05, renew_symbol=True, max_workers=16)
        self.data_manager = DataManagerFast(history_param={'period': 2000, 'bar_window': 1, 'min_num_bars': 480}, max_workers=16)
        self.strategy_manager = StrategyManagerFast()
        self.order_manager = OrderManager(live=False, one_time_invest_ratio=0.05, max_buy_per_min=2, max_ratio_per_asset=0.10)
        self.account = AccountLocal()
        self.account.set_cash(100000.00)
        self.prophecy_history = pd.DataFrame()
        self.csv_name = None

    def run(self, start, end, file_name):

        self.initialize(start, end, file_name)

        while self.time_manager.before_end():
            self._local_trade()
            self.time_manager.advance_current()

        Printer.store_prophecy_history(self.prophecy_history, self.csv_name)

    def _local_trade(self):
        if self.time_manager.is_market_open():
            recent = self.data_manager.update_recent_data(
                self.symbol_manager.symbols, self.time_manager.current, self.time_manager.timezone
            )
            for symbol in self.account.positions.assets:
                if symbol in recent:
                    self.account.positions.update_price(symbol, recent[symbol]['close'].iloc[-1])
            print(f"업데이트시간: {self.time_manager.current}")
            print(f"총평가가치: ${r2(self.account.get_total_value())}, 현금: ${r2(self.account.cash)}, 보유종목_평가금액: ${r2(self.account.positions.value)}")
            if recent:
                prophecy = self.strategy_manager.evaluate(self.data_manager.history, recent)
                buy_list = prophecy[prophecy['buy']]['symbol'].tolist()
                sell_list = prophecy[prophecy['sell']]['symbol'].tolist()
                keep_list = prophecy[prophecy['keep_profit']]['symbol'].tolist()
                if buy_list or sell_list or keep_list:
                    print(f"매수의견: {buy_list}, 매도의견: {sell_list}, 보유의견: {keep_list}")
                self.order_manager.execute_orders(prophecy, self.prophecy_history)
                print("time:", self.time_manager.current)
            if self.time_manager.current.minute >= 0:
                self.account.positions.print_positions()

    def initialize(self, start, end, file_name):
        self.time_manager.set_period(start, end)
        symbols = self.symbol_manager.initialize_symbols(self.time_manager.start)
        self.data_manager.fetch_history(symbols, self.time_manager.current, self.time_manager.timezone)
        self.strategy_manager.initialize_strategies(symbols)
        self.csv_name = file_name + f"_{start}_{end}_{datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')}.csv"
        self.csv_name = self.csv_name.replace(":", "-")

if __name__ == "__main__":
    trader = TraderLocal()

    file_name = "trader_local_maengja"
    start = '2024-06-01 09:30:00'
    end = '2024-06-30 16:00:00'

    log_file = setup_logging(file_name, start, end)

    trader.run(start, end, file_name)

    log_file.close()