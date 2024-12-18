import pandas as pd
import pytz
from datetime import datetime, timedelta
from Common.Common import Printer, setup_logging, r2
from Status.Status import AccountLive
import schedule
import time
from Trader.Managers import TimeManager, SymbolManager, DataManagerFast, StrategyManagerFast, OrderManager

class TraderLive:

    def __init__(self):
        self.time_manager = TimeManager()
        self.symbol_manager = SymbolManager(max_symbols=-1, asset_filter_rate=0.05, renew_symbol=True, max_workers=12)
        self.data_manager = DataManagerFast(history_param={'period': 2000, 'bar_window': 1, 'min_num_bars': 480}, max_workers=12)
        self.strategy_manager = StrategyManagerFast()
        self.order_manager = OrderManager(live = True, one_time_invest_ratio=0.05, max_buy_per_min=2, max_ratio_per_asset=0.10)
        self.account = AccountLive()
        self.account.update()
        self.prophecy_history = pd.DataFrame()
        self.csv_name = None

    def run(self, start, end, file_name):

        self.initialize(start, end, file_name)

        schedule.clear()
        schedule.every().minute.at(":05").do(self._live_trade)

        self.time_manager.sync_current()
        while self.time_manager.before_end():
            try:
                schedule.run_pending()
            except Exception as e:
                print(f"Error occurred: {e}")
            time.sleep(1)
            self.time_manager.sync_current()

        Printer.store_prophecy_history(self.prophecy_history, self.csv_name)

    def _live_trade(self):
        if self.time_manager.is_market_open():
            self.time_manager.sync_current()
            recent = self.data_manager.update_recent_data(
                self.symbol_manager.symbols, self.time_manager.current, self.time_manager.timezone
            )
            self.account.update()
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
                print("time:",self.time_manager.current)
            if self.time_manager.current.minute >= 0:
                self.account.update()
                self.account.positions.print_positions()


    def initialize(self, start, end, file_name):
        self.time_manager.set_period(start, end)
        self.time_manager.sync_current()
        symbols = self.symbol_manager.initialize_symbols(self.time_manager.current)
        self.time_manager.sync_current()
        self.data_manager.fetch_history(symbols, self.time_manager.current, self.time_manager.timezone)
        self.time_manager.sync_current()
        self.strategy_manager.initialize_strategies(symbols)
        self.csv_name = file_name + f"_{start}_{end}_{datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')}.csv"


if __name__ == "__main__":
    trader = TraderLive()

    file_name = "trader_live_maengja"
    start = datetime.now()
    end = datetime.now()+timedelta(hours=7)

    log_file = setup_logging(file_name, start, end)

    trader.run(start, end, file_name)

    log_file.close()