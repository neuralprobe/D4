import pandas as pd
import pytz
from datetime import datetime, timedelta
from Common.Common import Printer, r2
from Status.Status import AccountLive
import schedule
import time
from Trader.Managers import TimeManager, SymbolManager, DataManagerFast, StrategyManagerFast, OrderManager
from Common.Logger import Logger, search_and_export_to_excel

class TraderLive:

    def __init__(self):
        self.time_manager = TimeManager()
        self.symbol_manager = SymbolManager(max_symbols=-1, asset_filter_rate=0.05, renew_symbol=True, max_workers=12)
        self.data_manager = DataManagerFast(history_param={'period': 2000, 'bar_window': 1, 'min_num_bars': 480}, max_workers=12)

        self.logger = None
        self.account = None
        self.order_manager = None
        self.strategy_manager = StrategyManagerFast()
        self.prophecy_history = pd.DataFrame()

        self.prophecy_log_file = None
        self.trader_log_file = None
        self.order_log_file = None
        self.account_log_file = None
        self.position_log_file = None

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

        Printer.store_prophecy_history(self.prophecy_history, self.prophecy_log_file)

    def _live_trade(self):
        if self.time_manager.is_market_open():
            self.time_manager.sync_current()
            recent = self.data_manager.update_recent_data(
                self.symbol_manager.symbols, self.time_manager.current, self.time_manager.timezone
            )
            self.account.update()
            if recent:
                prophecy = self.strategy_manager.evaluate(self.data_manager.history, recent)
                buy_list = prophecy[prophecy['buy']]['symbol'].tolist()
                sell_list = prophecy[prophecy['sell']]['symbol'].tolist()
                keep_list = prophecy[prophecy['keep_profit']]['symbol'].tolist()
                if buy_list or sell_list or keep_list:
                    if not self.logger.initiated:
                        self.logger("시간, 의견, 종목,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,")
                        self.logger.initiated = True
                    self.logger(f"{self.time_manager.current.tz_localize(None)}, BUY,",", ".join(buy_list))
                    self.logger(f"{self.time_manager.current.tz_localize(None)}, SELL,",", ".join(sell_list))
                    self.logger(f"{self.time_manager.current.tz_localize(None)}, KEEP,",", ".join(keep_list))
                self.order_manager.execute_orders(prophecy, self.prophecy_history)
            self.account.update()
            self.account.print()


    def initialize(self, start, end, file_name):
        self.time_manager.set_period(start, end)
        self.time_manager.sync_current()
        symbols = self.symbol_manager.initialize_symbols(self.time_manager.current)
        self.time_manager.sync_current()
        self.data_manager.fetch_history(symbols, self.time_manager.current, self.time_manager.timezone)

        self.prophecy_log_file = file_name + "_prophecy" + f"_{start}_{end}_{datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H-%M-%S')}.csv"
        self.prophecy_log_file = self.prophecy_log_file.replace(":", "-")
        self.trader_log_file = file_name + "_trader" + f"_{start}_{end}_{datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H-%M:%S')}.csv"
        self.trader_log_file = self.trader_log_file.replace(":", "-")
        self.order_log_file = file_name + "_order" + f"_{start}_{end}_{datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')}.csv"
        self.order_log_file = self.order_log_file.replace(":", "-")
        self.account_log_file = file_name + "_account" + f"_{start}_{end}_{datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')}.csv"
        self.account_log_file = self.account_log_file.replace(":", "-")

        self.time_manager.sync_current()
        self.logger = Logger(self.trader_log_file)
        self.account = AccountLive(self.account_log_file, self.time_manager)
        self.account.update()
        self.order_manager = OrderManager(live = True, one_time_invest_ratio=0.05, max_buy_per_min=2, max_ratio_per_asset=0.10, logfile=self.order_log_file, time_manager=self.time_manager)
        self.strategy_manager.initialize_strategies(symbols)

if __name__ == "__main__":
    trader = TraderLive()

    file_name = "trader_live_maengja"
    start = datetime.now(pytz.timezone('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")
    end = (datetime.now(pytz.timezone('America/New_York'))+timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

    trader.run(start, end, file_name)

    Logger.close_all()

    search_and_export_to_excel(file_name, start.replace(":", "-"), end.replace(":", "-"))