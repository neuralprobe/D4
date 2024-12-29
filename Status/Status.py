from os import access

import pandas as pd
from alpaca.trading.enums import QueryOrderStatus
from alpaca.trading.requests import GetOrdersRequest
from ApiAccess.ApiAccess import ClientType, ClientManager
from Common.Common import SingletonMeta, r2
from Common.Logger import Logger

class AccountBase(metaclass=SingletonMeta):
    """Base class for account management."""
    def __init__(self, acc_logfile=None, time_manager=None):
        self.cash = 0
        self.logger = Logger(acc_logfile)
        self.time = time_manager
        self.positions = None

    def get_total_value(self):
        pass

    def update(self, change = 0):
        pass

    def print(self):
        if not self.logger.initiated:
            self.logger("시간, 총평가가치, 현금, 평가금액, 보유종목, 수량, 평균가, 현재가, 추적손절가, 손절가, 손절지표")
            self.logger.initiated = True
        if len(self.positions.assets.keys()):
            for symbol in self.positions.assets.keys():
                self.logger(f"{self.time.current}, {r2(self.get_total_value())}, {r2(self.cash)}, {r2(self.positions.value)}, "
                            f"{symbol}, {r2(self.positions.assets[symbol]['qty'])}, {r2(float(self.positions.assets[symbol]['avg_price'])) }, "
                            f"{r2(float(self.positions.assets[symbol]['price']))}, {r2(float(self.positions.assets[symbol]['stop_trailing'])) }, "
                            f"{r2(float(self.positions.assets[symbol]['stop_value']))}, {self.positions.assets[symbol]['stop_key']}")
        else:
            self.logger(
                f"{self.time.current}, {r2(self.get_total_value())}, {r2(self.cash)}, {r2(self.positions.value)}, "
                f"None, {r2(0.0)}, {r2(0.0)}, "
                f"{r2(0.0)}, {r2(0.0)}, "
                f"{r2(0.0)}, None")


class AccountLocal(AccountBase):
    """Simulated account class."""
    def __init__(self, acc_logfile=None, time_manager=None):
        super().__init__(acc_logfile, time_manager)
        self.positions = PositionLocal(acc_logfile)

    def get_total_value(self):
        return self.cash + self.positions.value

    def set_cash(self, cash):
        self.cash = cash

    def update(self, change = 0):
        self.cash += change

class AccountLive(AccountBase):
    """Live account class."""
    def __init__(self, acc_logfile=None, time_manager=None):
        super().__init__(acc_logfile, time_manager)
        self.positions = PositionLive(acc_logfile)
        self.trading_client = ClientManager().get_client(ClientType.TRADE)
        self.order_list = OrderList(True)

    def get_total_value(self):
        self.positions.update()
        return self.cash + self.positions.value

    def update(self, change = 0):
        self.positions.update()
        self.cash = float(self.trading_client.get_account().cash)


class PositionBase(metaclass=SingletonMeta):
    """Base class for position management."""
    def __init__(self, logfile=None):
        self.assets = {}
        self.value = 0.0
        self.logger = Logger(logfile)

class PositionLocal(PositionBase):
    """Simulated position class."""
    def __init__(self, logfile=None):
        super().__init__(logfile)

    def add_new_asset(self, new_asset):
        """Add a new asset or update an existing one."""
        symbol = new_asset['symbol']
        self.value += new_asset['cost']
        if symbol in self.assets:
            self.assets[symbol]['price'] = new_asset['price']
            self.assets[symbol]['qty'] += new_asset['qty']
            self.assets[symbol]['market_value'] = new_asset['price'] * self.assets[symbol]['qty']
            self.assets[symbol]['cost'] += new_asset['cost']
            self.assets[symbol]['avg_price'] = self.assets[symbol]['cost'] / self.assets[symbol]['qty']
            self.assets[symbol]['stop_value'] = max(self.assets[symbol]['stop_value'], new_asset['stop_value'])
            self.assets[symbol]['stop_key'] = new_asset['stop_key']
            self.assets[symbol]['stop_trailing'] = max(self.assets[symbol]['stop_trailing'], new_asset['stop_trailing'])

        else:
            self.assets[symbol] = dict(time=new_asset['time'], price=new_asset['price'],
                                       avg_price=new_asset['cost'] / new_asset['qty'],
                                       qty=new_asset['qty'],
                                       market_value=new_asset['cost'], cost=new_asset['cost'],
                                       stop_value=new_asset['stop_value'], stop_key=new_asset['stop_key'],
                                       stop_trailing=new_asset['stop_trailing'])

    def remove_asset(self, symbol):
        """Remove an asset from the positions."""
        if symbol in self.assets:
            self.value -= self.assets[symbol]['market_value']
            del self.assets[symbol]

    def update_price(self, symbol, price):
        """Update the current price of a specific asset."""
        if symbol in self.assets:
            prev_market_value = self.assets[symbol]['market_value']
            new_market_value = price * self.assets[symbol]['qty']
            self.assets[symbol]['price'] = price
            self.assets[symbol]['market_value'] = new_market_value
            self.value = self.value + new_market_value - prev_market_value
        if not self.assets:
            self.value = 0.0


class PositionLive(PositionBase):
    """Live position class."""
    def __init__(self, logfile=None):
        super().__init__(logfile)
        self.trading_client = ClientManager().get_client(ClientType.TRADE)
        self.assets_info = {}
        self.Trailing = (1.0-0.01)

    def add_new_asset(self, new_asset):
        symbol = new_asset['symbol']
        self.assets_info[symbol] = dict(stop_value=new_asset['stop_value'], stop_key=new_asset['stop_key'], stop_trailing=new_asset['stop_trailing'])

    def remove_asset(self, symbol):
        """Remove an asset from the positions."""
        try:
            symbols = self.assets_info.keys()
            if symbol in symbols:
                del self.assets_info[symbol]
        except Exception as e:
            self.logger(f"remove_asset error occurred for {symbol}: {e}")
            x=1

    def update(self):
        positions = self.trading_client.get_all_positions()
        symbols = [asset.symbol for asset in positions]
        self.value = 0.0
        try:
            for asset in positions:
                self.value += float(asset.market_value)
                if asset.symbol in self.assets:
                    asset_info = self.assets[asset.symbol]
                    asset_info['price'] = float(asset.current_price)
                    asset_info['avg_price'] = float(asset.avg_entry_price)
                    asset_info['qty'] = float(asset.qty)
                    asset_info['market_value'] = float(asset.market_value)
                    asset_info['cost'] = float(asset.cost_basis)
                    if asset.symbol in self.assets_info:
                        asset_info['stop_value'] = self.assets_info[asset.symbol]['stop_value']
                        asset_info['stop_key'] = self.assets_info[asset.symbol]['stop_key']
                        asset_info['stop_trailing'] = self.assets_info[asset.symbol]['stop_trailing']
                    else:
                        asset_info['stop_value'] = 0.0
                        asset_info['stop_key'] = ''
                        asset_info['stop_trailing'] = float(asset.current_price) * self.Trailing
                    asset_info['valid'] = True
                else:
                    if asset.symbol in self.assets_info:
                        self.assets[asset.symbol] = dict(time=pd.Timestamp.now(tz='America/New_York'),
                                                         price=float(asset.current_price), qty=float(asset.qty),
                                                         cost=float(asset.cost_basis), avg_price=float(asset.avg_entry_price),
                                                         stop_value=self.assets_info[asset.symbol]['stop_value'],
                                                         stop_key=self.assets_info[asset.symbol]['stop_key'],
                                                         stop_trailing=self.assets_info[asset.symbol]['stop_trailing'],
                                                         valid=True)
                    else:
                        self.assets[asset.symbol] = dict(time=pd.Timestamp.now(tz='America/New_York'),
                                                         price=float(asset.current_price), qty=float(asset.qty),
                                                         cost=float(asset.cost_basis),
                                                         avg_price=float(asset.avg_entry_price),
                                                         stop_value=0.0,
                                                         stop_key='',
                                                         stop_trailing= float(asset.current_price) * self.Trailing,
                                                         valid=True)
        except Exception as e:
            self.logger(f"remove_asset error 1 occurred : {e}")
            x=1
        try:
            asset_symbols=list(self.assets.keys())
            for symbol in asset_symbols:
                if symbol not in symbols:
                    del self.assets[symbol]
        except Exception as e:
            self.logger(f"remove_asset error 2 occurred for {symbol}: {e}")
            x=1

class OrderList(metaclass=SingletonMeta):
    """Class for managing trading orders."""
    def __init__(self, live):
        self.trading_client = ClientManager().get_client(ClientType.TRADE)
        self.orders = {}
        self.live = live

    def update(self):
        if not self.live:
            return
        del_arr = []
        for symbol, uid in self.orders.items():
            my_order = self.trading_client.get_order_by_client_id(uid)
            assert(my_order.symbol == symbol)
            if my_order.filled_at!=None:
                del_arr.append(symbol)
        for el in del_arr:
            del self.orders[el]
