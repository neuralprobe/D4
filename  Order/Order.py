from abc import ABC, abstractmethod
import math
import time
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from Status.Status import AccountLocal, AccountLive, PositionLocal, PositionLive
from ApiAccess.ApiAccess import ClientType, ClientManager


class SingletonMeta(type):
    """A metaclass for Singleton pattern."""
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

class BuyerBase(metaclass=SingletonMeta, ABC):
    """Base class for buying operations."""

    def __init__(self, trade_cfg):
        self.one_time_invest_ratio = trade_cfg['one_time_invest_ratio'];

    @abstractmethod
    def buy(self, prophecy, buy_symbol):
        pass


class BuyerLocal(BuyerBase):

    def __init__(self, trade_cfg):
        super().__init__(trade_cfg)
        self.account = AccountLocal()

    def buy(self, prophecy, buy_symbol):
        if not len(prophecy):
            return
        buy_symbol_df = prophecy[prophecy['symbol'] == buy_symbol]
        price = buy_symbol_df['price'].iloc[-1]
        one_time_invest = math.floor(self.account.get_total_value() * self.one_time_invest_ratio)
        qty = min(math.floor(one_time_invest / price), math.floor(self.account.cash / price))
        if qty == 0:
            return
        cost = price * qty
        market_order_data = {
            'time': buy_symbol_df['time'].iloc[-1],
            'symbol': buy_symbol,
            'price': price,
            'qty': qty,
            'cost': cost,
            'stop_loss': buy_symbol_df['stop_loss'].iloc[-1],
            'stop_loss_name': buy_symbol_df['stop_loss_name'].iloc[-1]
        }
        self.account.positions.add_new_asset(market_order_data)
        self.account.update(-cost)


class BuyerLive(BuyerBase):

    def __init__(self, trade_cfg):
        super().__init__(trade_cfg)
        self.account = AccountLive()

    def buy(self, prophecy, buy_symbol):
        if not len(prophecy):
            return
        buy_symbol_df = prophecy[prophecy['symbol'] == buy_symbol]
        price = buy_symbol_df['current_close'].iloc[-1]
        one_time_invest = math.floor(self.account.get_total_value() * self.one_time_invest_ratio)
        qty = min(math.floor(one_time_invest / price), math.floor(self.account.cash / price))
        if qty == 0:
            return
        cost = price * qty
        market_order_data = MarketOrderRequest(
            symbol=buy_symbol,
            qty=qty,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY
        )
        market_order_info = dict(stop_loss=buy_symbol_df['stop_loss'].iloc[-1],
                                 stop_loss_name=buy_symbol_df['stop_loss_name'].iloc[-1])
        self.trading_client.submit_order(order_data=market_order_data)
        sleep_counter = 0
        time.sleep(0.5)
        while self.account.order_list.check_open() > 0:
            time.sleep(1)
            if sleep_counter >= 10:
                self.trading_client.cancel_orders()
                return
        self.account.positions.add_new_asset(market_order_info)
        self.account.update()


class SellerBase(metaclass=SingletonMeta, ABC):
    """Base class for selling operations."""

    def __init__(self, trade_cfg):
        pass

    @abstractmethod
    def sell(self, prophecy, sell_symbol):
        pass

class SellerLocal(SellerBase):

    def __init__(self, trade_cfg):
        super().__init__(trade_cfg)
        self.account = AccountLocal()

    def sell(self, prophecy, sell_symbol):
        if not len(prophecy):
            return
        sell_symbol_df = prophecy[prophecy['symbol'] == sell_symbol]
        time = sell_symbol_df['time'].iloc[-1]
        price = sell_symbol_df['current_close'].iloc[-1]

        qty = self.portfolio.local_portfolio[sell_symbol]['qty']
        sell_value = price * qty

        self.update_cash()

        if live:
            # 실제 주문
            market_order_data = MarketOrderRequest(
                symbol=sell_symbol,
                qty=qty,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY
            )
            self.trading_client.submit_order(order_data=market_order_data)
        else:
            # 시뮬레이션 주문
            market_order_data = {
                'time': time,
                'symbol': sell_symbol,
                'price': price,
                'qty': qty,
                'cost': 0,
                'stop_loss': 0
            }
            self.portfolio.remove_asset(market_order_data)

        self.account.earn_local_cash(sell_value)
        self.update_cash()


class SellerLive(SellerBase):

    def __init__(self, trade_cfg):
        super().__init__(trade_cfg)
        self.account = AccountLive()

    def sell(self, prophecy, sell_symbol, live=False):
        if not len(prophecy):
            return
        sell_symbol_df = prophecy[prophecy['symbol'] == sell_symbol]
        time = sell_symbol_df['time'].iloc[-1]
        price = sell_symbol_df['current_close'].iloc[-1]

        qty = self.portfolio.local_portfolio[sell_symbol]['qty']
        sell_value = price * qty

        self.update_cash()

        if live:
            # 실제 주문
            market_order_data = MarketOrderRequest(
                symbol=sell_symbol,
                qty=qty,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY
            )
            self.trading_client.submit_order(order_data=market_order_data)
        else:
            # 시뮬레이션 주문
            market_order_data = {
                'time': time,
                'symbol': sell_symbol,
                'price': price,
                'qty': qty,
                'cost': 0,
                'stop_loss': 0
            }
            self.portfolio.remove_asset(market_order_data)

        self.account.earn_local_cash(sell_value)
        self.update_cash()
