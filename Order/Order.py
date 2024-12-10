import math
import time
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from Status.Status import AccountLocal, AccountLive
from Common.Common import SingletonMeta, r2


class BuyerBase(metaclass=SingletonMeta):
    """Base class for buying operations."""

    def __init__(self, trade_cfg):
        self.one_time_invest_ratio = trade_cfg['one_time_invest_ratio']

    def buy(self, prophecy, buy_symbol):
        pass

    def _get_qty(self, price):
        pass

class BuyerLocal(BuyerBase):

    def __init__(self, trade_cfg):
        super().__init__(trade_cfg)
        self.account = AccountLocal()

    def buy(self, prophecy, buy_symbol):
        if not len(prophecy):
            return False
        buy_symbol_df = prophecy[prophecy['symbol'] == buy_symbol]
        price = buy_symbol_df['price'].iloc[-1]
        qty = self._get_qty(price)
        if qty == 0:
            return False
        cost = price * qty
        market_order_data = dict(time=buy_symbol_df['time'].iloc[-1], symbol=buy_symbol, price=price,
                                 qty=qty, cost=cost, stop_loss=buy_symbol_df['stop_loss'].iloc[-1],
                                 stop_loss_name=buy_symbol_df['stop_loss_name'].iloc[-1])
        if buy_symbol in self.account.positions.assets:
            print(f"매수:{buy_symbol}, 수량:{r2(qty)}, 매수가:{r2(price)}, "
                  f"평균가:{r2(self.account.positions.assets[buy_symbol]['avg_price'])}, "
                  f"비용 :{r2(cost)}")
        else:
            print(f"매수:{buy_symbol}, 수량:{r2(qty)}, 매수가:{r2(price)}, 비용 :{r2(cost)}")
        self.account.positions.add_new_asset(market_order_data)
        self.account.update(-cost)
        return True

    def _get_qty(self, price):
        one_time_invest = math.floor(self.account.get_total_value() * self.one_time_invest_ratio)
        qty = min(math.floor(one_time_invest / price), math.floor(self.account.cash / price))
        return qty


class BuyerLive(BuyerBase):

    def __init__(self, trade_cfg):
        super().__init__(trade_cfg)
        self.account = AccountLive()

    def buy(self, prophecy, buy_symbol):
        if not len(prophecy):
            return False
        self.account.update()
        buy_symbol_df = prophecy[prophecy['symbol'] == buy_symbol]
        price = buy_symbol_df['price'].iloc[-1]
        qty = self._get_qty(price)
        if qty == 0:
            return False
        cost = price * qty
        market_order_data = MarketOrderRequest(
            symbol=buy_symbol,
            qty=qty,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY
        )
        market_order_info = dict(stop_loss=buy_symbol_df['stop_loss'].iloc[-1],
                                 stop_loss_name=buy_symbol_df['stop_loss_name'].iloc[-1])
        self.account.trading_client.submit_order(order_data=market_order_data)
        if buy_symbol in self.account.positions.assets:
            print(f"매수:{buy_symbol}, 수량:{r2(qty)}, 매수가:{r2(price)}, "
                  f"평균가:{r2(self.account.positions.assets[buy_symbol]['avg_price'])}, "
                  f"비용 :{r2(cost)}")
        else:
            print(f"매수:{buy_symbol}, 수량:{r2(qty)}, 매수가:{r2(price)}, 비용 :{r2(cost)}")
        self.account.positions.add_new_asset(market_order_info)
        time.sleep(1)
        self.account.update()

    def _get_qty(self, price):
        one_time_invest = math.floor(self.account.get_total_value() * self.one_time_invest_ratio)
        qty = min(math.floor(one_time_invest / price), math.floor(self.account.cash / price))
        return qty


class SellerBase(metaclass=SingletonMeta):
    """Base class for selling operations."""

    def __init__(self):
        pass

    def sell(self, prophecy, sell_symbol):
        pass


class SellerLocal(SellerBase):

    def __init__(self):
        super().__init__()
        self.account = AccountLocal()

    def sell(self, prophecy, sell_symbol):
        if not len(prophecy) or sell_symbol not in self.account.positions.assets.keys():
            return False
        price = self.account.positions.assets[sell_symbol]['price']
        qty = self.account.positions.assets[sell_symbol]['qty']
        market_value = self.account.positions.assets[sell_symbol]['market_value']
        cost = self.account.positions.assets[sell_symbol]['cost']
        avg_price = self.account.positions.assets[sell_symbol]['avg_price']
        print(f"매도:{sell_symbol}, 수량:{r2(qty)}, 매도가:{r2(price)}, 평균가:{r2(avg_price)}, 이익:{r2(market_value-cost)}")
        self.account.positions.remove_asset(sell_symbol)
        self.account.update(market_value)
        return True

class SellerLive(SellerBase):

    def __init__(self):
        super().__init__()
        self.account = AccountLive()

    def sell(self, prophecy, sell_symbol):
        if not len(prophecy) or sell_symbol not in self.account.positions.assets.keys():
            return False
        self.account.update()
        price = self.account.positions.assets[sell_symbol]['price']
        qty = self.account.positions.assets[sell_symbol]['qty']
        market_value = self.account.positions.assets[sell_symbol]['market_value']
        cost = self.account.positions.assets[sell_symbol]['cost']
        avg_price = self.account.positions.assets[sell_symbol]['avg_price']
        market_order_data = MarketOrderRequest(
            symbol=sell_symbol,
            qty=qty,
            side=OrderSide.SELL,
            time_in_force=TimeInForce.DAY
        )
        self.account.trading_client.submit_order(order_data=market_order_data)
        print(f"매도:{sell_symbol}, 수량:{r2(qty)}, 매도가:{r2(price)}, 평균가:{r2(avg_price)}, 이익:{r2(market_value - cost)}")
        time.sleep(1)
        self.account.positions.remove_asset(sell_symbol)
        self.account.update()
        return True