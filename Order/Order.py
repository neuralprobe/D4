import math
import time
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from Status.Status import AccountLocal, AccountLive
from Common.Common import SingletonMeta, r2


class BuyerBase(metaclass=SingletonMeta):
    """Base class for buying operations."""

    def __init__(self, trade_cfg, logger, time_manager):
        self.one_time_invest_ratio = trade_cfg['one_time_invest_ratio']
        self.logger = logger
        self.time_manager = time_manager

    def buy(self, prophecy, buy_symbol):
        pass

    def _get_qty(self, price):
        pass

class BuyerLocal(BuyerBase):

    def __init__(self, trade_cfg, logger, time_manager):
        super().__init__(trade_cfg, logger, time_manager)
        self.account = AccountLocal()

    def buy(self, prophecy, buy_symbol, order_list):
        if not len(prophecy):
            return False
        buy_symbol_df = prophecy[prophecy['symbol'] == buy_symbol]
        price = buy_symbol_df['price'].iloc[-1]
        qty = self._get_qty(price)
        if qty == 0:
            return False
        cost = price * qty
        market_order_data = dict(time=buy_symbol_df['time'].iloc[-1], symbol=buy_symbol, price=price,
                                 qty=qty, cost=cost,
                                 stop_value=buy_symbol_df['stop_value'].iloc[-1],
                                 stop_key=buy_symbol_df['stop_key'].iloc[-1],
                                 stop_trailing=buy_symbol_df['stop_trailing'].iloc[-1])
        if not self.logger.initiated:
            self.logger("시간, 매매, 종목, 수량, 현재가, 평균가, 현금변화, 이익")
            self.logger.initiated = True
        if buy_symbol in self.account.positions.assets:
            self.logger(f"{self.time_manager.current.tz_localize(None)}, BUY, {buy_symbol}, {r2(qty)}, {r2(price)}, "
                        f"{r2(self.account.positions.assets[buy_symbol]['avg_price'])}, "
                        f"{-r2(cost)}, {r2(0.0)}")
        else:
            self.logger(f"{self.time_manager.current.tz_localize(None)}, BUY, {buy_symbol}, {r2(qty)}, {r2(price)}, "
                        f"{r2(price)}, "
                        f"{-r2(cost)}, {r2(0.0)}")
        self.account.positions.add_new_asset(market_order_data)
        self.account.update(-cost)
        return True

    def _get_qty(self, price):
        one_time_invest = math.floor(self.account.get_total_value() * self.one_time_invest_ratio)
        qty = math.floor(max(min(math.floor(one_time_invest / price), math.floor(self.account.cash / price)),0.0))
        return qty


class BuyerLive(BuyerBase):

    def __init__(self, trade_cfg, logger, time_manager):
        super().__init__(trade_cfg, logger, time_manager)
        self.account = AccountLive()

    def buy(self, prophecy, buy_symbol, order_list):
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
            time_in_force=TimeInForce.DAY,
        )
        market_order_info = dict(symbol=buy_symbol,
                                 stop_value=buy_symbol_df['stop_value'].iloc[-1],
                                 stop_key=buy_symbol_df['stop_key'].iloc[-1],
                                 stop_trailing=buy_symbol_df['stop_trailing'].iloc[-1])
        try:
            order=self.account.trading_client.submit_order(order_data=market_order_data)
            order_list.orders[buy_symbol]=order.client_order_id
        except Exception as e:
            self.logger(f"Buy error occurred for {buy_symbol}: {e}")

        if not self.logger.initiated:
            self.logger("시간, 매매, 종목, 수량, 현재가, 평균가, 현금변화, 이익")
            self.logger.initiated = True
        if buy_symbol in self.account.positions.assets:
            self.logger(f"{self.time_manager.current.tz_localize(None)}, BUY, {buy_symbol}, {r2(qty)}, {r2(price)}, "
                        f"{r2(self.account.positions.assets[buy_symbol]['avg_price'])}, "
                        f"{-r2(cost)}, {r2(0.0)}")
        else:
            self.logger(f"{self.time_manager.current.tz_localize(None)}, BUY, {buy_symbol}, {r2(qty)}, {r2(price)}, "
                        f"{r2(price)}, "
                        f"{-r2(cost)}, {r2(0.0)}")
        self.account.positions.add_new_asset(market_order_info)
        time.sleep(1)
        self.account.update()

    def _get_qty(self, price):
        one_time_invest = math.floor(self.account.get_total_value() * self.one_time_invest_ratio)
        qty = math.floor(max(min(math.floor(one_time_invest / price), math.floor(self.account.cash / price)),0.0))
        return qty


class SellerBase(metaclass=SingletonMeta):
    """Base class for selling operations."""

    def __init__(self, logger, time_manager):
        self.logger = logger
        self.time_manager = time_manager

    def sell(self, prophecy, sell_symbol):
        pass


class SellerLocal(SellerBase):

    def __init__(self, logger, time_manager):
        super().__init__(logger, time_manager)
        self.account = AccountLocal()

    def sell(self, prophecy, sell_symbol, order_list):
        if not len(prophecy) or sell_symbol not in self.account.positions.assets.keys():
            return False
        price = self.account.positions.assets[sell_symbol]['price']
        qty = self.account.positions.assets[sell_symbol]['qty']
        market_value = self.account.positions.assets[sell_symbol]['market_value']
        cost = self.account.positions.assets[sell_symbol]['cost']
        avg_price = self.account.positions.assets[sell_symbol]['avg_price']
        if not self.logger.initiated:
            self.logger("시간, 매매, 종목, 수량, 현재가, 평균가, 현금변화, 이익")
            self.logger.initiated = True
        self.logger(f"{self.time_manager.current.tz_localize(None)}, SELL, {sell_symbol}, {r2(qty)}, {r2(price)}, "
                    f"{r2(avg_price)}, "
                    f"{r2(market_value)}, {r2(market_value-cost)}")
        self.account.positions.remove_asset(sell_symbol)
        self.account.update(market_value)
        return True

class SellerLive(SellerBase):

    def __init__(self, logger, time_manager):
        super().__init__(logger, time_manager)
        self.account = AccountLive()

    def sell(self, prophecy, sell_symbol, order_list):
        try:
            if not len(prophecy) or sell_symbol not in self.account.positions.assets.keys():
                return False
            self.account.update()
        except Exception as e:
            self.logger(f"Sell error 1 occurred for {sell_symbol}: {e}")
            x = 1
        try:
            price = self.account.positions.assets[sell_symbol]['price']
            qty = self.account.positions.assets[sell_symbol]['qty']
            market_value = self.account.positions.assets[sell_symbol]['market_value']
            cost = self.account.positions.assets[sell_symbol]['cost']
            avg_price = self.account.positions.assets[sell_symbol]['avg_price']
        except Exception as e:
            self.logger(f"Sell error 2 occurred for {sell_symbol}: {e}")
            x = 1
        try:
            market_order_data = MarketOrderRequest(
                symbol=sell_symbol,
                qty=qty,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY
            )

            order = self.account.trading_client.submit_order(order_data=market_order_data)
            order_list.orders[sell_symbol] = order.client_order_id
        except Exception as e:
            self.logger(f"Sell error 3 occurred for {sell_symbol}: {e}")
            x = 1
        try:
            if not self.logger.initiated:
                self.logger("시간, 매매, 종목, 수량, 현재가, 평균가, 현금변화, 이익")
                self.logger.initiated = True
            self.logger(f"{self.time_manager.current.tz_localize(None)}, SELL, {sell_symbol}, {r2(qty)}, {r2(price)}, "
                        f"{r2(avg_price)}, "
                        f"{r2(market_value)}, {r2(market_value - cost)}")
            time.sleep(1)
            self.account.positions.remove_asset(sell_symbol)
            self.account.update()
            return True

        except Exception as e:
            self.logger(f"Sell error 4 occurred for {sell_symbol}: {e}")
            x=1