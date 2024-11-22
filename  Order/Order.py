import math
from ApiAccess.ApiAccess import ClientType, ClientManager
from Status.Status import Account, Portfolio, Account_Live, Portfolio_Live

class Order:
    """Base class for trading functionality."""
    def __init__(self, account_cls, portfolio_cls, invest_ratio):
        self.account = account_cls()
        self.portfolio = portfolio_cls()
        self.one_time_invest_ratio = invest_ratio
        self.one_time_invest = math.ceil(self.get_total_value()[0] * self.one_time_invest_ratio)
        self.cash = self.account.get_local_cash()
        self.trading_client = get_client('trade')

    def get_total_value(self):
        """Calculate the total value of cash and portfolio."""
        local_cash = self.account.get_local_cash()
        local_portfolio_value = self.portfolio.get_local_portfolio_value()
        self.total_value = local_cash + local_portfolio_value
        return self.total_value, local_cash, local_portfolio_value

    def update_cash(self):
        """Update the current cash value."""
        self.cash = self.account.get_local_cash()


class BuyerBase(Order):
    """Base class for buying logic."""
    def buy(self, current_prophecy, buy_symbol, live=False):
        if not len(current_prophecy):
            return
        buy_symbol_df = current_prophecy[current_prophecy['symbol'] == buy_symbol]
        time = buy_symbol_df['time'].iloc[-1]
        price = buy_symbol_df['current_close'].iloc[-1]
        stop_loss = buy_symbol_df['stop_loss'].iloc[-1]
        stop_loss_name = buy_symbol_df['stop_loss_name'].iloc[-1]

        qty = self.one_time_invest // price if price < self.one_time_invest else 1
        cost = price * qty

        self.update_cash()

        if self.cash < cost:
            # Not enough cash
            return

        # Handle live or simulation buy
        if live:
            market_order_data = MarketOrderRequest(
                symbol=buy_symbol,
                qty=qty,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY
            )
            self.trading_client.submit_order(order_data=market_order_data)
        else:
            market_order_data = {
                'time': time,
                'symbol': buy_symbol,
                'price': price,
                'qty': qty,
                'cost': cost,
                'stop_loss': stop_loss,
                'stop_loss_name': stop_loss_name
            }
            self.portfolio.add_new_asset(market_order_data)

        # Update cash after buying
        self.account.spend_local_cash(cost)
        self.update_cash()


class Buyer(BuyerBase):
    def __init__(self):
        super().__init__(Account, Portfolio, invest_ratio=0.10)

class Buyer_Live(BuyerBase):
    def __init__(self):
        super().__init__(Account_Live, Portfolio_Live, invest_ratio=0.05)

class SellerBase(Order):
    """Base class for selling logic."""
    def sell(self, current_prophecy, sell_symbol, live=False):
        if not len(current_prophecy):
            return
        sell_symbol_df = current_prophecy[current_prophecy['symbol'] == sell_symbol]
        time = sell_symbol_df['time'].iloc[-1]
        price = sell_symbol_df['current_close'].iloc[-1]
        qty = self.portfolio.local_portfolio[sell_symbol]['qty']
        sell_value = price * qty

        self.update_cash()

        # Handle live or simulation sell
        if live:
            market_order_data = MarketOrderRequest(
                symbol=sell_symbol,
                qty=qty,
                side=OrderSide.SELL,
                time_in_force=TimeInForce.DAY
            )
            self.trading_client.submit_order(order_data=market_order_data)
        else:
            market_order_data = {
                'time': time,
                'symbol': sell_symbol,
                'price': price,
                'qty': qty,
                'cost': 0,
                'stop_loss': 0
            }
            self.portfolio.remove_asset(market_order_data)

        # Update cash after selling
        self.account.earn_local_cash(sell_value)
        self.update_cash()

class Seller(SellerBase):
    def __init__(self):
        super().__init__(Account, Portfolio, invest_ratio=0.10)


class Seller_Live(SellerBase):
    def __init__(self):
        super().__init__(Account_Live, Portfolio_Live, invest_ratio=0.05)





