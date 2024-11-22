from abc import ABC, abstractmethod
from ApiAccess.ApiAccess import ClientType, ClientManager


class SingletonMeta(type):
    """A metaclass for Singleton pattern."""
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class AccountBase(metaclass=SingletonMeta, ABC):
    """Base class for account management."""
    def __init__(self):
        self.client = ClientManager()
        self.trading_client = self.client.get_client(ClientType.TRADE)
        self.cash;

    @abstractmethod
    def get_cash(self):
        pass

    def update(self, change = 0):
        pass


class Account_Local(AccountBase):
    """Simulated account class."""
    def __init__(self):
        super().__init__()

    def get_local_cash(self):
        """Get the local cash value."""
        return self.local_cash

    def spend_local_cash(self, amount):
        """Decrease the local cash by a given amount."""
        self.local_cash -= amount

    def earn_local_cash(self, amount):
        """Increase the local cash by a given amount."""
        self.local_cash += amount


class Account_Live(AccountBase):
    """Live account class."""
    def __init__(self):
        super().__init__()
        self.local_cash = self.cash  # Sync local cash with remote cash


class PortfolioBase(metaclass=SingletonMeta):
    """Base class for portfolio management."""
    def __init__(self):
        self.trading_client = get_client('trade')
        self.local_portfolio = {}
        self.local_value = 0.0

    def update(self):
        """Update the portfolio from the remote API."""
        self.portfolio = self.trading_client.get_all_positions()

    def add_new_asset(self, asset):
        """Add a new asset or update an existing one."""
        symbol = asset['symbol']
        if symbol in self.local_portfolio:
            existing = self.local_portfolio[symbol]
            existing['qty'] += asset['qty']
            existing['cost'] += asset['cost']
            existing['avg_price'] = existing['cost'] / existing['qty']
            existing['stop_loss'] = max(existing['stop_loss'], asset['stop_loss'])
            existing['stop_loss_name'] = asset['stop_loss_name']
            existing['current_close'] = asset['price']
        else:
            self.local_portfolio[symbol] = {
                'time': asset['time'],
                'qty': asset['qty'],
                'cost': asset['cost'],
                'avg_price': asset['cost'] / asset['qty'],
                'stop_loss': asset['stop_loss'],
                'stop_loss_name': asset['stop_loss_name'],
                'current_close': asset['price']
            }

    def remove_asset(self, symbol):
        """Remove an asset from the portfolio."""
        if symbol in self.local_portfolio:
            del self.local_portfolio[symbol]

    def get_local_portfolio_value(self):
        """Calculate the total value of the local portfolio."""
        return sum(
            asset['qty'] * asset['current_close']
            for asset in self.local_portfolio.values()
        )

    def update_current_price(self, symbol, close):
        """Update the current price of a specific asset."""
        if symbol in self.local_portfolio:
            self.local_portfolio[symbol]['current_close'] = close

    def print_portfolio(self):
        """Print the current portfolio."""
        if self.local_portfolio:
            print("Portfolio:")
            for symbol, data in self.local_portfolio.items():
                print(
                    f"{symbol} | Qty: {data['qty']} | Avg Price: {data['avg_price']} | "
                    f"Current Price: {data['current_close']} | Stop Loss: {data['stop_loss']} | "
                    f"Indicator: {data['stop_loss_name']}"
                )
        else:
            print("Portfolio is empty.")


class Portfolio(PortfolioBase):
    """Simulated portfolio class."""
    def __init__(self):
        super().__init__()


class Portfolio_Live(PortfolioBase):
    """Live portfolio class."""
    def __init__(self):
        super().__init__()
        for asset in self.trading_client.get_all_positions():
            self.add_new_asset({
                'symbol': asset.symbol,
                'time': pd.Timestamp.now(tz='America/New_York'),
                'qty': float(asset.qty),
                'cost': float(asset.cost_basis),
                'price': float(asset.current_price),
                'stop_loss': float(asset.avg_entry_price) * 0.99,
                'stop_loss_name': ''
            })

from alpaca.trading.enums import QueryOrderStatus
from alpaca.trading.requests import GetOrdersRequest

class Orders:
    """Class for managing trading orders."""
    def __init__(self):
        self.trading_client = get_client('trade')
        self.orders_open = []
        self.orders_closed = []

    def update(self):
        """Update the open and closed orders."""
        self.orders_open = self.trading_client.get_orders(
            filter=GetOrdersRequest(
                status=QueryOrderStatus.OPEN, limit=100, nested=True
            )
        )
        self.orders_closed = self.trading_client.get_orders(
            filter=GetOrdersRequest(
                status=QueryOrderStatus.CLOSED, limit=100, nested=True
            )
        )


# Simulated Account and Portfolio
# account = Account()
# portfolio = Portfolio()
#
# account.spend_local_cash(1000)
# portfolio.add_new_asset({
#     'symbol': 'AAPL',
#     'time': pd.Timestamp.now(),
#     'qty': 10,
#     'cost': 1500,
#     'price': 150,
#     'stop_loss': 140,
#     'stop_loss_name': '10% drop'
# })
# portfolio.print_portfolio()
#
# # Live Account and Portfolio
# live_account = Account_Live()
# live_portfolio = Portfolio_Live()
# live_portfolio.print_portfolio()


# orders = Orders()
# orders.update()
# print("Open Orders:", orders.orders_open)
# print("Closed Orders:", orders.orders_closed)
