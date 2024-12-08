import pandas as pd
from alpaca.trading.enums import QueryOrderStatus
from alpaca.trading.requests import GetOrdersRequest
from ApiAccess.ApiAccess import ClientType, ClientManager
from Common.Common import SingletonMeta


class AccountBase(metaclass=SingletonMeta):
    """Base class for account management."""
    def __init__(self):
        self.cash = 0

    def get_total_value(self):
        pass

    def update(self, change = 0):
        pass


class AccountLocal(AccountBase):
    """Simulated account class."""
    def __init__(self):
        super().__init__()
        self.positions = PositionLocal()

    def get_total_value(self):
        return self.cash + self.positions.value

    def set_cash(self, cash):
        self.cash = round(cash,2)

    def update(self, change = 0):
        self.cash += round(change,2)


class AccountLive(AccountBase):
    """Live account class."""
    def __init__(self):
        super().__init__()
        self.positions = PositionLive()
        self.trading_client = ClientManager().get_client(ClientType.TRADE)
        self.order_list = OrderList()

    def get_total_value(self):
        self.positions.update()
        return self.cash + self.positions.value

    def update(self, change = 0):
        self.positions.update()
        self.cash = float(self.trading_client.get_account().cash)


class PositionBase(metaclass=SingletonMeta):
    """Base class for position management."""
    def __init__(self):
        self.assets = {}
        self.value = 0.0

    def print_positions(self):
        """Print the current positions."""
        if self.assets:
            print("Positions:")
            for symbol, data in self.assets.items():
                print(
                    f"{symbol} | Time: {data['time']} | "
                    f"Price: {data['price']} | AvgPrice: {data['avg_price']} | Qty: {data['qty']} | Value: {data['market_value']} | "
                    f"StopLoss: {data['stop_loss']} | StopLossName: {data['stop_loss_name']}"
                )
        else:
            print("Position is empty.")


class PositionLocal(PositionBase):
    """Simulated position class."""
    def __init__(self):
        super().__init__()

    def add_new_asset(self, new_asset):
        """Add a new asset or update an existing one."""
        symbol = new_asset['symbol']
        self.value += new_asset['cost']
        if symbol in self.assets:
            self.assets[symbol]['price'] = new_asset['price']
            self.assets[symbol]['qty'] += new_asset['qty']
            self.assets[symbol]['market_value'] = new_asset['price'] * self.assets[symbol]['qty']
            self.assets[symbol]['cost'] += new_asset['cost']
            self.assets[symbol]['avg_price'] = round(self.assets[symbol]['cost'] / self.assets[symbol]['qty'], 2)
            self.assets[symbol]['stop_loss'] = max(self.assets[symbol]['stop_loss'], new_asset['stop_loss'])
            self.assets[symbol]['stop_loss_name'] = new_asset['stop_loss_name']

        else:
            self.assets[symbol] = dict(time=new_asset['time'], price=new_asset['price'],
                                       avg_price=round(new_asset['cost'] / new_asset['qty'],2),
                                       qty=new_asset['qty'],
                                       market_value=new_asset['cost'], cost=new_asset['cost'],
                                       stop_loss=new_asset['stop_loss'], stop_loss_name=new_asset['stop_loss_name'])

    def remove_asset(self, symbol):
        """Remove an asset from the positions."""
        if symbol in self.assets:
            self.value -= self.assets[symbol]['market_value']
            del self.assets[symbol]

    def update_price(self, symbol, price):
        """Update the current price of a specific asset."""
        if symbol in self.assets:
            prev_market_value = self.assets[symbol]['market_value']
            new_market_value = round(price * self.assets[symbol]['qty'], 2)
            self.assets[symbol]['price'] = price
            self.assets[symbol]['market_value'] = new_market_value
            self.value = self.value + new_market_value - prev_market_value

    def print_positions(self):
        if len(self.assets.keys()):
            print("보유종목/수량/평균가/현재가/손절가/손절지표:", [(symbol,
                                                        round(self.assets[symbol]['qty'],2),
                                                        round(self.assets[symbol]['avg_price'],2),
                                                        round(self.assets[symbol]['price'],2),
                                                        round(self.assets[symbol]['stop_loss'],2),
                                                        self.assets[symbol]['stop_loss_name'])  for symbol in self.assets.keys()])
        else:
            print("보유종목: 없음")


class PositionLive(PositionBase):
    """Live position class."""
    def __init__(self):
        super().__init__()
        self.trading_client = ClientManager().get_client(ClientType.TRADE)
        self.assets_info = {}

    def update(self):
        positions = self.trading_client.get_all_positions()
        self.value = 0.0
        for asset in positions:
            self.value += float(asset.market_value)
            if asset.symbol in self.assets:
                asset_info = self.assets[asset.symbol]
                asset_info['price'] = float(asset.current_price)
                asset_info['avg_price'] = float(asset.avg_entry_price)
                asset_info['qty'] = float(asset.qty)
                asset_info['market_value'] = float(asset.market_value)
                asset_info['cost'] = float(asset.cost_basis)
                asset_info['stop_loss'] = self.assets_info[asset.symbol]['stop_loss']
                asset_info['stop_loss_name'] = self.assets_info[asset.symbol]['stop_loss_name']
                asset_info['valid'] = True

            else:
                self.assets[asset.symbol] = dict(time=pd.Timestamp.now(tz='America/New_York'),
                                                 price=float(asset.current_price), qty=float(asset.qty),
                                                 cost=float(asset.cost_basis), avg_price=float(asset.avg_entry_price),
                                                 stop_loss=self.assets_info[asset.symbol]['stop_loss'],
                                                 stop_loss_name=self.assets_info[asset.symbol]['stop_loss_name'],
                                                 valid=True)
        invalid_symbols = [symbol for symbol in self.assets if not self.assets[symbol]['valid']]
        for symbol in invalid_symbols:
            del self.assets[symbol]

    def add_new_asset(self, new_asset):
        symbol = new_asset['symbol']
        self.assets_info[symbol] = dict(stop_loss=new_asset['stop_loss'], stop_loss_name=new_asset['stop_loss_name'])

    def remove_asset(self, symbol):
        """Remove an asset from the positions."""
        if symbol in self.assets:
            self.value -= self.assets[symbol]['market_value']
            del self.assets[symbol]
        if symbol in self.assets_info:
            del self.assets_info[symbol]


class OrderList(metaclass=SingletonMeta):
    """Class for managing trading orders."""
    def __init__(self):
        self.trading_client = ClientManager().get_client(ClientType.TRADE)
        self.orders_open = []

    def check_open(self):
        """Update the open and closed orders."""
        self.orders_open = self.trading_client.get_orders(
            filter=GetOrdersRequest(
                status=QueryOrderStatus.OPEN, limit=100, nested=True
            )
        )
        return len(self.orders_open)
