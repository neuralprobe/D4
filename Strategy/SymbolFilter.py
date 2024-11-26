import pandas as pd
from datetime import datetime, timedelta
import math
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed
from API_Access.API_Access import get_client
import Common.Common as Common


class AssetFilter:
    """Base class for asset filtering with Alpaca API."""

    def __init__(self, filter_type, file_path):
        self.filter_type = filter_type
        self.file_path = file_path

    def read_existing_symbols(self):
        """Read existing symbols from file."""
        return Common.read_csv_to_list(self.file_path)

    def write_symbols_to_file(self, symbols):
        """Write filtered symbols to file."""
        Common.write_list_to_csv(symbols, self.file_path)


class EquityFilter(AssetFilter):
    """Filter US equities based on trading volume and value."""

    def __init__(self, renew=False, start_timestamp=pd.Timestamp.now()):
        super().__init__('US_EQUITY', f'data/top_symbols_us_{start_timestamp}.csv')
        self.renew = renew
        self.start_timestamp = start_timestamp

    def get_equities(self):
        """Fetch tradable US equities from Alpaca API."""
        trading_client = get_client('trade')
        search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)
        assets = trading_client.get_all_assets(search_params)
        return pd.DataFrame([
            {'symbol': asset.symbol, 'tradable': asset.tradable}
            for asset in assets if asset.tradable
        ]).set_index('symbol')

    def get_stock_bars(self, symbols):
        """Fetch 60-day stock bar data for given symbols."""
        stock_client = get_client('stock-history')
        request_size = 1024
        df_daily_bars = pd.DataFrame()
        for i in range(math.ceil(len(symbols) / request_size)):
            batch = symbols[i * request_size: (i + 1) * request_size]
            request_params = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=TimeFrame.Day,
                start=self.start_timestamp - timedelta(days=120),
                end=self.start_timestamp,
                feed=DataFeed.SIP
            )
            daily_bars = stock_client.get_stock_bars(request_params).df
            df_daily_bars = pd.concat([df_daily_bars, daily_bars.groupby(level='symbol').tail(60)])
        return df_daily_bars

    def filter_symbols(self):
        """Filter US equities and return top 5% by trading value."""
        if not self.renew:
            return self.read_existing_symbols()

        tradable_assets = self.get_equities()
        symbols = tradable_assets.index.to_list()
        daily_bars = self.get_stock_bars(symbols)

        # Calculate trading value
        daily_bars['trading_value'] = daily_bars['volume'] * daily_bars['vwap']
        mean_trading_value = daily_bars.groupby(level='symbol')['trading_value'].mean()
        sorted_symbols = mean_trading_value.sort_values(ascending=False)

        # Select top 5%
        threshold = int(len(sorted_symbols) * 0.05)
        top_symbols = sorted_symbols.head(threshold).index.tolist()
        self.write_symbols_to_file(top_symbols)
        return top_symbols


class CryptoFilter(AssetFilter):
    """Filter cryptocurrencies based on trading volume and value."""

    def __init__(self, renew=False):
        super().__init__('CRYPTO', 'data/top_symbols_crypto.csv')
        self.renew = renew

    def get_cryptos(self):
        """Fetch tradable cryptocurrencies from Alpaca API."""
        trading_client, _, crypto_client = get_client('all-history')
        search_params = GetAssetsRequest(asset_class=AssetClass.CRYPTO)
        assets = trading_client.get_all_assets(search_params)
        return pd.DataFrame([
            {'symbol': asset.symbol, 'tradable': asset.tradable}
            for asset in assets if asset.tradable and '/USD' in asset.symbol
        ]).set_index('symbol')

    def get_crypto_bars(self, symbols):
        """Fetch 60-day crypto bar data for given symbols."""
        crypto_client = get_client('crypto-history')
        request_params = CryptoBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame.Day,
            start=datetime.today() - timedelta(days=120),
            end=datetime.today()
        )
        daily_bars = crypto_client.get_crypto_bars(request_params).df
        return daily_bars.groupby(level='symbol').tail(60)

    def filter_symbols(self):
        """Filter cryptocurrencies and return top 20% by trading value."""
        if not self.renew:
            return self.read_existing_symbols()

        tradable_assets = self.get_cryptos()
        symbols = tradable_assets.index.to_list()
        daily_bars = self.get_crypto_bars(symbols)

        # Calculate trading value
        daily_bars['trading_value'] = daily_bars['volume'] * daily_bars['vwap']
        mean_trading_value = daily_bars.groupby('symbol')['trading_value'].mean()
        threshold = mean_trading_value.quantile(0.80)
        top_symbols = mean_trading_value[mean_trading_value > threshold].index.to_list()
        self.write_symbols_to_file(top_symbols)
        return top_symbols


if __name__ == "__main__":
    # 필터링 실행
    equity_filter = EquityFilter(renew=True)
    top_equities = equity_filter.filter_symbols()
    print("Top US Equities:", top_equities)

    crypto_filter = CryptoFilter(renew=True)
    top_cryptos = crypto_filter.filter_symbols()
    print("Top Cryptos:", top_cryptos)
