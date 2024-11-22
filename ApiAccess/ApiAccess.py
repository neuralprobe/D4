from enum import Enum
import yaml

from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.live import StockDataStream, CryptoDataStream
from alpaca.data.enums import DataFeed

class ClientType(Enum):
    TRADE = 1
    STOCK_HISTORY = 2
    CRYPTO_HISTORY = 3
    STOCK_STREAM = 4
    CRYPTO_STREAM = 5


class ClientManager:
    def __init__(self, relative_location=''):
        self.relative_location = relative_location
        self.keys = self._load_keys()

    def _load_keys(self):
        """Load keys from the keys.yaml file."""
        with open(f"{self.relative_location}keys.yaml") as f:
            return yaml.safe_load(f)

    def get_alpaca_paper_creds(self):
        """Retrieve Alpaca paper trading credentials."""
        alpaca_paper = self.keys.get('alpaca_paper', {})
        return {key: alpaca_paper.get(key) for key in ["api_key", "api_secret", "base_url"]}

    def get_fmp_key(self):
        """Retrieve FMP API key."""
        return self.keys.get('fmp', {}).get('api_key')

    def get_client(self, client_type : ClientType):
        """Get the appropriate client based on the type."""
        alpaca_creds = self.get_alpaca_paper_creds()
        api_key, api_secret = alpaca_creds["api_key"], alpaca_creds["api_secret"]
        clients = {
            ClientType.TRADE: lambda: TradingClient(api_key, api_secret),
            ClientType.STOCK_HISTORY: lambda: StockHistoricalDataClient(api_key, api_secret),
            ClientType.CRYPTO_HISTORY: lambda: CryptoHistoricalDataClient(),
            ClientType.STOCK_STREAM: lambda: StockDataStream(api_key=api_key, secret_key=api_secret, feed=DataFeed.SIP),
            ClientType.CRYPTO_STREAM: lambda: CryptoDataStream(api_key, api_secret),
        }

        return clients.get(client_type, lambda: None)()


