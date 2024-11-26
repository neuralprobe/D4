import pandas as pd
import pandas_ta as ta
from scipy.signal import find_peaks
import numpy as np
from Status.Status import Portfolio


class BaseDecision:
    def __init__(self, symbol):
        self.symbol = symbol
        self.note = {'time': [], 'symbol': []}
        self.current_hour = None
        self.current_minute = None
        self.portfolio = Portfolio()

        # 매개변수 설정
        self.params = {
            "BB_1": {"length": 20, "std": 2, "buy_margin": 0.01},
            "BB_2": {"length": 4, "std": 4, "buy_margin": 0.01},
            "BB_trailing_stoploss": 0.2,
            "Price_Oscillator": {"length": 14},
            "RSI": {"length": 14, "hill_window": 32, "hills": 3},
            "SMA": {"margin": 0.01, "periods": [5, 20, 60, 120, 240, 480]},
            "note_list_limit": 3,
        }

    def calculate_indicators(self, data):
        """Calculate technical indicators and add to the data."""
        # Bollinger Bands
        bb_1 = ta.bbands(data['close'], length=self.params["BB_1"]["length"], std=self.params["BB_1"]["std"])
        bb_2 = ta.bbands(data['close'], length=self.params["BB_2"]["length"], std=self.params["BB_2"]["std"])
        for col, prefix in zip(bb_1.columns, ['bb1_', 'bb2_']):
            data[f'{prefix}{col}'] = bb_1[col] if prefix == 'bb1_' else bb_2[col]

        # Price Oscillator
        data['SMA_for_PO'] = ta.sma(data['close'], length=self.params["Price_Oscillator"]["length"])
        data['Price_Oscillator'] = ((data['close'] - data['SMA_for_PO']) / data['SMA_for_PO']) * 100.0

        # RSI
        data['RSI'] = ta.rsi(data['close'], length=self.params["RSI"]["length"])

        # SMA
        for period in self.params["SMA"]["periods"]:
            if period <= len(data):
                data[f'SMA_{period}'] = ta.sma(data['close'], length=period)

    def trim_notes(self):
        """Limit the size of note lists."""
        for key in self.note:
            if len(self.note[key]) > self.params["note_list_limit"]:
                self.note[key] = self.note[key][-self.params["note_list_limit"]:]

    def update_stop_loss(self, data, recent):
        """Update stop-loss for the symbol in the portfolio."""
        if self.symbol in self.portfolio.local_portfolio:
            stop_loss_name = self.portfolio.local_portfolio[self.symbol]['stop_loss_name']
            trailing_stop = data['close'].iloc[-1] * (1 - self.params["BB_trailing_stoploss"] / 100)
            self.portfolio.local_portfolio[self.symbol]['stop_loss'] = max(
                self.portfolio.local_portfolio[self.symbol]['stop_loss'], trailing_stop
            )
            self.portfolio.local_portfolio[self.symbol]['current_close'] = recent['close'].iloc[-1]

    def detect_upward_breakout(self, data, recent, metric, margin=0):
        """Check if the price breaks above a given metric with a margin."""
        threshold = data[metric].iloc[-1]
        threshold_with_margin = threshold + recent['close'].iloc[-1] * margin
        current_close = recent['close'].iloc[-1]
        current_low = data['low'].iloc[-1]

        return (current_low <= threshold_with_margin) and (current_close > threshold_with_margin)


class Maengja(BaseDecision):
    def __init__(self, symbol):
        super().__init__(symbol)

    def update(self, data, recent):
        """Main update logic to calculate signals and update notes."""
        # Update technical indicators
        self.calculate_indicators(data)

        # Store timestamps
        self.current_hour = data.index[-1]
        self.current_minute = recent.index[-1]
        self.note['time'].append(self.current_minute)
        self.note['symbol'].append(self.symbol)

        # Buy and Sell logic
        self.update_buy_signal(data, recent)
        self.update_sell_signal(data, recent)
        self.trim_notes()
        return self.note

    def update_buy_signal(self, data, recent):
        """Logic for determining buy signals."""
        aligned = self.note.get('SMA_align_strength', [0])[-1] > 0.99
        bb1_touch = self.note.get('touch_bb1_lower', [False])[-1]
        bb2_touch = self.note.get('touch_bb2_lower', [False])[-1]
        sma_breakthrough = self.note.get('check_SMA_breakthrough', [0])[-1] > 0

        buy_signal = aligned and (bb1_touch or bb2_touch or sma_breakthrough)
        self.note.setdefault('buy', []).append(buy_signal)

    def update_sell_signal(self, data, recent):
        """Logic for determining sell signals."""
        stop_loss_hit = self.note.get('stoploss_downward_breakout', [False])[-1]
        resistance_break = self.note.get('resistance_upward_breakout', [False])[-1]

        sell_signal = stop_loss_hit or resistance_break
        self.note.setdefault('sell', []).append(sell_signal)


class MaengjaTrailing(Maengja):
    def __init__(self, symbol):
        super().__init__(symbol)

    def update_stop_loss(self, data, recent):
        """Override stop-loss update with trailing logic."""
        if self.symbol in self.portfolio.local_portfolio:
            trailing_stop = data['close'].iloc[-1] * (1 - self.params["BB_trailing_stoploss"] / 100)
            self.portfolio.local_portfolio[self.symbol]['stop_loss'] = max(
                self.portfolio.local_portfolio[self.symbol]['stop_loss'], trailing_stop
            )
            self.portfolio.local_portfolio[self.symbol]['current_close'] = recent['close'].iloc[-1]


class MaengjaTrailingNoSellEnd(MaengjaTrailing):
    def __init__(self, symbol):
        super().__init__(symbol)

    def update_sell_signal(self, data, recent):
        """Exclude end-of-day selling logic."""
        stop_loss_hit = self.note.get('stoploss_downward_breakout', [False])[-1]
        resistance_break = self.note.get('resistance_upward_breakout', [False])[-1]

        sell_signal = stop_loss_hit or resistance_break
        self.note.setdefault('sell', []).append(sell_signal)

