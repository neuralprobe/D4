import pandas as pd
import pandas_ta as ta
from scipy.signal import find_peaks
import numpy as np
from Status.Status import SingletonMeta, PositionLocal, PositionLive
from itertools import product

class Maengja:

    def __init__(self, symbol):
        self.symbol = symbol
        self.note = {'time': [], 'symbol': []}
        self.current_hour = None
        self.current_minute = None
        self.market_end_time = (15, 59)
        self.positions = PositionLive() if SingletonMeta.is_instantiated(PositionLive) else PositionLocal()
        self.params = dict(BB_1=dict(length=20, std=2, buy_margin=0.01),
                           BB_2=dict(length=4, std=4, buy_margin=0.01),
                           BB_trailing_stop_loss=0.2,
                           Price_Oscillator=dict(length=14),
                           RSI=dict(length=14, hill_window=32, hills=3),
                           SMA=dict(margin=0.01, periods=[5, 20, 60, 120, 240, 480]),
                           note_list_limit=3)
        self.sma_cols = [f'SMA_{period}' for period in self.params['SMA']['periods']]

    def update(self, data, recent):
        self.calculate_indicators(data)

        self.current_hour = data.index[-1]
        self.current_minute = recent.index[-1]
        self.note['time'].append(self.current_minute)
        self.note['symbol'].append(self.symbol)

        self.breakthrough_metric_upward_two_level(data, recent, 'bb1_lower', self.params['BB_1']['buy_margin'])
        self.breakthrough_metric_upward_two_level(data, recent, 'bb2_lower', self.params['BB_2']['buy_margin'])
        self.get_po_divergence(data)
        self.check_rsi(data, self.params['RSI']['hill_window'], self.params['RSI']['hills'])
        self.check_sma_alignment(data)
        self.check_sma_breakthrough(data, recent, self.params['SMA']['margin'])
        self.update_buy_signal(data, recent)

        self.detect_stoploss_downward_breakout(recent)
        self.resistance_upward_breakout(data, recent)
        self.top_resist_downward_break(data, recent)
        self.update_sell_signal(data, recent)

        self.trim_notes()
        return self.note

    def calculate_indicators(self, data):
        bb_columns = ['lower', 'mid', 'upper', 'bandwidth', 'percent']
        bb_1 = ta.bbands(data['close'], length=self.params["BB_1"]["length"], std=self.params["BB_1"]["std"])
        bb_1.columns = bb_columns
        bb_2 = ta.bbands(data['close'], length=self.params["BB_2"]["length"], std=self.params["BB_2"]["std"])
        bb_2.columns = bb_columns
        for col, prefix in product(bb_columns, ['bb1_', 'bb2_']):
            data[f'{prefix}{col}'] = bb_1[col] if prefix == 'bb1_' else bb_2[col]

        data['SMA_for_PO'] = ta.sma(data['close'], length=self.params["Price_Oscillator"]["length"])
        data['Price_Oscillator'] = ((data['close'] - data['SMA_for_PO']) / data['SMA_for_PO']) * 100.0
        data['RSI'] = ta.rsi(data['close'], length=self.params["RSI"]["length"])
        for period in self.params["SMA"]["periods"]:
            if period <= len(data):
                data[f'SMA_{period}'] = ta.sma(data['close'], length=period)

    def breakthrough_metric_upward_two_level(self, data, recent, metric, margin):
        bullish_breakout = (
                self.detect_upward_breakout(data, recent, metric, 0.0)
                or self.detect_upward_breakout_keeping(data, recent, metric, 0.0, f'bullish_breakout_{metric}',
                                                       f'touch_{metric}')
        )
        self.note.setdefault(f'bullish_breakout_{metric}',[]).append(bullish_breakout)
        bullish_breakout_margin = self.detect_upward_breakout(data, recent, metric, margin)
        self.note.setdefault(f'bullish_breakout_{metric}_margin',[]).append(bullish_breakout_margin)
        touch = bullish_breakout and bullish_breakout_margin
        self.note.setdefault(f'touch_{metric}',[]).append(touch)

    @staticmethod
    def detect_upward_breakout(data, recent, metric, margin=0.0):
        threshold = data[metric].iloc[-1]
        threshold_with_margin = threshold + recent['close'].iloc[-1] * margin
        price = recent['close'].iloc[-1]
        current_low = data['low'].iloc[-1]
        if (current_low <= threshold_with_margin) and (price > threshold_with_margin):
            return True
        else:
            time_diff = abs(data.index[-1] - recent.index[-1])
            if time_diff > pd.Timedelta(hours=4):
                prev_close = data['close'].iloc[-1]
                if (prev_close <= threshold_with_margin) and (price > threshold_with_margin):
                    return True
        return False

    def detect_upward_breakout_keeping(self, data, recent, metric, margin, bullish_breakout_metric, touch_metric):
        threshold = data[metric].iloc[-1]
        threshold_with_margin = threshold + recent['close'].iloc[-1] * margin
        price = recent['close'].iloc[-1]
        bullish_breakout_keeping = (
                price > threshold_with_margin
                and self.note.get(bullish_breakout_metric, [False])[-1]
        )
        if self.note.get(touch_metric, [False])[-1]:
            bullish_breakout_keeping = False
        return bullish_breakout_keeping

    def get_po_divergence(self, data):
        if 'PO_divergence' not in data.columns:
            data['PO_divergence'] = np.zeros(len(data), dtype=float)
        peaks, dips = self.get_peaks_and_dips(data['close'], 2)
        peak_first = peaks[0] < dips[0]
        dip_first = peaks[0] > dips[0]
        close_peaks = data['close'].iloc[peaks].to_list()
        close_dips = data['close'].iloc[dips].to_list()
        po_peaks = data['Price_Oscillator'].iloc[peaks].to_list()
        po_dips = data['Price_Oscillator'].iloc[dips].to_list()
        bullish = self.is_bullish_divergence(close_dips, po_dips)
        bearish = self.is_bearish_divergence(close_peaks, po_peaks)
        position = self.decide_divergence_position(bullish, bearish, peak_first, dip_first)
        data.loc[self.current_hour, 'PO_divergence'] = position
        self.note.setdefault('PO_divergence', []).append(position)

    def get_peaks_and_dips(self, series, num_peaks):
        return self._compute_peaks_and_dips(series, len(series), num_peaks)

    def get_peaks_and_dips_in_window(self, series, window):
        return self._compute_peaks_and_dips(series, window)

    @staticmethod
    def _compute_peaks_and_dips(series, window, num_peaks=None):
        windowed_series = series.iloc[-window:]
        peaks, _ = find_peaks(windowed_series)
        dips, _ = find_peaks(-windowed_series)
        if series.iloc[-1] > series.iloc[-2]:
            peaks = np.append(peaks, len(windowed_series) - 1)
        elif series.iloc[-1] < series.iloc[-2]:
            dips = np.append(dips, len(windowed_series) - 1)
        if num_peaks is not None:
            peaks = peaks[-num_peaks:]
            dips = dips[-num_peaks:]
        return peaks, dips

    @staticmethod
    def is_bullish_divergence(close_dips, po_dips):
        return (
                (close_dips[0] > close_dips[1] and po_dips[0] < po_dips[1]) or
                (close_dips[0] < close_dips[1] and po_dips[0] > po_dips[1]) or
                (close_dips[0] == close_dips[1] and po_dips[0] > po_dips[1])
        )

    @staticmethod
    def is_bearish_divergence(close_peaks, po_peaks):
        return (
                (close_peaks[0] < close_peaks[1] and po_peaks[0] > po_peaks[1]) or
                (close_peaks[0] > close_peaks[1] and po_peaks[0] < po_peaks[1]) or
                (close_peaks[0] == close_peaks[1] and po_peaks[0] > po_peaks[1])
        )

    @staticmethod
    def decide_divergence_position(bullish, bearish, peak_first, dip_first):
        if bullish and not bearish:
            return 1
        elif not bullish and bearish:
            return -1
        elif peak_first:
            return -1 if bearish else 0
        elif dip_first:
            return 1 if bullish else 0
        return 0

    def check_rsi(self, data, window, num_hills):
        if 'RSI_check' not in data.columns:
            data['RSI_check'] = np.full(len(data), 0, dtype=float)
        current_rsi = data['RSI'].iloc[-1]
        position = 0
        if current_rsi < 30:
            _, dips = self.get_peaks_and_dips_in_window(data['RSI'], window)
            if (data['RSI'].iloc[dips] < 30).sum() >= num_hills:
                position += 1
        elif current_rsi > 70:
            peaks, _ = self.get_peaks_and_dips_in_window(data['RSI'], window)
            if (data['RSI'].iloc[peaks] > 70).sum() >= num_hills:
                position -= 1
        data.loc[self.current_hour, 'RSI_check'] = position
        self.note.setdefault('RSI_check', []).append(position)

    def check_sma_alignment(self, data):
        if 'SMA_align_strength' not in data.columns:
            data['SMA_align_strength'] = np.full(len(data), 0.0, dtype=float)
        current_sma = [data[col].iloc[-1] for col in self.sma_cols]
        align_strength = sum(
            (1 if current_sma[i] > current_sma[i + 1] else -1)
            for i in range(len(current_sma) - 1)
        )
        align_strength /= len(current_sma) - 1  # 정규화 (-1 ~ +1 범위)
        data.loc[self.current_hour, 'SMA_align_strength'] = align_strength
        self.note.setdefault('SMA_align_strength', []).append(align_strength)

    def check_sma_breakthrough(self, data, recent, margin):
        sma_below_close_name = ''
        sma_below_close_value = 0
        bullish_breakouts = 0
        for sma_name in self.sma_cols:
            sma_value = data[sma_name].iloc[-1]
            if self.detect_upward_breakout(data, recent, sma_name, margin):
                bullish_breakouts += 1
                if sma_value > sma_below_close_value:
                    sma_below_close_name = sma_name
                    sma_below_close_value = sma_value
        self.note.setdefault('check_SMA_breakthrough', []).append(bullish_breakouts)
        self.note.setdefault('SMA_below_close', []).append(sma_below_close_name)

    def update_buy_signal(self, data, recent):
        is_aligned = self.note['SMA_align_strength'][-1] > 0.99
        is_bb1_touch = self.note['touch_bb1_lower'][-1]
        is_bb2_touch = self.note['touch_bb2_lower'][-1]
        is_at_least_one_touch = is_bb1_touch or is_bb2_touch
        is_sma_breakthrough = self.note['check_SMA_breakthrough'][-1] > 0.1
        is_po_divergence_bearish = self.note['PO_divergence'][-1] < 0
        is_rsi_check_bearish = self.note['RSI_check'][-1] < 0
        is_bearish = is_po_divergence_bearish or is_rsi_check_bearish

        buy = is_aligned and (is_at_least_one_touch or is_sma_breakthrough) and not is_bearish
        self.note.setdefault('buy', []).append(buy)

        buy_reason = (
                ('bb1' if is_bb1_touch else '') +
                ('-' if is_bb2_touch and (is_bb2_touch or is_sma_breakthrough) else '') +
                ('bb2' if is_bb2_touch else '') +
                ('-' if is_bb2_touch and is_sma_breakthrough else '') +
                ('sma' if is_sma_breakthrough else '')
        )
        self.note.setdefault('buy_reason', []).append(buy_reason)

        buy_strength = (
                int(is_bb1_touch) +
                int(is_bb2_touch) +
                int(is_sma_breakthrough) +
                self.note['PO_divergence'][-1] +
                self.note['RSI_check'][-1]
        )
        self.note.setdefault('buy_strength', []).append(buy_strength)

        stop_loss_candidates = []
        stop_loss_name_candidates = []

        if is_bb1_touch:
            stop_loss_candidates.append(data['close'].iloc[-1] * (100 - self.params['BB_trailing_stop_loss']) * 0.01)
            stop_loss_name_candidates.append('bb1_lower')
        if is_bb2_touch:
            stop_loss_candidates.append(data['close'].iloc[-1] * (100 - self.params['BB_trailing_stop_loss']) * 0.01)
            stop_loss_name_candidates.append('bb2_lower')
        if is_sma_breakthrough:
            sma_name = self.note['SMA_below_close'][-1]
            stop_loss_candidates.append(data[sma_name].iloc[-1])
            stop_loss_name_candidates.append(sma_name)

        if self.symbol in self.positions.assets:
            stop_loss_candidates.append(self.positions.assets[self.symbol]['stop_loss'])
            stop_loss_name_candidates.append(self.positions.assets[self.symbol]['stop_loss_name'])

        if stop_loss_candidates:
            stop_loss = max(stop_loss_candidates)
            stop_loss_name = stop_loss_name_candidates[stop_loss_candidates.index(stop_loss)]
        else:
            stop_loss = 0.0
            stop_loss_name = ''

        self.note.setdefault('price', []).append(recent['close'].iloc[-1])
        self.note.setdefault('stop_loss', []).append(stop_loss)
        self.note.setdefault('stop_loss_name', []).append(stop_loss_name)
        self.note.setdefault('trading_value', []).append(data.loc[self.current_hour, 'trading_value'])

    def detect_stoploss_downward_breakout(self, recent):
        if self.symbol not in self.positions.assets:
            self.note.setdefault('stoploss_downward_breakout', []).append(False)
            return
        threshold = self.positions.assets[self.symbol]['stop_loss']
        price = recent['close'].iloc[-1]
        bearish_breakout = price < threshold
        self.note.setdefault('stoploss_downward_breakout', []).append(bearish_breakout)

    def resistance_upward_breakout(self, data, recent):
        if self.symbol not in self.positions.assets:
            self.note.setdefault('resistance_upward_breakout', []).append(False)
            self.note.setdefault('new_stop_loss_candidate', []).append(0.0)
            self.note.setdefault('new_stop_loss_name_candidate', []).append('')
            return
        resistance_upward_breakout = False
        current_stop_loss = self.positions.assets[self.symbol]['stop_loss']
        current_stop_loss_name = self.positions.assets[self.symbol]['stop_loss_name']
        current_stop_loss_value = data[current_stop_loss_name].iloc[-1]
        new_stop_loss_candidate = current_stop_loss_value
        new_stop_loss_name_candidate = current_stop_loss_name
        for metric in self.sma_cols + ['bb1_upper', 'bb2_upper']:
            metric_value = data[metric].iloc[-1]
            if metric_value > current_stop_loss_value:
                if self.detect_upward_breakout(data, recent, metric, 0):
                    resistance_upward_breakout = True
                    new_stop_loss_candidate = metric_value
                    new_stop_loss_name_candidate = metric
        self.note.setdefault('resistance_upward_breakout', []).append(resistance_upward_breakout)
        self.note.setdefault('new_stop_loss_candidate', []).append(new_stop_loss_candidate)
        self.note.setdefault('new_stop_loss_name_candidate', []).append(new_stop_loss_name_candidate)

    def top_resist_downward_break(self, data, recent):
        if self.symbol not in self.positions.assets:
            self.note.setdefault('top_resist_downward_break', []).append(False)
            return
        current_resistances = [data[col].iloc[-1] for col in self.sma_cols] + [
            data['bb1_upper'].iloc[-1], data['bb2_upper'].iloc[-1]
        ]
        current_high = recent['high'].iloc[-1]
        price = recent['close'].iloc[-1]
        high_resist_free = all(current_high <= resistance for resistance in current_resistances)
        bearish_breakout = any(
            current_high > resistance >= price for resistance in current_resistances
        )
        top_resist_downward_break = high_resist_free and bearish_breakout
        self.note.setdefault('top_resist_downward_break', []).append(top_resist_downward_break)

    def update_sell_signal(self, data, recent):
        do_stop_loss = self.note['stoploss_downward_breakout'][-1]
        is_resistance_upward_breakout = self.note['resistance_upward_breakout'][-1]
        is_po_divergence_bullish = self.note['PO_divergence'][-1] > 0
        is_rsi_check_bullish = self.note['RSI_check'][-1] > 0
        is_bullish = is_po_divergence_bullish or is_rsi_check_bullish
        now = self.note['time'][-1]
        is_now_end_of_day = (
                (now.hour, now.minute) == self.market_end_time
                and self.symbol not in self.positions.assets.keys()
        )
        do_take_profit = is_resistance_upward_breakout and not is_bullish
        top_resist_downward_break = self.note['top_resist_downward_break'][-1]
        do_keep_profit = is_resistance_upward_breakout and is_bullish
        if do_keep_profit:
            self.positions.assets[self.symbol]['stop_loss'] = max(
                self.positions.assets[self.symbol]['stop_loss'],
                self.note['new_stop_loss']
            )
        sell = (
                (do_stop_loss or do_take_profit or top_resist_downward_break or is_now_end_of_day)
                and not do_keep_profit
        )
        self.note.setdefault('sell', []).append(sell)
        sell_reason = (
                ('|StopLoss|' if do_stop_loss else '') +
                ('|TakeProfit|' if do_take_profit else '') +
                ('|ResistFree|' if top_resist_downward_break else '') +
                ('|EndMarket|' if is_now_end_of_day else '')
        )
        self.note.setdefault('sell_reason', []).append(sell_reason)
        self.note.setdefault('keep_profit', []).append(do_keep_profit)

    def trim_notes(self):
        for key in self.note:
            if len(self.note[key]) > self.params["note_list_limit"]:
                self.note[key] = self.note[key][-self.params["note_list_limit"]:]





# def update_stop_loss(self, data, recent):
#     """Update stop-loss for the symbol in the portfolio."""
#     if self.symbol not in self.positions.assets:
#         return
#     trailing_stop = data['close'].iloc[-1] * (1 - self.params["BB_trailing_stop_loss"] / 100)
#     change_stop_loss = self.positions.assets[self.symbol]['stop_loss'] >= trailing_stop
#     if change_stop_loss:
#         self.positions.assets[self.symbol]['stop_loss'] = trailing_stop
#         self.positions.assets[self.symbol]['stop_loss_name'] = "trailing_stop"
#     self.positions.assets[self.symbol]['price'] = recent['close'].iloc[-1]
#
# class MaengjaTrailing(Maengja):
#     def __init__(self, symbol):
#         super().__init__(symbol)
#
#     def update_stop_loss(self, data, recent):
#         """Override stop-loss update with trailing logic."""
#         if self.symbol in self.positions.assets:
#             trailing_stop = data['close'].iloc[-1] * (1 - self.params["BB_trailing_stoploss"] / 100)
#             self.positions.assets[self.symbol]['stop_loss'] = max(
#                 self.positions.assets[self.symbol]['stop_loss'], trailing_stop
#             )
#             self.positions.assets[self.symbol]['price'] = recent['close'].iloc[-1]
#
#
# class MaengjaTrailingNoSellEnd(MaengjaTrailing):
#     def __init__(self, symbol):
#         super().__init__(symbol)
#
#     def update_sell_signal(self, data, recent):
#         """Exclude end-of-day selling logic."""
#         stop_loss_hit = self.note.get('stoploss_downward_breakout', [False])[-1]
#         resistance_break = self.note.get('resistance_upward_breakout', [False])[-1]
#
#         sell_signal = stop_loss_hit or resistance_break
#         self.note.setdefault('sell', []).append(sell_signal)
#
#