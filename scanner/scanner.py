from datetime import time, date, datetime,timedelta

import pandas as pd
from dateutil.parser import parse

from scanner.settings import logger

from pprint import pprint
import time as t

class BaseScanner:
    def __init__(self, client, symbol: str, start_date: str, end_date: str, minimum_price: float, maximum_price: float,
                 minimum_average_turnover: float, minimum_average_volume: int, adjusted: bool = False,
                 outside_normal_session: bool = True):
        self.client = client
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.minimum_price = minimum_price
        self.maximum_price = maximum_price
        self.minimum_average_turnover = minimum_average_turnover
        self.minimum_average_volume = minimum_average_volume
        self.adjusted = adjusted
        self.outside_normal_session = outside_normal_session
        self.daily_data = None
        self.minute_data = None
        logger.debug(f"""{self.symbol}: Scanner instance successfully started, 
                         start_date: {start_date}, end_date: {end_date}, adjusted: {adjusted}, 
                         minimum_price: {self.minimum_price}, maximum_price: {self.maximum_price}""")

    def get_candles_data(self):
        self.daily_data = self.client.get_data(symbol=self.symbol, start_date=self.start_date, end_date=self.end_date,
                                               time_frame='day', multiplier=1, adjusted=self.adjusted,
                                               outside_normal_session=self.outside_normal_session)
        if self.daily_data is None or not len(self.daily_data):
            logger.debug(f'{self.symbol}: No Daily Data Found, check inputs again!')
            return
        last_price = self.daily_data['close'].iloc[-1]
        if not self.minimum_price <= last_price <= self.maximum_price:
            logger.debug(f'{self.symbol}: last price: {last_price} not matching minimum/maximum price conditions, '
                         f'so ignoring stock')
            return

        avg_volume = self.daily_data['volume'].mean()
        if avg_volume < self.minimum_average_volume:
            logger.debug(f'{self.symbol}: average volume: {avg_volume} is'
                         f' than parameter value of {self.minimum_average_volume} so ignoring stock')
            return

        avg_turnover = avg_volume * self.daily_data['close'].mean()
        if avg_turnover < self.minimum_average_turnover:
            logger.debug(f'{self.symbol}: average turnover: {avg_turnover} is'
                         f' than parameter value of {self.minimum_average_turnover} so ignoring stock')
            return

        self.minute_data = self.client.get_data(symbol=self.symbol, start_date=self.start_date, end_date=self.end_date,
                                                time_frame='minute', multiplier=1, adjusted=self.adjusted,
                                                outside_normal_session=self.outside_normal_session)


class CandleBreakOut(BaseScanner):
    def __init__(self, client, symbol: str, start_date: str, end_date: str, minimum_price: float, maximum_price: float,
                 daily_breakout_period: int, weekly_breakout_period: int, monthly_breakout_period: int,
                 minimum_average_turnover: float, minimum_average_volume: int, minimum_traded_volume: int,
                 adjusted: bool = False, outside_normal_session: bool = True):
        super().__init__(client, symbol, start_date, end_date, minimum_price, maximum_price, minimum_average_turnover,
                         minimum_average_volume, adjusted, outside_normal_session)
        self.daily_breakout_period = daily_breakout_period
        self.weekly_breakout_period = weekly_breakout_period
        self.monthly_breakout_period = monthly_breakout_period
        self.minimum_traded_volume = minimum_traded_volume
        self.records = pd.DataFrame(columns=['symbol', 'scan_name', 'breakout_time', 'side', 'range_high',
                                             'range_low', 'range_start_time', 'range_end_time', 'sector', 'industry',
                                             'market_cap', 'share_class_shares_outstanding',
                                             'weighted_shares_outstanding',
                                             'open', 'high', 'low', 'close',
                                             'high_time', 'low_time',
                                             'breakout_price',
                                             'breakout_volume',
                                             'price_change_5min', 'price_change_15min',
                                             'volume_change_5min', 'volume_change_15min',
                                             'breakout_to_high',
                                             'breakout_to_low',
                                             'breakout_to_close',
                                             ])

    def run(self):
        self.get_candles_data()
        if self.minute_data is None or not len(self.minute_data) or self.daily_data is None or not len(self.daily_data):
            logger.debug(f'{self.symbol}: No Data Found, check inputs again!')
            return
        for scan in ['Multi-day-breakout', 'Multi-week-breakout', 'Multi-month-breakout']:
            self.run_scan(scan)
        return self.records

    def run_scan(self, scan_name):
        df = self.daily_data.copy()
        df_min = self.minute_data.copy()

        if scan_name == 'Multi-week-breakout':
            df = df.resample('W').agg(
                {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'})
            df.index -= pd.tseries.frequencies.to_offset('6D')
            period = self.weekly_breakout_period
        elif scan_name == 'Multi-month-breakout':
            df = df.resample('M').agg(
                {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'})
            df.index -= pd.tseries.frequencies.to_offset('1M')
            df.index += pd.tseries.frequencies.to_offset('1D')
            period = self.monthly_breakout_period
        else:
            period = self.daily_breakout_period

        df['range_high'] = df['high'].rolling(period).max()
        df['range_low'] = df['low'].rolling(period).min()

        l = list()
        for i in range(period + 1, len(df)):
            _high = df['high'].iloc[i]
            _low = df['low'].iloc[i]
            range_high = df['range_high'].iloc[i - 1]
            range_low = df['range_low'].iloc[i - 1]
            _time = df.index[i]
            _close = df['close'].iloc[i]
            _volume = df['volume'].iloc[i]
            if _high > range_high or _low < range_low:
                _open = df['open'].iloc[i]
                _price = _high if _high > range_high else _low
                side = 'upper' if _high > range_high else 'lower'
                if side == 'upper':
                    _price = _open if _open > range_high else _high
                else:
                    _price = _open if _open < range_low else _low
                range_start_time = df.index[i - period]
                range_end_time = df.index[i - 1]
                record = {'symbol': self.symbol, 'scan_name': scan_name, 'time': str(_time), 'price': _price,
                          'side': side, 'range_high': range_high, 'range_low': range_low,
                          'range_start_time': str(range_start_time), 'range_end_time': str(range_end_time),
                          'high': _high, 'low': _low
                          }
                l.append(record)

        for i in range(len(l)):
            df_min['date'] = df_min.index.date
            df_min['date'] = df_min['date'].astype(str)
            df_min['price_change_5min'] = (df_min['close'].pct_change(periods=5).shift(-5) * 100).round(3)
            df_min['volume_change_5min'] = (df_min['volume'].pct_change(periods=5).shift(-5) * 100).round(3)
            df_min['price_change_15min'] = (df_min['close'].pct_change(periods=15).shift(-15) * 100).round(3)
            df_min['volume_change_15min'] = (df_min['volume'].pct_change(periods=15).shift(-15) * 100).round(3)

            curr_record = l[i]
            date1 = datetime.strptime(curr_record['time'], '%Y-%m-%d %H:%M:%S%z').strftime('%Y-%m-%d')
            if i != len(l) - 1:
                next_record = l[i + 1]
                date2 = datetime.strptime(next_record['time'], '%Y-%m-%d %H:%M:%S%z').strftime('%Y-%m-%d')
                df_needed = df_min[(df_min['date'] >= date1) & (df_min['date'] <= date2)]
            else:
                df_needed = df_min[df_min['date'] >= date1]
            try:
                high, low, close = df_needed['high'].max(), df_needed['low'].min(), list(df_needed['close'])[-1]
            except:
                return

            curr_record['high_time'] = str(df_needed['high'].idxmax())
            curr_record['low_time'] = str(df_needed['low'].idxmin())

            if curr_record['side'] == 'lower':
                maxx = curr_record['low']
                df_needed['abs_diff'] = abs(df_needed['low'] - maxx)
            else:
                maxx = curr_record['high']
                df_needed['abs_diff'] = abs(df_needed['high'] - maxx)
            i = df_needed['abs_diff'].idxmin()
            closest_row = df_needed.loc[i]

            curr_record['breakout_time'] = str(i)
            breakout_price = closest_row['low'] if curr_record['side'] == 'lower' else closest_row['high']
            curr_record['breakout_price'] = breakout_price
            curr_record['breakout_volume'] = closest_row['volume']
            curr_record['price_change_5min'] = closest_row['price_change_5min']
            curr_record['price_change_15min'] = closest_row['price_change_15min']
            curr_record['volume_change_5min'] = closest_row['volume_change_5min']
            curr_record['volume_change_15min'] = closest_row['volume_change_15min']
            curr_record['high'] = closest_row['high']
            curr_record['low'] = closest_row['low']
            curr_record['open'] = closest_row['open']
            curr_record['close'] = closest_row['close']
            curr_record['breakout_to_high'] = round((abs(high - breakout_price) / breakout_price) * 100, 2)
            curr_record['breakout_to_low'] = round((abs(low - breakout_price) / breakout_price) * 100, 2)
            curr_record['breakout_to_close'] = round((abs(close - breakout_price) / breakout_price) * 100, 2)
            del curr_record['time']
            del curr_record['price']

            try:
                ticker_details = self.client.get_ticker_details(symbol=self.symbol, date=str(_time.date()))
                curr_record.update(ticker_details)
                self.records = pd.concat([self.records, pd.DataFrame([curr_record])], ignore_index=True)
            except Exception as e:
                logger.exception(e)
                return


class MultiDayRunners(BaseScanner):
    def __init__(self, client, symbol: str, start_date: str, end_date: str, minimum_price: float, maximum_price: float,
                 minimum_average_turnover: float, minimum_average_volume: int, multi_day_runners_period: int,
                 adjusted: bool = False, outside_normal_session: bool = True):
        super().__init__(client, symbol, start_date, end_date, minimum_price, maximum_price, minimum_average_turnover,
                         minimum_average_volume, adjusted, outside_normal_session)
        self.multi_day_runners_period = multi_day_runners_period
        self.records = pd.DataFrame(columns=['symbol', 'scan_name', 'time', 'price', 'side', 'candles',
                                             'start_time', 'start_price', 'sector', 'industry',
                                             'market_cap', 'share_class_shares_outstanding',
                                             'weighted_shares_outstanding'])

    def run(self):
        self.get_candles_data()
        if self.minute_data is None or not len(self.minute_data) or self.daily_data is None or not len(self.daily_data):
            logger.debug(f'{self.symbol}: No Data Found, check inputs again!')
            return
        self.run_scan()

        return self.records

    def run_scan(self):
        df = self.daily_data.copy()
        for i in range(len(df)):
            _high = df['high'].iloc[i]
            _low = df['low'].iloc[i]
            _time = df.index[i]
            _close = df['close'].iloc[i]
            if i >= self.multi_day_runners_period:
                side = None
                scan = 'Multiday-Runners'
                for j in range(self.multi_day_runners_period):
                    if not df['close'].iloc[i - j] > df['open'].iloc[i - j]:
                        break
                else:
                    side = 'upper'
                for j in range(self.multi_day_runners_period):
                    if not df['open'].iloc[i - j] > df['close'].iloc[i - j]:
                        break
                else:
                    side = 'lower'

                if side:
                    record = {'symbol': self.symbol, 'scan_name': scan, 'time': str(_time), 'price': _close,
                              'side': side,
                              'candles': self.multi_day_runners_period,
                              'start_time': str(df.index[i - self.multi_day_runners_period + 1]),
                              'start_price': df['open'].iloc[i - self.multi_day_runners_period + 1]}
                    ticker_details = self.client.get_ticker_details(symbol=self.symbol, date=str(_time.date()))
                    record.update(ticker_details)
                    self.records = self.records.append(record, ignore_index=True)


class DipBuyDays(BaseScanner):
    def __init__(self, client, symbol: str, start_date: str, end_date: str, minimum_price: float, maximum_price: float,
                 minimum_first_move_size_percent: float, minimum_red_candles: int, minimum_bounce_size_percent: float,
                 minimum_average_turnover: float, minimum_average_volume: int,
                 minimum_traded_volume: int, adjusted: bool = False,
                 outside_normal_session: bool = True):
        super().__init__(client, symbol, start_date, end_date, minimum_price, maximum_price, minimum_average_turnover,
                         minimum_average_volume, adjusted, outside_normal_session)
        self.minimum_first_move_size_percent = minimum_first_move_size_percent
        self.minimum_red_candles = minimum_red_candles
        self.minimum_bounce_size_percent = minimum_bounce_size_percent
        self.minimum_traded_volume = minimum_traded_volume
        self.records = pd.DataFrame(columns=['symbol', 'scan_name', 'time', 'price',
                                             'open_of_day_before_first_move', 'first_move_start_time',
                                             'first_move_end_time', 'first_move_candles', 'first_move_size',
                                             'number_of_red_candles', 'bounce_start_time', 'bounce_end_time',
                                             'bounce_candles', 'bounce_size', 'sector', 'industry',
                                             'market_cap', 'share_class_shares_outstanding',
                                             'weighted_shares_outstanding'])

    def run(self):
        self.get_candles_data()
        if self.minute_data is None or not len(self.minute_data) or self.daily_data is None or not len(
                self.daily_data):
            logger.debug(f'{self.symbol}: No Data Found, check inputs again!')
            return
        self.run_scan()

        return self.records

    def run_scan(self):
        df = self.daily_data.copy()
        open_of_day_before_first_move = None
        first_move_completed = False
        first_move_start_time = None
        first_move_end_time = None
        first_move_candles = 0
        first_move_size = 0
        bounce_start_time = None
        bounce_candles = 0
        bounce_size = 0
        red_candles = 0
        for i in range(1, len(df)):
            reset = False
            candle_change = ((df['close'].iloc[i] - df['open'].iloc[i]) / df['open'].iloc[i]) * 100
            if first_move_completed and red_candles >= self.minimum_red_candles and candle_change >= 0:
                if not df['open'].iloc[i] > open_of_day_before_first_move:
                    reset = True
                else:
                    bounce_size += candle_change
                    bounce_candles += 1
                    if not bounce_start_time:
                        bounce_start_time = df.index[i]
                    if bounce_size >= self.minimum_bounce_size_percent:
                        _time = df.index[i]
                        _close = df['close'].iloc[i]
                        record = {'symbol': self.symbol, 'scan_name': 'Dip-Buy-Days', 'time': str(_time),
                                  'price': _close, 'open_of_day_before_first_move': open_of_day_before_first_move,
                                  'first_move_start_time': str(first_move_start_time),
                                  'first_move_end_time': str(first_move_end_time),
                                  'first_move_candles': first_move_candles, 'first_move_size': first_move_size,
                                  'number_of_red_candles': red_candles, 'bounce_start_time': str(bounce_start_time),
                                  'bounce_end_time': str(_time), 'bounce_candles': bounce_candles,
                                  'bounce_size': bounce_size}
                        ticker_details = self.client.get_ticker_details(symbol=self.symbol, date=str(_time.date()))
                        record.update(ticker_details)
                        self.records = self.records.append(record, ignore_index=True)
                        reset = True
            if first_move_completed:
                if candle_change < 0:
                    if bounce_candles:
                        reset = True
                    else:
                        red_candles += 1
                else:
                    if not bounce_candles:
                        reset = True

            if not first_move_completed:
                if candle_change >= 0:
                    first_move_size += candle_change
                    first_move_candles += 1
                    if not first_move_start_time:
                        first_move_start_time = df.index[i]
                        open_of_day_before_first_move = df['open'].iloc[i - 1]
                    if first_move_size >= self.minimum_first_move_size_percent:
                        first_move_completed = True
                        first_move_end_time = df.index[i]
                else:
                    reset = True

            if reset:
                first_move_completed = False
                first_move_start_time = None
                first_move_candles = 0
                first_move_size = 0
                bounce_start_time = None
                bounce_candles = 0
                bounce_size = 0
                red_candles = 0


class PreMarketAfterMarketBreakout(BaseScanner):
    def __init__(self, client, symbol: str, start_date: str, end_date: str, minimum_price: float, maximum_price: float,
                 ah_pm_breakout_in_pre_market: bool, minimum_average_turnover: float, minimum_average_volume: int,
                 minimum_traded_volume: int, adjusted: bool = False,
                 outside_normal_session: bool = True):
        super().__init__(client, symbol, start_date, end_date, minimum_price, maximum_price, minimum_average_turnover,
                         minimum_average_volume, adjusted, outside_normal_session)
        self.ah_pm_breakout_in_pre_market = ah_pm_breakout_in_pre_market
        self.minimum_traded_volume = minimum_traded_volume
        self.records = pd.DataFrame(columns=['symbol', 'scan_name', 'time', 'price', 'side',
                                             'prev_ah_pm_high', 'prev_ah_pm_low', 'prev_ah_pm_start_time',
                                             'prev_ah_pm_end_time', 'sector', 'industry',
                                             'market_cap', 'share_class_shares_outstanding',
                                             'weighted_shares_outstanding',
                                             'open', 'high', 'low', 'close',
                                             'breakout_volume',
                                             'price_change_5min', 'price_change_15min',
                                             'volume_change_5min', 'volume_change_15min',
                                             'breakout_to_high', 'high_time',
                                             'breakout_to_low', 'low_time',
                                             'breakout_to_close',
                                             ])

    def run(self):
        self.get_candles_data()
        if self.minute_data is None or not len(self.minute_data) or self.daily_data is None or not len(
                self.daily_data):
            logger.debug(f'{self.symbol}: No Data Found, check inputs again!')
            return
        self.run_scan()

        return self.records

    def run_scan(self):
        df = self.minute_data.copy()
        df['date'] = df.index.date
        unique_dates = df['date'].unique()
        l = list()
        for i in range(1, len(unique_dates)):
            prev_ah = df[df['date'] == unique_dates[i - 1]].between_time('16:00', '20:00')
            prev_ah_high = prev_ah['high'].max()
            prev_ah_low = prev_ah['low'].min()
            if self.ah_pm_breakout_in_pre_market:
                day_df = df[df['date'] == unique_dates[i]]
            else:
                day_df = df[df['date'] == unique_dates[i]].between_time('09:30', '15:59')

            for j in range(len(day_df)):
                _high = day_df['high'].iloc[j]
                _low = day_df['low'].iloc[j]
                _open = day_df['open'].iloc[j]
                _close = df['close'].iloc[j]
                _volume = df['volume'].iloc[j]
                if day_df[:j]['volume'].sum() < self.minimum_traded_volume:
                    break
                if _high > prev_ah_high or _low < prev_ah_low:
                    _time = day_df.index[j]
                    _price = _high if _high > prev_ah_high else _low
                    side = 'upper' if _high > prev_ah_high else 'lower'
                    if side == 'upper':
                        _price = _open if _open > prev_ah_high else _high
                    else:
                        _price = _open if _open < prev_ah_low else _low
                    record = {'symbol': self.symbol, 'scan_name': 'AH-PM Breakout', 'time': str(_time), 'price': _price,
                              'side': side, 'prev_ah_pm_high': prev_ah_high, 'prev_ah_pm_low': prev_ah_low,
                              'prev_ah_pm_start_time': str(prev_ah.index[0]),
                              'prev_ah_pm_end_time': str(prev_ah.index[-1]),
                              'open': _open, 'low': _low, 'high': _high, 'close': _close,
                              'breakout_volume': _volume,
                              'breakout_index': day_df.index[j], 'breakout_to_high': 0, 'breakout_to_low': 0,
                              'breakout_to_close': 0}
                    l.append(record)
                    break
        for i in range(len(l)):
            if i != len(l) - 1:
                curr_record, next_record = l[i], l[i + 1]
                df_needed = df.loc[curr_record['breakout_index']:next_record['breakout_index']]
            else:
                curr_record = l[i]
                df_needed = df.loc[curr_record['breakout_index']:]
            df_needed['price_change_5min'] = (df_needed['close'].pct_change(periods=5).shift(-5) * 100).round(3)
            df_needed['volume_change_5min'] = (df_needed['volume'].pct_change(periods=5).shift(-5) * 100).round(3)
            df_needed['price_change_15min'] = (df_needed['close'].pct_change(periods=15).shift(-15) * 100).round(3)
            df_needed['volume_change_15min'] = (df_needed['volume'].pct_change(periods=15).shift(-15) * 100).round(3)

            high, low, close = df_needed['high'].max(), df_needed['low'].min(), df_needed.iloc[-1]['close']
            curr_record['high_time'] = str(df_needed['high'].idxmax())
            curr_record['low_time'] = str(df_needed['low'].idxmin())

            if curr_record['side'] == 'lower':
                minn = curr_record['low']
                df_needed['abs_diff'] = abs(df_needed['low'] - minn)
            else:
                maxx = curr_record['high']
                df_needed['abs_diff'] = abs(df_needed['high'] - maxx)

            i = df_needed['abs_diff'].idxmin()
            closest_row = df_needed.loc[i]
            curr_record['time'] = str(i)
            breakout_price = closest_row['low'] if curr_record['side'] == 'lower' else closest_row['high']
            curr_record['price'] = breakout_price
            curr_record['breakout_volume'] = closest_row['volume']

            curr_record['price_change_5min'] = closest_row['price_change_5min']
            curr_record['price_change_15min'] = closest_row['price_change_15min']
            curr_record['volume_change_5min'] = closest_row['volume_change_5min']
            curr_record['volume_change_15min'] = closest_row['volume_change_15min']
            curr_record['high'] = closest_row['high']
            curr_record['low'] = closest_row['low']
            curr_record['open'] = closest_row['open']
            curr_record['close'] = closest_row['close']
            curr_record['breakout_to_high'] = round((abs(high - breakout_price) / breakout_price) * 100, 2)
            curr_record['breakout_to_low'] = round((abs(low - breakout_price) / breakout_price) * 100, 2)
            curr_record['breakout_to_close'] = round((abs(close - breakout_price) / breakout_price) * 100, 2)

            del curr_record['breakout_index']
            ticker_details = self.client.get_ticker_details(symbol=self.symbol, date=str(_time.date()))
            curr_record.update(ticker_details)
            self.records = pd.concat([self.records, pd.DataFrame([curr_record])], ignore_index=True)


class DipBuysIntraday(BaseScanner):
    def __init__(self, client, symbol: str, start_date: str, end_date: str, minimum_price: float, maximum_price: float,
                 minimum_eod_dip_percent: float, minimum_eod_dip_bought_percent: float, minimum_average_turnover: float,
                 minimum_average_volume: int, minimum_range: float, minimum_traded_volume: int, adjusted: bool = False,
                 outside_normal_session: bool = True):
        super().__init__(client, symbol, start_date, end_date, minimum_price, maximum_price, minimum_average_turnover,
                         minimum_average_volume, adjusted, outside_normal_session)
        self.minimum_eod_dip_percent = minimum_eod_dip_percent
        self.minimum_eod_dip_bought_percent = minimum_eod_dip_bought_percent
        self.minimum_range = minimum_range
        self.minimum_traded_volume = minimum_traded_volume
        self.records = pd.DataFrame(columns=['symbol', 'scan_name', 'time', 'price', 'open', 'high', 'low',
                                             'close', 'prev_day_close', 'dip_low', 'dip_low_time', 'dip_percent',
                                             'dip_bought_percent', 'final_change',
                                             'pm_high', 'pm_low', 'pm_volume', 'gap_percent',
                                             'volume_until_dip', 'first_5min_volume_after_dip',
                                             'first_15min_volume_after_dip',
                                             'price_change_first_5min_after_dip', 'price_change_first_15min_after_dip',
                                             'open_to_dip_percent',
                                             'pm_high_to_dip_percent', 'high_after_dip_buy',
                                             'high_time_after_dip_buy', 'dip_buy_volume', 'sector', 'industry',
                                             'market_cap', 'share_class_shares_outstanding',
                                             'weighted_shares_outstanding'])

    def run(self):
        self.get_candles_data()
        if self.minute_data is None or not len(self.minute_data) or self.daily_data is None or not len(
                self.daily_data):
            logger.debug(f'{self.symbol}: No Data Found, check inputs again!')
            return
        self.run_scan()

        return self.records

    def run_scan(self):
        df = self.minute_data.copy()
        df['price_change_first_5min_after_dip'] = (df['close'].pct_change(periods=5).shift(-5) * 100).round(3)
        df['first_5min_volume_after_dip'] = df['volume'].rolling(window=5, min_periods=1).sum().shift(-5)
        df['price_change_first_15min_after_dip'] = (df['close'].pct_change(periods=15).shift(-15) * 100).round(3)
        df['first_15min_volume_after_dip'] = df['volume'].rolling(window=15, min_periods=1).sum().shift(-15)
        df['date'] = df.index.date
        unique_dates = df['date'].unique()
        for j, dt in enumerate(unique_dates):
            if not j:
                continue
            day_df = df[df['date'] == dt]
            pm_df = day_df.between_time('04:00', '09:29')
            day_df = day_df.between_time('09:30', '15:59')
            if not len(day_df) or not len(pm_df):
                continue
            pm_volume = pm_df['volume'].sum()
            pm_high = pm_df['high'].max()
            pm_low = pm_df['low'].min()
            prev_close = df[df['date'] == unique_dates[j - 1]]['close'].iloc[-1]
            _open = day_df['open'].iloc[0]
            _close = day_df['close'].iloc[-1]
            _high = day_df['high'].max()
            _low = day_df['low'].min()
            gap_percent = ((_open - prev_close) / prev_close) * 100
            final_change = ((_close - _open) / _open) * 100
            day_dip_percent = 0
            dip_time = None
            dip_low = float('inf')
            dip_bought_high = float('-inf')
            volume_until_dip = 0
            l = []
            for i in range(len(day_df)):
                min_low = day_df['low'].iloc[i]
                min_high = day_df['high'].iloc[i]
                if min_high > dip_low and min_high > dip_bought_high and dip_time:
                    dip_bought_high = min_high
                    dip_bought_percent = ((dip_bought_high - dip_low) / dip_low) * 100
                    if abs(dip_bought_high - dip_low) < self.minimum_range:
                        break
                    _time = day_df.index[i]
                    if dip_bought_percent >= self.minimum_eod_dip_bought_percent:
                        if day_df[:i]['volume'].sum() < self.minimum_traded_volume:
                            break
                        if _time.time() >= time(14, 00):
                            scan_name = 'Eod-Dip-Buy-Panic'
                        else:
                            scan_name = 'Dip-Buy-Intraday'
                        dip_buy_volume = day_df[:i]['volume'].sum() - volume_until_dip
                        pm_high_to_dip_percent = ((pm_high - dip_low) / dip_low) * 100
                        open_to_dip_percent = ((_open - dip_low) / dip_low) * 100
                        record = {'symbol': self.symbol, 'scan_name': scan_name,
                                  'prev_day_close': prev_close, 'pm_high': pm_high,
                                  'pm_low': pm_low, 'pm_volume': pm_volume, 'time': str(_time),
                                  'price': dip_bought_high,
                                  'open': _open, 'high': _high, 'low': _low, 'close': _close,
                                  'open_to_dip_percent': open_to_dip_percent,
                                  'dip_low': dip_low, 'dip_low_time': str(dip_time),
                                  'volume_until_dip': volume_until_dip,
                                  'first_5min_volume_after_dip': df['first_5min_volume_after_dip'].iloc[i],
                                  'first_15min_volume_after_dip': df['first_15min_volume_after_dip'].iloc[i],
                                  'price_change_first_5min_after_dip': df['price_change_first_5min_after_dip'].iloc[i],
                                  'price_change_first_15min_after_dip': df['price_change_first_15min_after_dip'].iloc[
                                      i],
                                  'dip_percent': day_dip_percent, 'dip_bought_percent': dip_bought_percent,
                                  'final_change': final_change,
                                  'gap_percent': gap_percent,
                                  'pm_high_to_dip_percent': pm_high_to_dip_percent,
                                  'dip_buy_volume': dip_buy_volume,
                                  'breakout_index': day_df.index[i]}
                        l.append(record)
                        break
                if not dip_time and min_low < dip_low:
                    dip_low = min_low
                    day_dip_percent = ((dip_low - prev_close) / prev_close) * 100
                    if day_dip_percent <= -self.minimum_eod_dip_percent:
                        dip_time = day_df.index[i]
                        volume_until_dip = day_df[:i]['volume'].sum()

            for i in range(len(l)):
                if i != len(l) - 1:
                    curr_record, next_record = l[i], l[i + 1]
                    df_needed = df.loc[curr_record['breakout_index']:next_record['breakout_index']]
                else:
                    curr_record = l[i]
                    df_needed = df.loc[curr_record['breakout_index']:]
                high = df_needed['high'].max()
                curr_record['high_after_dip_buy'] = high
                curr_record['high_time_after_dip_buy'] = str(df_needed['high'].idxmax())
                del curr_record['breakout_index']
                ticker_details = self.client.get_ticker_details(symbol=self.symbol, date=str(_time.date()))
                curr_record.update(ticker_details)
                self.records = pd.concat([self.records, pd.DataFrame([curr_record])], ignore_index=True)


class GapDownDipBought(BaseScanner):
    def __init__(self, client, symbol: str, start_date: str, end_date: str, minimum_price: float, maximum_price: float,
                 minimum_gap_down_percent: float, minimum_dip_bought_percent: float,
                 minimum_average_turnover: float, minimum_average_volume: int,
                 minimum_range: float, minimum_traded_volume: int, adjusted: bool = False,
                 outside_normal_session: bool = True):
        super().__init__(client, symbol, start_date, end_date, minimum_price, maximum_price, minimum_average_turnover,
                         minimum_average_volume, adjusted, outside_normal_session)
        self.minimum_gap_down_percent = minimum_gap_down_percent
        self.minimum_dip_bought_percent = minimum_dip_bought_percent
        self.minimum_range = minimum_range
        self.minimum_traded_volume = minimum_traded_volume
        self.records = pd.DataFrame(columns=['symbol', 'scan_name', 'time', 'price', 'open', 'high', 'low',
                                             'close', 'dip_low', 'dip_low_time', 'dip_percent',
                                             'dip_bought_percent', 'final_change', 'prev_day_close',
                                             'pm_high', 'pm_low', 'pm_volume', 'gap_percent',
                                             'volume_until_dip', 'first_5min_volume_after_dip',
                                             'first_15min_volume_after_dip', 'open_to_dip_percent',
                                             'price_change_first_5min_after_dip', 'price_change_first_15min_after_dip',
                                             'pm_high_to_dip_percent', 'high_after_dip_buy',
                                             'high_time_after_dip_buy', 'dip_buy_volume', 'sector', 'industry',
                                             'market_cap', 'share_class_shares_outstanding',
                                             'weighted_shares_outstanding'])

    def run(self):
        self.get_candles_data()
        if self.minute_data is None or not len(self.minute_data) or self.daily_data is None or not len(
                self.daily_data):
            logger.debug(f'{self.symbol}: No Data Found or Price/Volume/Turnover conditions not matched')
            return
        self.run_scan()

        return self.records

    def run_scan(self):
        df = self.minute_data.copy()
        df['price_change_first_5min_after_dip'] = (df['close'].pct_change(periods=5).shift(-5) * 100).round(3)
        df['first_5min_volume_after_dip'] = df['volume'].rolling(window=5, min_periods=1).sum().shift(-5)
        df['price_change_first_15min_after_dip'] = (df['close'].pct_change(periods=15).shift(-15) * 100).round(3)
        df['first_15min_volume_after_dip'] = df['volume'].rolling(window=15, min_periods=1).sum().shift(-15)
        df['date'] = df.index.date
        unique_dates = df['date'].unique()
        for j, dt in enumerate(unique_dates):
            if not j:
                continue
            day_df = df[df['date'] == dt]

            pm_df = day_df.between_time('04:00', '09:29')
            day_df = day_df.between_time('09:30', '15:59')
            if not len(day_df) or not len(pm_df):
                continue
            pm_volume = pm_df['volume'].sum()
            pm_high = pm_df['high'].max()
            pm_low = pm_df['low'].min()
            prev_close = df[df['date'] == unique_dates[j - 1]]['close'].iloc[-1]
            _open = day_df['open'].iloc[0]
            _close = day_df['close'].iloc[-1]
            _high = day_df['high'].max()
            _low = day_df['low'].min()
            gap_percent = ((_open - prev_close) / prev_close) * 100
            final_change = ((_close - _open) / _open) * 100
            day_dip_percent = 0
            dip_time = None
            dip_low = float('inf')
            dip_bought_high = float('-inf')
            volume_until_dip = 0
            l = []
            for i in range(len(day_df)):
                min_low = day_df['low'].iloc[i]
                min_high = day_df['high'].iloc[i]
                if dip_time and min_high > dip_low and min_high > dip_bought_high:
                    dip_bought_high = min_high
                    dip_bought_percent = ((dip_bought_high - dip_low) / dip_low) * 100
                    if abs(dip_bought_high - dip_low) < self.minimum_range:
                        break
                    _time = day_df.index[i]
                    if gap_percent <= -self.minimum_gap_down_percent and \
                            dip_bought_percent >= self.minimum_dip_bought_percent:

                        if day_df[:i]['volume'].sum() < self.minimum_traded_volume:
                            break
                        dip_buy_volume = day_df[:i]['volume'].sum() - volume_until_dip
                        pm_high_to_dip_percent = ((pm_high - dip_low) / dip_low) * 100
                        open_to_dip_percent = ((_open - dip_low) / dip_low) * 100
                        scan_name = 'Gap_down_dip_bought'
                        try:
                            df_after = day_df[i + 1:]
                            high_after_dip_buy = df_after['high'].max()
                            high_after_dip_buy_time = df_after[df_after['high'] == high_after_dip_buy].index[0]
                        except Exception as e:
                            logger.exception(e)
                            high_after_dip_buy = ''
                            high_after_dip_buy_time = ''
                        record = {'symbol': self.symbol, 'scan_name': scan_name, 'time': str(_time),
                                  'price': dip_bought_high, 'open': _open, 'high': _high, 'low': _low,
                                  'close': _close, 'dip_low': dip_low, 'dip_low_time': str(dip_time),
                                  'dip_percent': day_dip_percent, 'dip_bought_percent': dip_bought_percent,
                                  'final_change': final_change, 'prev_day_close': prev_close, 'pm_high': pm_high,
                                  'pm_low': pm_low, 'pm_volume': pm_volume, 'gap_percent': gap_percent,
                                  'volume_until_dip': volume_until_dip,

                                  'first_5min_volume_after_dip': df['first_5min_volume_after_dip'].iloc[i],
                                  'first_15min_volume_after_dip': df['first_15min_volume_after_dip'].iloc[i],
                                  'price_change_first_5min_after_dip': df['price_change_first_5min_after_dip'].iloc[i],
                                  'price_change_first_15min_after_dip': df['price_change_first_15min_after_dip'].iloc[
                                      i],

                                  'open_to_dip_percent': open_to_dip_percent,
                                  'pm_high_to_dip_percent': pm_high_to_dip_percent,
                                  'high_after_dip_buy': high_after_dip_buy,
                                  'high_time_after_dip_buy': str(high_after_dip_buy_time),
                                  'dip_buy_volume': dip_buy_volume,
                                  'breakout_index': day_df.index[i]}
                        l.append(record)
                        break

                if not dip_time and min_low < dip_low:
                    dip_low = min_low
                    day_dip_percent = ((dip_low - prev_close) / prev_close) * 100
                    if day_dip_percent <= -self.minimum_gap_down_percent:
                        dip_time = day_df.index[i]
                        volume_until_dip = day_df[:i]['volume'].sum()

            for i in range(len(l)):
                if i != len(l) - 1:
                    curr_record, next_record = l[i], l[i + 1]
                    df_needed = df.loc[curr_record['breakout_index']:next_record['breakout_index']]
                else:
                    curr_record = l[i]
                    df_needed = df.loc[curr_record['breakout_index']:]

                high = df_needed['high'].max()
                curr_record['high_after_dip_buy'] = high
                curr_record['high_time_after_dip_buy'] = str(df_needed['high'].idxmax())
                del curr_record['breakout_index']
                ticker_details = self.client.get_ticker_details(symbol=self.symbol, date=str(_time.date()))
                curr_record.update(ticker_details)
                self.records = pd.concat([self.records, pd.DataFrame([curr_record])], ignore_index=True)


class DelistingPreNotice(BaseScanner):
    def __init__(self, client, symbol: str, start_date: str, end_date: str, minimum_price: float, maximum_price: float,
                 minimum_average_turnover: float, minimum_average_volume: int, move_days: int, minimum_move_size: float,
                 minimum_move_volume: float, adjusted: bool = False, outside_normal_session: bool = True):
        super().__init__(client, symbol, start_date, end_date, minimum_price, maximum_price, minimum_average_turnover,
                         minimum_average_volume, adjusted, outside_normal_session)
        self.move_days = move_days
        self.minimum_move_size = minimum_move_size
        self.minimum_move_volume = minimum_move_volume
        self.records = pd.DataFrame(columns=['symbol', 'scan_name', 'time', 'price', 'move_size_percent', 'move_range',
                                             'move_start_time', 'move_start_price', 'move_end_time', 'move_end_price',
                                             'move_days', 'move_green_days', 'move_red_days', 'move_volume',
                                             'market_cap', 'share_class_shares_outstanding',
                                             'weighted_shares_outstanding'])

    def run(self):
        self.get_candles_data()
        if self.minute_data is None or not len(self.minute_data) or self.daily_data is None or not len(
                self.daily_data):
            logger.debug(f'{self.symbol}: No Data Found or Price/Volume/Turnover conditions not matched')
            return
        self.run_scan()

        return self.records

    def run_scan(self):
        df = self.daily_data.copy()
        df['range_high'] = df['high'].rolling(window=30).max()
        df = df.dropna()
        move_days = 0
        move_green_days = 0
        move_red_days = 0
        move_started = False
        move_start_time = None
        move_start_price = None
        move_volume = 0
        for i in range(len(df)):
            _high = df['high'].iloc[i]
            range_high = df['range_high'].iloc[i]
            if not move_started and (_high >= 1 or range_high < 1):
                continue
            _close = df['close'].iloc[i]
            _open = df['open'].iloc[i]
            change = _close - _open
            if not move_started:
                move_volume = df['volume'].iloc[i]
                move_days = 0
                if change > 0:
                    move_green_days = 0
                    move_red_days = 0
                elif change < 0:
                    move_red_days = 0
                    move_green_days = 0
                move_started = True
                move_start_time = df.index[i]
                move_start_price = _open
            if move_started:
                move_range = _close - move_start_price
                move_size = (move_range / move_start_price) * 100
                move_days += 1
                move_volume += df['volume'].iloc[i]
                if change > 0:
                    move_green_days += 1
                elif change < 0:
                    move_red_days += 1
                if move_size >= self.minimum_move_size and move_volume >= self.minimum_move_volume and \
                        move_days <= self.move_days:
                    move_end_time = df.index[i]
                    move_started = False
                    move_end_price = _close
                    record = {'symbol': self.symbol, 'scan_name': 'Delisting-Pre-Notice-Move',
                              'time': str(move_end_time),
                              'price': move_end_price, 'move_size_percent': move_size, 'move_range': move_range,
                              'move_start_time': str(move_start_time), 'move_start_price': move_start_price,
                              'move_end_time': str(move_end_time), 'move_end_price': move_end_price,
                              'move_days': move_days, 'move_green_days': move_green_days,
                              'move_red_days': move_red_days, 'move_volume': move_volume}
                    ticker_details = self.client.get_ticker_details(symbol=self.symbol, date=str(move_end_time.date()))
                    record.update(ticker_details)
                    self.records = self.records.append(record, ignore_index=True)


class DelistingPostNotice(BaseScanner):
    def __init__(self, client, symbol: str, start_date: str, end_date: str, minimum_price: float, maximum_price: float,
                 minimum_average_turnover: float, minimum_average_volume: int, move_days: int, minimum_move_size: float,
                 minimum_move_volume: float, adjusted: bool = False, outside_normal_session: bool = True):
        super().__init__(client, symbol, start_date, end_date, minimum_price, maximum_price, minimum_average_turnover,
                         minimum_average_volume, adjusted, outside_normal_session)
        self.move_days = move_days
        self.minimum_move_size = minimum_move_size
        self.minimum_move_volume = minimum_move_volume
        self.records = pd.DataFrame(columns=['symbol', 'scan_name', 'time', 'price','pm_high', 'pm_low', 'pm_volume', 'move_size_percent', 'move_range',
                                             'move_start_time', 'move_start_price', 'move_end_time', 'move_end_price',
                                             'move_days', 'move_green_days', 'move_red_days', 'move_volume',
                                             'market_cap', 'share_class_shares_outstanding',
                                             'weighted_shares_outstanding'])

    def run(self):
        self.get_candles_data()
        if self.minute_data is None or not len(self.minute_data) or self.daily_data is None or not len(
                self.daily_data):
            logger.debug(f'{self.symbol}: No Data Found or Price/Volume/Turnover conditions not matched')
            return
        self.run_scan()

        return self.records

    def run_scan(self):
        df = self.daily_data.copy()
        df['range_high'] = df['high'].rolling(window=30).max()
        df = df.dropna()
        move_days = 0
        move_green_days = 0
        move_red_days = 0
        move_started = False
        move_start_time = None
        move_start_price = None
        move_volume = 0
        for i in range(len(df)):
            range_high = df['range_high'].iloc[i]
            if range_high >= 1 and not move_started:
                continue
            _close = df['close'].iloc[i]
            _open = df['open'].iloc[i]
            change = _close - _open
            if not move_started:
                move_volume = df['volume'].iloc[i]
                move_days = 0
                if change > 0:
                    move_green_days = 0
                    move_red_days = 0
                elif change < 0:
                    move_red_days = 0
                    move_green_days = 0
                move_started = True
                move_start_time = df.index[i]
                move_start_price = _open
            if move_started:
                move_range = _close - move_start_price
                move_size = (move_range / move_start_price) * 100
                move_days += 1
                move_volume += df['volume'].iloc[i]
                if change > 0:
                    move_green_days += 1
                elif change < 0:
                    move_red_days += 1
                if move_size >= self.minimum_move_size and move_volume >= self.minimum_move_volume and \
                        move_days <= self.move_days:
                    move_end_time = df.index[i]
                    move_started = False
                    move_end_price = _close
                    record = {'symbol': self.symbol, 'scan_name': 'Delisting-Post-Notice-Move',
                              'time': str(move_end_time),
                              'price': move_end_price, 'move_size_percent': move_size, 'move_range': move_range,
                              'move_start_time': str(move_start_time), 'move_start_price': move_start_price,
                              'move_end_time': str(move_end_time), 'move_end_price': move_end_price,
                              'move_days': move_days, 'move_green_days': move_green_days,
                              'move_red_days': move_red_days, 'move_volume': move_volume}
                    ticker_details = self.client.get_ticker_details(symbol=self.symbol, date=str(move_end_time.date()))
                    record.update(ticker_details)
                    self.records = self.records.append(record, ignore_index=True)


class ReverseSplit(BaseScanner):
    def __init__(self, client, symbol: str, start_date: str, end_date: str, minimum_price: float, maximum_price: float,
                 minimum_average_turnover: float, minimum_average_volume: int, move_days: int, minimum_move_size: float,
                 minimum_move_volume: float, rs_split_df, adjusted: bool = False, outside_normal_session: bool = True):
        super().__init__(client, symbol, start_date, end_date, minimum_price, maximum_price, minimum_average_turnover,
                         minimum_average_volume, adjusted, outside_normal_session)
        self.move_days = move_days
        self.minimum_move_size = minimum_move_size
        self.minimum_move_volume = minimum_move_volume
        self.rs_split_df = rs_split_df
        self.split_ratio = None
        self.split_date = None
        self.records = pd.DataFrame(columns=['symbol', 'price','scan_name', 'reverse_time', 'reverse_price', 'split_date', 'split_ratio',
                                             'move_size_percent', 'move_range',
                                             'move_start_time', 'move_start_price', 'move_end_time', 'move_end_price',
                                             'move_days', 'move_green_days', 'move_red_days', 'move_volume',
                                             'market_cap', 'share_class_shares_outstanding',
                                             'weighted_shares_outstanding',
                                             'open', 'high', 'low', 'close',
                                             'high_time',
                                             'reverse_volume',
                                             'prev_close', 'gap_percent'
                                             ])
    def run(self):
        try:
            self.start_date = self.split_date = str(self.rs_split_df.loc[self.symbol]['date'].date())
            self.split_ratio = self.rs_split_df.loc[self.symbol]['split_ratio']
        except AttributeError as e:
            logger.exception(e)
            logger.debug(f'{self.symbol}: Error getting reverse split data so ignoring symbol')
            return
        if parse(self.start_date) >= parse(self.end_date):
            logger.debug(f'{self.symbol}: end date is less than split date so changing it to present date')
            self.end_date = str(date.today())

        self.get_candles_data()
        if self.minute_data is None or not len(self.minute_data) or self.daily_data is None or not len(
                self.daily_data):
            logger.debug(f'{self.symbol}: No Data Found or Price/Volume/Turnover conditions not matched')
            return
        self.run_scan()

        return self.records

    def run_scan(self):
        main_df = self.daily_data.copy()
        df_min = self.minute_data.copy()
        l=[]
        for i in range(len(main_df)):
            move_volume = 0
            move_days = 0
            move_green_days = 0
            move_red_days = 0
            move_start_time = main_df.index[i]
            move_start_price = main_df['open'].iloc[i]
            df = main_df[i:]
            for j in range(len(df)):
                _high = df['high'].iloc[j]
                _open = df['open'].iloc[j]
                _close = df['close'].iloc[j]
                change = _close - _open
                move_range = _high - move_start_price
                move_size = (move_range / move_start_price) * 100
                move_days += 1
                move_volume += df['volume'].iloc[j]
                if change > 0:
                    move_green_days += 1
                elif change < 0:
                    move_red_days += 1
                if move_size >= self.minimum_move_size and move_volume >= self.minimum_move_volume and \
                        move_days <= self.move_days:
                    move_end_time = df.index[j]
                    move_end_price = _high
                    record = {'symbol': self.symbol, 'scan_name': 'Reverse-Split', 'time': str(move_end_time),
                              'price': move_end_price, 'split_date': self.split_date, 'split_ratio': self.split_ratio,
                              'move_size_percent': move_size, 'move_range': move_range,
                              'move_start_time': str(move_start_time), 'move_start_price': move_start_price,
                              'move_end_time': str(move_end_time), 'move_end_price': move_end_price,
                              'move_days': move_days, 'move_green_days': move_green_days,
                              'move_red_days': move_red_days, 'move_volume': move_volume,'high': _high,}
                    l.append(record)
                    break
                if move_days > self.move_days:
                    break

        for i in range(len(l)):
            df_min['date'] = df_min.index.date
            df_min['date'] = df_min['date'].astype(str)
            curr_record = l[i]
            date1 = datetime.strptime(curr_record['time'], '%Y-%m-%d %H:%M:%S%z').strftime('%Y-%m-%d')
            if i != len(l) - 1:
                next_record = l[i + 1]
                date2 = datetime.strptime(next_record['time'], '%Y-%m-%d %H:%M:%S%z').strftime('%Y-%m-%d')
                df_needed = df_min[(df_min['date'] >= date1) & (df_min['date'] <= date2)]
            else:
                df_needed = df_min[df_min['date'] >= date1]
            try:
                high, low, close = df_needed['high'].max(), df_needed['low'].min(), list(df_needed['close'])[-1]
            except:
                return
            curr_record['high_time'] = str(df_needed['high'].idxmax())
            
            maxx = curr_record['high']
            df_needed['abs_diff'] = abs(df_needed['high'] - maxx)
            i = df_needed['abs_diff'].idxmin()
            closest_row = df_needed.loc[i]

            curr_record['reverse_time'] = str(i)
            breakout_price =closest_row['high']
            curr_record['reverse_price'] = breakout_price
            curr_record['reverse_volume'] = closest_row['volume']
            curr_record['high'] = closest_row['high']
            curr_record['low'] = closest_row['low']
            curr_record['open'] = closest_row['open']
            curr_record['close'] = closest_row['close']
            
            try:
                df_that_day=df_min[df_min['date'] == date1]
                one_day_before = datetime.strptime(date1, '%Y-%m-%d')- timedelta(days=1)
                one_day_before=one_day_before.strftime('%Y-%m-%d')
                prev_day_df=df_min[df_min['date'] == one_day_before]
            except :
                pass
                
            try:
                prev_close = prev_day_df['close'].iloc[-1]  
                gap_percent = ((df_that_day['open'].iloc[0] - prev_close) / prev_close) * 100  
                curr_record['prev_close']=prev_close
                curr_record['gap_percent']=gap_percent
            except:
                pass # No data available
            
            try:
                pm_df = df_that_day.between_time('04:00', '09:29')
                pm_volume = pm_df['volume'].sum()
                pm_high = pm_df['high'].max()
                pm_low = pm_df['low'].min()
                curr_record['pm_high']=pm_high
                curr_record['pm_low']=pm_low
                curr_record['pm_volume']=pm_volume
            except:
                pass # No Data available
            
            del curr_record['time']

            try:
                ticker_details = self.client.get_ticker_details(symbol=self.symbol, date=str(i))
                curr_record.update(ticker_details)
                self.records = pd.concat([self.records, pd.DataFrame([curr_record])], ignore_index=True)
            except Exception as e:
                logger.exception(e)
                return
            


