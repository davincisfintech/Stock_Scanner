import os
import pickle
from datetime import datetime
from typing import Union
from urllib.parse import urlparse, parse_qs

import pandas as pd
import requests
from polygon import RESTClient

from scanner.clients.base import DataClient
from scanner.settings import logger, TZ, DATA_DIR


class PolygonClient(DataClient):
    def __init__(self, api_key, archive_data, use_archived_data):
        self.api_key = api_key
        self.archive_data = archive_data
        self.use_archived_data = use_archived_data

    def get_all_exchanges(self):
        with RESTClient(self.api_key) as client:
            res = client.stocks_equities_exchanges()
            return [(i.name, i.mic) for i in res.exchange if hasattr(i, 'mic')]

    def get_all_symbols(self, market='stocks', ticker_types=None, limit=1000):
        ticker_types = ['CS'] if ticker_types is None else ticker_types
        all_tickers = []
        with RESTClient(self.api_key) as client:
            for t in ticker_types:
                cursor = None
                while True:
                    params = {'cursor': cursor} if cursor else {}
                    resp = client.reference_tickers_v3(market=market, limit=limit, type=t, **params)
                    all_tickers.extend(resp.results)
                    if hasattr(resp, 'count') and resp.count == 1000 and hasattr(resp, 'next_url'):
                        cursor = parse_qs(urlparse(resp.next_url).query)['cursor'][0]
                    else:
                        break
                    
        all_tickers = [
            {'symbol': i['ticker'], 'type': i.get('type', ''), 'exchange': i.get('primary_exchange', ''),
             'name': i['name'],
             'currency': i['currency_name'], 'locale': i['locale']} for i in all_tickers]
        return all_tickers

    def get_ticker_details(self, symbol, date):
        results = dict()
        results['market_cap'] = ''
        results['share_class_shares_outstanding'] = ''
        results['weighted_shares_outstanding'] = ''
        results['sector'] = ''
        results['industry'] = ''
        return results
        with RESTClient(self.api_key) as client:
            try:
                res = client.reference_ticker_details_vx(symbol=symbol, date=date).results
                results['market_cap'] = res.get('market_cap', '')
                results['share_class_shares_outstanding'] = res.get('share_class_shares_outstanding', '')
                results['weighted_shares_outstanding'] = res.get('weighted_shares_outstanding', '')
            except (requests.exceptions.HTTPError, TypeError, AttributeError) as e:
                logger.exception(e)
                results['market_cap'] = ''
                results['share_class_shares_outstanding'] = ''
                results['weighted_shares_outstanding'] = ''
            try:
                res = vars(client.reference_ticker_details(symbol=symbol, date=date))
                results['sector'] = res.get('sector', '')
                results['industry'] = res.get('industry', '')
            except (requests.exceptions.HTTPError, TypeError) as e:
                logger.exception(e)
                results['sector'] = ''
                results['industry'] = ''
            return results

    def get_ticker_news(self, symbol, published_utc):
        with RESTClient(self.api_key) as client:
            res = client.reference_ticker_news_v2(ticker=symbol, published_utc=published_utc, sort='published_utc',
                                                  order='desc')
            return res.results

    def get_data(self, symbol: str, start_date: str, end_date: str, time_frame: str, multiplier: int,
                 limit: int = 50000, adjusted: bool = False, sort: str = 'asc',
                 outside_normal_session: bool = True) -> Union[pd.DataFrame, None]:
        file_name = f'{symbol}_{multiplier}{time_frame}_{start_date}_{end_date}_{adjusted}_' \
                    f'{outside_normal_session}'.replace('/', '-')
        if self.use_archived_data:
            try:
                with open(DATA_DIR / f'{file_name}.pickle', 'rb') as data:
                    data = pickle.load(data)
                    if len(data):
                        return data
            except FileNotFoundError as e:
                pass
                # logger.debug(e)
                # logger.debug(f'{symbol}: data file not found, fetching new data...')

        cur_min = None
        # Send request to api for data
        with RESTClient(self.api_key) as client:
            while True:
                try:
                    if cur_min is None or cur_min != datetime.now().minute:
                        resp = client.stocks_equities_aggregates(ticker=symbol, multiplier=multiplier,
                                                                 timespan=time_frame, from_=start_date, to=end_date,
                                                                 adjusted=adjusted, sort=sort, limit=limit)
                        # Convert data to pandas data frame
                        df = pd.DataFrame(resp.results)
                        break
                except requests.exceptions.HTTPError as e:
                    logger.exception(e)
                    cur_min = datetime.now().minute
                    logger.debug(f'symbol: {symbol}, time_frame: {time_frame}, '
                                 f'Polygon api per minute request limit reached, '
                                 f'waiting for next minute to start to make new requests')
                except Exception as e:
                    logger.exception(e)
                    return

        # Convert to timestamp to specified timezone datetime
        df['time'] = df['t'].apply(lambda x: datetime.fromtimestamp(x / 1000).astimezone(tz=TZ))

        # Set time column as index
        df = df.set_index('time')

        # Rearrange and Rename columns
        df = df[["o", "h", "l", "c", "v"]]
        df.columns = ["open", "high", "low", "close", "volume"]

        # adjust data based normal market or normal + after market
        if not outside_normal_session and time_frame in ['hour', 'minute']:
            df = df.between_time('9:00', '15:59') if time_frame == 'hour' else df.between_time('9:30', '15:59')

        if self.archive_data:
            # Create data directory if not already exists
            if not os.path.exists(DATA_DIR):
                os.mkdir(DATA_DIR)
            with open(DATA_DIR / f'{file_name}.pickle', 'wb') as data:
                pickle.dump(df, data)
        return df
