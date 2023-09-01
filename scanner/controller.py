import json
import multiprocessing

from concurrent.futures import ThreadPoolExecutor
import os
import time as t
from datetime import datetime

import pandas as pd
from dateutil.parser import parse

from scanner.clients.polygon import PolygonClient
from scanner.scanner import CandleBreakOut, MultiDayRunners, DipBuyDays, PreMarketAfterMarketBreakout, \
    GapDownDipBought, DipBuysIntraday, DelistingPreNotice, DelistingPostNotice, ReverseSplit
from scanner.settings import logger, TZ, BASE_DIR, CONFIG_DIR, RECORDS_DIR

scanner_class_dict = {'candle_breakout': CandleBreakOut, 'multi_day_runners': MultiDayRunners,
                      'dip_buy_days': DipBuyDays, 'pm_am_breakout': PreMarketAfterMarketBreakout,
                      'gap_down_dip_bought': GapDownDipBought, 'dip_buys_intraday': DipBuysIntraday,
                      'delisting_pre_notice': DelistingPreNotice, 'delisting_post_notice': DelistingPostNotice,
                      'reverse_split': ReverseSplit}


class Controller:
    def __init__(self, scan_instances, tickers_df, params_df, scan_name, output_file):
        self.scan_instances = scan_instances
        self.tickers_df = tickers_df
        self.params_df = params_df
        self.output_file = output_file
        self.scan_name = scan_name

    @staticmethod
    def run_instance(obj):
        return obj.run()

    def run(self):
        logger.debug('Running Scanner...')
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())  # Use all available CPU cores
        res = pool.map(self.run_instance, self.scan_instances)
        pool.close()
        pool.join()
  
        if not len(res):
            logger.debug('No Results Found')
            t.sleep(3)
            return
        if not os.path.exists(RECORDS_DIR):
            os.mkdir(RECORDS_DIR)
        filter_dir = RECORDS_DIR / self.scan_name
        if not os.path.exists(filter_dir):
            os.mkdir(filter_dir)
        df = pd.concat(res, axis=0)
        df = pd.merge(self.tickers_df, df, on='symbol', how='inner')
        logger.debug('Backtest done, exporting results to excel...')
        file = filter_dir / f'{self.scan_name}_{self.output_file}_{datetime.now(tz=TZ)}.xlsx'.replace(' ',
                                                                                                      '_').replace(':',
                                                                                                                   '_')
        with pd.ExcelWriter(file) as writer:
            df.to_excel(writer, sheet_name='Results', index=False)
            self.params_df.to_excel(writer, sheet_name='Parameters', index=False)

        logger.debug(f'Done, check {file} for results')
        t.sleep(3)


def run(filter_name):
    # Parameters
    try:
        params = params_df = pd.read_excel(BASE_DIR / f'parameters/{filter_name}.xlsx', engine='openpyxl',
                                           sheet_name='params')
    except (FileNotFoundError, ValueError) as e:
        logger.exception(e)
        logger.debug(f"Make sure file {filter_name}.xlsx with params and symbols sheets exists in parameters folder")
        t.sleep(3)
        return

    try:
        # Read api details
        with open(CONFIG_DIR / 'config.json') as config:
            config = json.load(config)
            api_key = config['polygon_api_key']
    except (FileNotFoundError, KeyError) as e:
        logger.exception(e)
        logger.debug('Make sure config.json file exists in config folder with required api details')
        t.sleep(3)
        return

    # Base parameters
    params.index = params['parameter']
    params = params.to_dict()['value']
    try:
        params['start_date'] = str(parse(str(params['start_date'])).date())
        params['end_date'] = str(parse(str(params['end_date'])).date())
    except Exception as e:
        logger.exception(e)
        logger.debug('Please enter start_time and end_time in correct format')
        t.sleep(3)
        return
    output_file = params['output_file'].strip()
    del params['output_file']
    params['adjusted'] = True if params['adjusted'].strip().lower() == 'yes' else False
    if 'ah_pm_breakout_in_pre_market' in params:
        ah_pm_breakout_in_pre_market = params['ah_pm_breakout_in_pre_market']
        params[
            'ah_pm_breakout_in_pre_market'] = True if ah_pm_breakout_in_pre_market.strip().lower() == 'yes' else False

    # Ticker types
    ticker_types = [s.strip().upper() for s in params['ticker_types'].split(',') if s.strip()]
    del params['ticker_types']
    if not len(ticker_types):
        logger.debug('Please provide ticker types')
        t.sleep(3)
        return

    # Symbols
    data_client = PolygonClient(api_key=api_key, archive_data=True, use_archived_data=True)
    tickers = data_client.get_all_symbols(ticker_types=ticker_types)
    symbols = [s['symbol'] for s in tickers]

    if filter_name == 'reverse_split':
        try:
            reverse_split_df = pd.read_excel(BASE_DIR / 'rs_list.xlsx')
            reverse_split_df = reverse_split_df[['RS Date', 'Symbol', 'Split Ratio']]
            reverse_split_df.columns = ['date', 'symbol', 'split_ratio']
            reverse_split_df = reverse_split_df.set_index('symbol')
        except (FileNotFoundError, KeyError, ValueError) as e:
            logger.exception(e)
            logger.debug('Make sure file rs_split.xlsx exists in main folder and contains required columns '
                         'with right names')
            return

        symbols = [s for s in symbols if s in reverse_split_df.index]
        if not len(symbols):
            logger.debug('No reverse split data found for in rs_list.xlsx for given symbols and ticker types')
            return
        params['rs_split_df'] = reverse_split_df

    tickers_df = pd.DataFrame(data=tickers)
    exchanges = data_client.get_all_exchanges()
    exchanges = pd.DataFrame(exchanges)
    exchanges.columns = ['exchange_name', 'exchange']
    tickers_df = pd.merge(tickers_df, exchanges, how='inner', on='exchange')
    scanner_class = scanner_class_dict[filter_name]   #symbols
    scan_instances = [scanner_class(client=data_client, symbol=s, **params) for s in ['TSLA','AMD','AAPL','NVDA','GOOGL']] 
    controller = Controller(scan_instances=scan_instances, tickers_df=tickers_df, params_df=params_df,
                            output_file=output_file, scan_name=filter_name)
    controller.run()
