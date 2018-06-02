import pandas as pd
import datetime
import numpy as np
import re
from glob import glob
import os
from dateutil import parser

def parse_time(ss):
    result = []
    months = {'JAN':1, 'FEB':2, 'MAR':3, "APR": 4, "MAY":5, 'JUN':6, 'JUL':7, 'AUG':8, 'SEP':9, 'OCT':10, 'NOV':11, 'DEC':12}
    for s in ss:
        date, hour, minute, second = s.split(':')
        day, month, year = re.match('(\d+)([А-ЯA-Z]+)(\d+)', s).groups()

        result.append(datetime.datetime(year=int('20'+year), month=months[month], day=int(day), hour=int(hour),
                                        minute=int(minute), second=int(second)))
    return result

def date_format(date):
    return date.strftime('%d.%m.%Y')

def load_currencies():
    result = []
    dates = pd.read_csv('courses/36.csv', sep='\t', encoding='utf-8')['Дата']
    for file in glob('courses/*.csv'):
        currency = int(os.path.basename(file).split('.')[0].split('_')[0])
        df = pd.read_csv(file, sep='\t', encoding='utf-8')
        if df.shape[1]==2:
            df['Курс'] = float(df.iloc[0]['Единиц'].split()[1])
            df['Единиц'] = float(df.iloc[0]['Единиц'].split()[0])

            df = pd.concat([df] * len(dates), ignore_index=True)
            df['Дата'] = dates

        date_spaces = []
        dates_datetime = [datetime.datetime.strptime(d, '%d.%m.%Y') for d in dates]
        last_date = dates_datetime[0]
        for i, (date, count, price) in enumerate(zip(dates_datetime, df['Единиц'], df['Курс'])):
            days_diff = int(round((date - last_date).total_seconds() / (3600 * 24), 0))
            for j in range(1,days_diff):
                date_spaces.append({
                    'Дата': date_format(last_date+datetime.timedelta(days=j)),
                    'Единиц': count,
                    'Курс': price
                })
            last_date = date

        df = pd.concat([df, pd.DataFrame(date_spaces)])
        df['Курс'] = df['Курс'].map(lambda x: str(x).replace(',', '.')).astype(np.float32)
        df['Единиц'] = df['Единиц'].astype(np.float32)
        df['Курс'] = df['Курс'].div(df['Единиц'])
        del df['Единиц']

        df['currency'] = currency
        result.append(df)

    return pd.concat(result)

def load(file, nrows=None):
    df = pd.read_csv(file, nrows=nrows)
    currencies = load_currencies()

    df['TRDATETIME'] = parse_time(df['TRDATETIME'])
    df['Дата'] = df['TRDATETIME'].map(date_format)
    df = df.merge(currencies, on=['currency', 'Дата'], how='left')
    df['amount'] = df['amount']*df['Курс']
    df = df.merge(currencies[currencies['currency']==840].rename(columns={'Курс': 'Курс_usd', 'currency': 'currency_usd'}), on='Дата', how='left')

    df.loc[df['channel_type'].isnull(), 'channel_type'] = 'OTHER'

    df.loc[df['trx_category'].isin(['WD_ATM_PARTNER', 'C2C_OUT', 'WD_ATM_OTHER', 'WD_ATM_ROS']), 'trx_category'] = 'CACH_OUT'
    df.loc[df['trx_category'].isin(['DEPOSIT', 'C2C_IN']), 'trx_category'] = 'DEPOSIT'

    df.sort_values(['TRDATETIME'], inplace=True)
    df['day'] = (df['TRDATETIME'] - df['TRDATETIME'].min()).map(lambda x:x.days)
    df['day_of_week'] = df['TRDATETIME'].dt.dayofweek
    df['weekend'] = df['day_of_week']>=5
    df['hour'] = df['TRDATETIME'].dt.hour
    df = df.merge(pd.DataFrame({'cl_id': sorted(df['cl_id'].unique()), 'index': np.arange(len(df['cl_id'].unique()))}), on='cl_id')

    mcc_df = pd.read_csv('mcc_codes.csv', encoding='utf-8')
    df = df.merge(mcc_df, on='MCC', how='left')
    df.loc[df['MCC_group'].isnull(), 'MCC_group'] = 'Неопознанные'
    df.loc[df['MCC_desc'].isnull(), 'MCC_desc'] = 'Неопознанные'
    df['month'] = df['TRDATETIME'].map(lambda x: x.month+ (12 if x.year==2017 else 24 if x.year==2018 else 0)-10)

    return df
