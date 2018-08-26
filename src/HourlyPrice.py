from datetime import datetime
from pandas import Series

import numpy as np
import pandas as pd
import os


class HourlyPrice:
    generated_resource_path = "../resources-generated/price/"

    def __init__(self, csv_path):
        self.csv_path = csv_path
        os.makedirs(os.path.dirname(self.generated_resource_path), exist_ok=True)

    def _mapDateToPrice(self, df):
        DATE_TIME = 'DateTime'
        HOURS = 'Hours'

        df[DATE_TIME] = Series(np.zeros(df.shape[0]))
        df[HOURS] = df[HOURS].map(lambda hour: int(str(hour).split('-')[0]))
        df[DATE_TIME] = df.apply(
            lambda row: datetime.strptime(row.loc['Date'], '%m/%d/%Y').replace(hour=row.loc['Hours']).strftime(
                '%d.%m.%Y %H:%M:%S'), axis=1)

        return df[[DATE_TIME, 'SYS']]

    def priceData(self):
        file_path = self.generated_resource_path + '2016_prices_converted_pickle'
        if not os.path.exists(file_path):
            csv_pickle_path = self.generated_resource_path + 'raw_enegy_data_csv'
            if not os.path.exists(csv_pickle_path):
                csv = pd.read_csv(self.csv_path)
                pd.to_pickle(csv, self.generated_resource_path + 'raw_enegy_data_csv')

            priceing_data = pd.read_pickle(csv_pickle_path)
            selected_columns = ['Date', 'Day', 'Hours', 'SYS']
            priceing_data = priceing_data[selected_columns]
            priceing_data = self._mapDateToPrice(priceing_data)
            pd.to_pickle(priceing_data, file_path)
        else:
            priceing_data = pd.read_pickle(file_path)
        return priceing_data
