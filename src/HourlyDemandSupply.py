import pandas as pd
import os
import ntpath

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 100000)


class HourlyDemandSupply(object):
    generated_resource_path = "../resources-generated/"

    def __init__(self, path_to_xlsx):
        self.path_to_xlsx = path_to_xlsx
        self.PRICE_VALUE = 'PRICE_VALUE'
        self.VOLUME_VALUE = 'VOLUME_VALUE'

    def process(self):
        file_name = ntpath.basename(self.path_to_xlsx)
        file_name = file_name.split('.')[0] + '_converted_pickle'
        file_path = self.generated_resource_path + file_name
        os.makedirs(os.path.dirname(self.generated_resource_path), exist_ok=True)
        if not os.path.exists(file_path):
            priceing_data = self.import_file()
            table_buy, table_sell = self.get_hourly_price_data(priceing_data)

            pd.to_pickle((table_buy, table_sell), file_path)
        else:
            table_buy, table_sell = pd.read_pickle(file_path)

        return table_buy, table_sell

    def import_file(self):
        os.makedirs(os.path.dirname(self.path_to_xlsx), exist_ok=True)
        file_name = ntpath.basename(self.path_to_xlsx)
        file_name = file_name.split('.')[0] + '_pickle'
        file_path = self.generated_resource_path + file_name
        os.makedirs(os.path.dirname(self.generated_resource_path), exist_ok=True)
        if not os.path.exists(file_path):
            csv = pd.read_excel(self.path_to_xlsx)
            pd.to_pickle(csv, file_path)

        return pd.read_pickle(file_path)

    def get_hourly_price_data(self, day_unparsed_form_pickle):

        column_count = len(day_unparsed_form_pickle.columns)
        return_table_buy = pd.DataFrame()
        return_table_sell = pd.DataFrame()
        last_index = 0
        for index in range(2, column_count + 1, 2):
            current_day = day_unparsed_form_pickle.iloc[:, last_index:index]
            first_column = current_day.columns[0]

            buy_curve_start_index = current_day[current_day[first_column] == 'Buy curve'].iloc[:, 0:1].index[0]
            sell_curve_start_index = current_day[current_day[first_column] == 'Sell curve'].iloc[:, 0:1].index[0]

            buy_curve_data = self.get_curve(buy_curve_start_index, sell_curve_start_index, current_day,
                                            first_column, correct_volume=True)
            sell_curve_data = self.get_curve(sell_curve_start_index, current_day.shape[0], current_day,
                                             first_column)

            sell_curve_data = sell_curve_data[sell_curve_data[self.VOLUME_VALUE] >
                                              buy_curve_data[self.VOLUME_VALUE].min()]
            sell_curve_data = sell_curve_data.dropna().reset_index(drop=True)

            # Add column group name and save to return tables
            current_date_time = current_day.columns[1]
            buy_curve_data = pd.concat([buy_curve_data], axis=1, keys=[current_date_time])
            return_table_buy = return_table_buy.append(buy_curve_data.T)

            sell_curve_data = pd.concat([sell_curve_data], axis=1, keys=[current_date_time])
            return_table_sell = return_table_sell.append(sell_curve_data.T)

            last_index = index
        # Forward fill NaN values
        return_table_buy = return_table_buy.T.fillna(method='ffill')
        return_table_sell = return_table_sell.T.fillna(method='ffill')
        return return_table_buy, return_table_sell

    def get_curve(self, curve_index_start, curve_index_end, current_day, first_column, correct_volume=False):
        curve_data = current_day.iloc[(curve_index_start + 1):curve_index_end, :]

        col_volume_values = 'Volume value'
        col_price_values = curve_data.columns[1]

        # Move volume data to separate column
        curve_data.loc[:, col_volume_values] = curve_data.loc[
            curve_data[first_column] == col_volume_values][col_price_values]
        curve_data.set_value(curve_data.loc[
                                 curve_data[first_column] == col_volume_values].index,
                             col_price_values, 0)
        curve_data.set_value(curve_data.loc[
                                 curve_data[col_volume_values] == float('nan')].index,
                             col_price_values, 0)
        # Price and volume numbers to same row
        curve_data[col_volume_values] = curve_data[col_volume_values].shift(-1)
        curve_data = curve_data.drop([first_column], axis=1)
        # Remove rows containing NaN values
        curve_data = curve_data.dropna().reset_index(drop=True)

        if correct_volume:
            volume_correction = current_day[current_day[first_column] == 'Bid curve chart data (Volume for net ' \
                                                                         'flows)'].ix[:, -1:].iloc[0][col_price_values]
            # Correct volume numbers
            curve_data[col_volume_values] = curve_data[col_volume_values] + volume_correction

        # Rename columns
        curve_data = curve_data.rename(
            columns={col_volume_values: self.VOLUME_VALUE,
                     col_price_values: self.PRICE_VALUE})

        return curve_data

    def plot_curve(self, buy_curve_data, sell_curve_data):
        import matplotlib.pyplot as plt
        VOLUME_VALUE = self.VOLUME_VALUE
        PRICE_VALUE = self.PRICE_VALUE
        plt.scatter(
            buy_curve_data[buy_curve_data.columns.get_level_values(0)[0], VOLUME_VALUE],
            buy_curve_data[buy_curve_data.columns.get_level_values(0)[0], PRICE_VALUE],
            marker='+', c='r')
        plt.scatter(
            sell_curve_data[sell_curve_data.columns.get_level_values(0)[0], VOLUME_VALUE],
            sell_curve_data[sell_curve_data.columns.get_level_values(0)[0], PRICE_VALUE],
            c='g')
        plt.title(sell_curve_data.columns.get_level_values(0)[0].split()[0])
        plt.show()
