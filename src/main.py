from src.HourlyDemandSupply import HourlyDemandSupply
from src.HourlyPrice import HourlyPrice

def buyAndSellTables():
    intra_day_data = HourlyDemandSupply('../resources/mcp_data_report_14-02-2017-00_00_00.xlsx')
    table_buy, table_sell = intra_day_data.process()
    intra_day_data.plot_curve(table_buy, table_sell)
    return table_buy, table_sell

def price():
    hourlyPrice = HourlyPrice('../resources/raw_enegy_data_csv.csv')
    return hourlyPrice.priceData()

def main():
    table_buy, table_sell = buyAndSellTables()
    # priceByHour = price()
    # print(priceByHour[:3])


if __name__ == '__main__':
    main()