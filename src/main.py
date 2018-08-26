from src.HourlyDemandSupply import HourlyDemandSupply


def main():
    intra_day_data = HourlyDemandSupply('../resources/mcp_data_report_14-02-2017-00_00_00.xlsx')
    table_buy, table_sell = intra_day_data.process()
    intra_day_data.plot_curve(table_buy, table_sell)
    return table_buy, table_sell

if __name__ == '__main__':
    main()