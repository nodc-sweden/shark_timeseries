from utils import calculateparameter as calculateparameter
from utils.sharkweb import Datasource
from utils.format_raw_data import FormatData
from utils.means import Means
import pathlib
import pandas as pd

def get_data(datatype: str, year_interval: list, stations: list, no_download: bool, name: str):
    print(f"no download is {no_download}")
    datasource = Datasource(pathlib.Path('..\data'), datatype = datatype, 
                            year_interval = year_interval, stations=stations, no_download=no_download, name=name)
    return FormatData(datasource.result_directory).load_raw_data(file_name=datasource.filename)

if __name__ == "__main__":
    
    download_specs = {"Gulf_of_Bothnia": {"datatype": "Physical and Chemical", "year_interval": [2000, 2023], "stations": ["MS4 / C14", "F9 / A13"], "no_download": True, "name": "GoF"},
                      "Skagerrak_Kattegat": {"datatype": "Physical and Chemical", "year_interval": [1994, 2023], "stations": ["Å13", "FLADEN"], "no_download": True, "name": "WC"},
                      "Baltic_Proper": 
                        {"datatype": "Physical and Chemical", 
                         "year_interval": [1994, 2023], 
                         "stations": ["BY5 BORNHOLMSDJ", "BY15 GOTLANDSDJ", "BY31 LANDSORTSDJ", "BY32 NORRKÖPINGSDJ"], 
                         "no_download": True, 
                         "name": "BP"}
                      }
    data_list = []
    for key, value in download_specs.items():
        data = get_data(datatype=value["datatype"], year_interval=value["year_interval"], stations=value["stations"], no_download=value["no_download"], name =value["name"])
        data_list.append(data)
    data = pd.concat(data_list)

    mean = Means(data = data, result_directory=pathlib.Path('..\data'))
    mean.calculate_depth_interval_mean(save_path=pathlib.Path("..\data\mean"), interval = [[0, 10], 'BW'], BW_type = 'STATN')
    # Total nutrients full year
    # for parameter in ["PTOT", "NTOT"]:
    variables = ["PTOT", "NTOT", "SALT"]
    variabler='_'.join([param for param in variables])
    FormatData(pathlib.Path('..\data')).filter_by_parameter(mean.df_depth_mean,parameters=variables).to_csv(pathlib.Path(f"..\data/{variabler}_visit.txt"), sep="\t", encoding="cp1252", index = False)
    month_mean = mean.calculate_month_mean(mean.df_depth_mean, variables)
    month_mean.to_csv(pathlib.Path(f"..\data/{variabler}_monthly.txt"), sep="\t", encoding="cp1252", index = False)
    year_mean = mean.calculate_year_mean(mean.df_depth_mean, variables)
    year_mean.to_csv(pathlib.Path(f"..\data/{variabler}_yearly.txt"), sep="\t", encoding="cp1252", index = False)
    
    # Oxygen data only autumn
    variables = ["O2_H2S"]
    variabler='_'.join([param for param in variables])
    data  = FormatData(pathlib.Path('..\data')).filter_by_month(mean.df_depth_mean, months=[8, 9, 10, 11])
    FormatData(pathlib.Path('..\data')).filter_by_parameter(data,parameters=variables).to_csv(pathlib.Path(f"..\data/{variabler}_visit.txt"), sep="\t", encoding="cp1252", index = False)
    
    month_mean = mean.calculate_month_mean(data, variables)
    month_mean.to_csv(pathlib.Path(f"..\data/{variabler}_monthly.txt"), sep="\t", encoding="cp1252", index = False)
    year_mean = mean.calculate_year_mean(data, variables)
    year_mean.to_csv(pathlib.Path(f"..\data/{variabler}_autumn_yearly.txt"), sep="\t", encoding="cp1252", index = False)

    # Temperature data only summer
    variables = ["TEMP"]
    variabler='_'.join([param for param in variables])
    data  = FormatData(pathlib.Path('..\data')).filter_by_month(mean.df_depth_mean, months=[7,8])
    FormatData(pathlib.Path('..\data')).filter_by_parameter(data,parameters=variables).to_csv(pathlib.Path(f"..\data/{variabler}_visit.txt"), sep="\t", encoding="cp1252", index = False)
    
    month_mean = mean.calculate_month_mean(data, variables)
    month_mean.to_csv(pathlib.Path(f"..\data/{variabler}_monthly.txt"), sep="\t", encoding="cp1252", index = False)
    year_mean = mean.calculate_year_mean(data, variables)
    year_mean.to_csv(pathlib.Path(f"..\data/{variabler}_summer_yearly.txt"), sep="\t", encoding="cp1252", index = False)
    
    