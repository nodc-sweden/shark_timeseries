from utils import calculateparameter as calculateparameter
from utils.sharkweb import Datasource
from utils.format_raw_data import FormatData
from utils.means import Means
import pathlib
import pandas as pd

def get_data(datatype: str, year_interval: list, stations: list, no_download: bool):
    print(f"no download is {no_download}")
    datasource = Datasource(pathlib.Path('..\data'), datatype = datatype, 
                            year_interval = year_interval, stations=stations, no_download=no_download)
    return FormatData(datasource.result_directory).load_raw_data(file_name=datasource.filename)

if __name__ == "__main__":
    
    download_specs = {
                      "Baltic_Proper": {"datatype": "Physical and Chemical", "year_interval": [1900, 2023], 
                                        "stations": 
                                        ["BY5 BORNHOLMSDJ", "BY15 GOTLANDSDJ", "BY31 LANDSORTSDJ", "BY32 NORRKÖPINGSDJ", "BY38 KARLSÖDJ", "BY29 / LL19", "BCS III-10", "HANÖBUKTEN"], 
                                        "no_download": False}
                      }
    data_list = []
    for key, value in download_specs.items():
        data = get_data(datatype=value["datatype"], year_interval=value["year_interval"], stations=value["stations"], no_download=value["no_download"])
        data_list.append(data)

    data = pd.concat(data_list)
    
    FormatData(pathlib.Path('..\data')).filter_by_parameter(data, ["O2_H2S", "H2S_padded"]).to_csv(pathlib.Path(f"..\data/H2S_profiles.txt"), sep="\t", encoding="cp1252", index = False)
    exit()
    print('means')
    mean = Means(data = data, result_directory=pathlib.Path('..\data'))
    mean.calculate_depth_interval_mean(save_path=pathlib.Path("..\data\mean"), interval = 'BW', BW_type = 'STATN')
    print(mean.df_depth_mean.columns)
    
    FormatData(pathlib.Path('..\data')).filter_by_parameter(mean.df_depth_mean, ["O2_H2S", "H2S_padded"]).to_csv(pathlib.Path(f"..\data/H2S_visit_BW.txt"), sep="\t", encoding="cp1252", index = False)
        # month_mean = mean.calculate_month_mean(mean.df_depth_mean, parameter)
        # month_mean.to_csv(pathlib.Path(f"..\data/{parameter}_monthly.txt"), sep="\t", encoding="cp1252", index = False)
        # mean.calculate_year_mean(month_mean, parameter).to_csv(pathlib.Path(f"..\data//{parameter}_yearly.txt"), sep="\t", encoding="cp1252", index = False)
    exit()
    # Oxygen data only autumn
    parameter = "O2_H2S"
    data  = FormatData(pathlib.Path('..\data')).filter_by_month(mean.df_depth_mean, months=[8, 9, 10, 11])
    FormatData(pathlib.Path('..\data')).filter_by_parameter(data, parameters = [parameter]).to_csv(pathlib.Path(f"..\data/{parameter}_visit.txt"), sep="\t", encoding="cp1252", index = False)

    month_mean = mean.calculate_month_mean(mean.df_depth_mean, parameter)
    month_mean.to_csv(pathlib.Path(f"..\data/{parameter}_monthly.txt"), sep="\t", encoding="cp1252", index = False)
    mean.calculate_year_mean(month_mean, parameter).to_csv(pathlib.Path(f"..\data//{parameter}_yearly.txt"), sep="\t", encoding="cp1252", index = False)
    
    # mean.calculate_depth_interval_mean(save_path=f"{datasource.result_directory}_mean", interval = [0,10], BW_type = 'STATN')
    # for parameter in ["PTOT", "NTOT"]:
    #     month_mean = mean.calculate_month_mean(mean.df_depth_mean, parameter)
    #     month_mean.to_csv(f"{datasource.result_directory}/{parameter}_monthly.txt", sep="\t", encoding="cp1252", index = False)
    #     mean.calculate_year_mean(month_mean, parameter).to_csv(f"{datasource.result_directory}/{parameter}_yearly.txt", sep="\t", encoding="cp1252", index = False)


    # mean = Means(data = dataframes[1], result_directory=datasource.result_directory)
    # mean.calculate_depth_interval_mean(save_path=f"{datasource.result_directory}_mean", interval = [[0,10], 'BW'], BW_type = 'STATN')
    # month_mean = mean.calculate_month_mean(mean.df_depth_mean, 'SLANG')
    # month_mean.to_csv(f"{datasource.result_directory}/SLANG_monthly.txt", sep="\t", encoding="cp1252", index = False)
    # mean.calculate_year_mean(month_mean, 'SLANG').to_csv(f"{datasource.result_directory}/SLANG_yearly.txt", sep="\t", encoding="cp1252", index = False)

    # dataframes[1].to_csv(f"{datasource.result_directory}/{datasource.filenames[0]}_formatted_cphl.txt", sep="\t", encoding="cp1252", index = False)

