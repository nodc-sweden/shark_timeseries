from utils import calculateparameter as calculateparameter
from utils.format_raw_data import FormatData, get_data, save, get_date_columns
from utils.means import Means
import pathlib
import pandas as pd
import json


if __name__ == "__main__":
    with open("./settings/download_specs_BY31_syrerapport.json") as file:
        download_specs = json.load(fp=file)

    data_list = []
    for key, value in download_specs.items():
        data = get_data(
            result_directory = pathlib.Path("./data"), 
            datatype=value["datatype"],
            year_interval=value["year_interval"],
            stations=value["stations"],
            bounds=value["bounds"],
            no_download=value["no_download"],
        )
        data_list.append(data)

    data = pd.concat(data_list)

    mean = Means(data=data, result_directory=pathlib.Path("./data"))
    mean.calculate_depth_interval_mean(
        save_path=pathlib.Path("./data/mean"),
        interval=[[0, 10], [95, 105], [235, 250], [350, 500], [80], "BW"],
        BW_type="STATN",
    )

    save(mean.df_depth_mean, "./result/syrerapport_tidsserie_BY31_largerbounds.txt")
    # mean.df_depth_mean.to_csv(pathlib.Path(f"..\data/syrerapport_tidsserie_BY31_largerbounds.txt"), sep="\t", encoding="utf-8", index = False)
    # FormatData(pathlib.Path('..\data')).filter_by_parameter(mean.df_depth_mean).to_csv(pathlib.Path(f"..\data/syrerapport_tidsserie.txt"), sep="\t", encoding="cp1252", index = False)
    parameter = ['O2_H2S', "H2S_padded"]
    month_mean = mean.calculate_month_mean(mean.df_depth_mean, [parameter])
    save(month_mean, f"./result/{'_'.join(parameter)}_monthly.txt")
    # month_mean.to_csv(pathlib.Path(f"..\data/{parameter}_monthly.txt"), sep="\t", encoding="utf-8", index = False)
    save(
        mean.calculate_year_mean(month_mean, parameter),
        f"./result/{'_'.join(parameter)}_yearly.txt",
    )
    # mean.calculate_year_mean(month_mean, parameter).to_csv(pathlib.Path(f"..\data//{parameter}_yearly.txt"), sep="\t", encoding="utf-8", index = False)

    # Oxygen data only autumn
    data = FormatData(pathlib.Path("./data")).filter_by_month(
        mean.df_depth_mean, months=[8, 9, 10, 11]
    )
    FormatData(pathlib.Path("./data")).filter_by_parameter(
        data, parameters=parameter
    ).to_csv(
        pathlib.Path(f"./result/{'_'.join(parameter)}_visit.txt"),
        sep="\t",
        encoding="utf-8",
        index=False,
    )

    month_mean = mean.calculate_month_mean(mean.df_depth_mean, parameter)
    month_mean.to_csv(
        pathlib.Path(f"./result/{'_'.join(parameter)}_monthly.txt"),
        sep="\t",
        encoding="cp1252",
        index=False,
    )
    mean.calculate_year_mean(month_mean, parameter).to_csv(
        pathlib.Path(f"./result/{'_'.join(parameter)}_yearly.txt"),
        sep="\t",
        encoding="cp1252",
        index=False,
    )

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
