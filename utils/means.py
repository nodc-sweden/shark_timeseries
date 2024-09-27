# -*- coding: utf-8 -*-
"""
@author: a002087
"""
import sys
import pandas as pd
import datetime as dt
import matplotlib.dates as mpldates
import pickle
import json
import os
import pathlib

current_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current_path)

class Means():

    def __init__(self, data: pd.DataFrame, result_directory: str, file_name: str = False):

        self.result_directory = result_directory

        if file_name: 
            file_path = pathlib.Path(self.result_directory, file_name)
            self.load_calculated_df(file_path)
        else:
            self.data = data
        self.format_check()
        self.data = self.format_date_columns(self.data)
        print(f"data columns for means: {self.data.columns}")

        with open("./settings/station_information.json",
            encoding="utf-8",) as f:
                self.station_information = json.load(f)

    def load_calculated_df(self, path):
        print("load file {}".format(path))
        # TODO: check if file exists, else return to read_path
        self.data = pd.read_csv(open(path, encoding="cp1252"), sep="\t")
        print("loading done")

    def format_check(self):

        for column in ["STATN", "SDATE"]:
            if not column in self.data.columns:
                raise KeyError(f"missing {column} in data. Make sure to format data using format_raw_data.py")

    def save_data(self, save_path, data, save_format = ['txt']):

        if 'pickle' in save_format:
            pickle.dump(self.data, open(f"{save_path}.pkl", "wb"))
        if 'txt' in save_format:
            data.sort_values(by=['numDATE', 'STATN', 'DEPH'], inplace=True)
            data.to_csv(
                f"{save_path}.txt",
                sep="\t",
                encoding="cp1252",
                index = False
            )

    def format_date_columns(self, data: pd.DataFrame):
        """
        Create additional columns
        for the date in different formats.
        """
        data["YEAR"] = data["SDATE"].dt.year
        data["MONTH"] = data["SDATE"].dt.month
        data["DAY"] = data["SDATE"].dt.day

        if not 'numDATE' in data.columns:
            data["numDATE"] = data.SDATE.apply(lambda x: mpldates.date2num(x))

        print(data.columns)        
        return data

    def _get_depth_range(self, depth):
        """
        Get the depth range based on the input parameter
        """
        if isinstance(depth, list):  # If depth is a list
            for d in depth:
                if not isinstance(d, (float, int)):
                    raise ValueError(f"Invalid depth {repr(d)} of {type(d)}, must be float or integer")
            if len(depth) == 1:
                return depth[0], depth[0]
            elif len(depth) == 2:
                return min(depth), max(depth) 
            else:
                raise ValueError("Invalid depth, must be a list with length 1 or 2")

    def _get_depth_str(self, depth):

        if len(depth) == 1:
            return f"{depth[0]} m"
        elif len(depth) == 2:
            if max(depth) >= 999:
                return f">= {min(depth)} m"
            else:
                return f"{min(depth)}-{max(depth)} m"

    def filter_BW(self, BW_type):

        print("Constructing bottomwater dataframe.....")
        print(self.data['STATN'].unique())
        depth_str = f"{BW_type} bottomwater depths"
        self.data["BW_bool"] = 0
        self.data["BW"] = ""
        if BW_type == "STATN":
            bw_key = "STN_BW"
        elif BW_type == "BASIN":
            bw_key = "BASIN_BW"
        else:
            raise ValueError(f"Invalid specification for bottom water (BW_type), must be a STATN or BASIN but is {BW_type}")

        for stn, values in self.station_information.items():
            if stn not in self.data['STATN'].unique():
                continue
            BW_depth = values[bw_key]
            
            if len(self.data.loc[(self.data.STATN == stn) & (self.data.DEPH >= BW_depth)]["DEPH"].unique()) == 0:
                print(f"no data >= {BW_depth} at {stn}")
                continue
            else:
                print(f"{stn} has data >= {BW_depth}. {self.data.loc[(self.data.STATN == stn) & (self.data.DEPH >= BW_depth)]['DEPH'].unique()}")
            self.data.loc[(self.data.STATN == stn) & (self.data.DEPH >= BW_depth), "BW_bool"] = 1
            self.data.loc[(self.data.STATN == stn) & (self.data.DEPH >= BW_depth), "Depth_interval"] = f">= {BW_depth} m"
            depth_str = f">= {BW_depth} m"
        df_selection = self.data.loc[self.data.BW_bool == 1, :]
        # df_selection = self.data.loc[(self.data.STATN == stn) & (self.data.DEPH >= BW_depth), :]

        print("check BW")
        print("depths in selection are {}".format(df_selection.DEPH.unique()))
        print("depths in input are {}".format(depth_str))

        return df_selection

    def filter_by_depth(self, depth: list, BW_type = ""):
        """
        Special case for bottomwater
        """
        if isinstance(depth, str):
            if BW_type in ["STATN", "BASIN"]:
                df_selection = self.filter_BW(BW_type)
                df_selection['Depth_interval']
                # depth_str = f"{BW_type} Bottom water"
            else:
                raise ValueError(f"Invalid specification for bottom water (BW_type), must be a STATN or BASIN but is {BW_type}")
        
        elif isinstance(depth, list):
            depth_range = self._get_depth_range(depth)
            depth_str = self._get_depth_str(depth)
            print("Filtering dataframe to depth range: {}-{} m".format(depth_range[0], depth_range[1]))
            df_selection = self.data.loc[
                (self.data.DEPH >= depth_range[0]) & (self.data.DEPH <= depth_range[1])
                ]
            df_selection['Depth_interval'] = depth_str
        else:
            raise ValueError("Invalid depth, must be a list or a str")
        
        print(f"BW dataselcetion columns: {df_selection.columns}")
        return df_selection

    def calculate_depth_means(self, depth, BW_type=""):

        df_selection = self.filter_by_depth(depth, BW_type)

        value_cols = ["SALT", "TEMP", "PHOS", "SIO3-SI", "sumNOx", "NTRA", 
                    "NTRZ", "AMON", "din_complex", "din_simple", "O2_H2S", "O2sat", "H2S_padded", "NTOT", "PTOT", "CPHL", 
                    "DEPH", "PH", "ALKY",]

        # Calculate mean by station and date
        # Leaves only the columns used in groupby call and values_cols
        df_depth_mean = (
            df_selection.groupby(["STATN", "REG_ID", "SDATE", "YEAR", "MONTH", "DAY", "Depth_interval"])
            .agg(dict.fromkeys(value_cols, "mean"))
            .reset_index()
        )

        # this adds necessary columns for date, year, and month
        try:
            df_depth_mean = self.format_date_columns(df_depth_mean)
        except ValueError as e:
            raise ValueError(f'{e}\n, {df_depth_mean.STATN.unique()}')
        
        return df_depth_mean

    def calculate_depth_interval_mean(self, save_path: str, interval, BW_type = ""):
        """
        calculate mean values and save the resulting data
        argument:
            - interval list or string. String is for bottomwater mean. list is for other depth choices
                - list with length 1, representing to get mean value at discrete depth
                - list with length 2, to get mean in a depth interval
                - list with lists of the above
        """

        # data = load_calculated_df(load_path)
        if isinstance(interval[0], list):
            result = []
            for interval_x in interval:
                result.append(self.calculate_depth_means(interval_x, BW_type))
            df_depth_mean = pd.concat(result)
            # self.save_data(save_path=f"{save_path}_multiple_mean", data=df_depth_mean)
        else:
            df_depth_mean = self.calculate_depth_means(interval, BW_type)
            # if interval[0] == 0 and interval[1] == 10:
            #     self.save_data(save_path=f"{save_path}_surface_mean", data=df_depth_mean)
            # else:
            #     self.save_data(save_path=f"{save_path}_{interval[0]}-{interval[1]}_mean", data=df_depth_mean)
        
        self.df_depth_mean = df_depth_mean
        
        return df_depth_mean

    # def calculate_BW_mean(self, save_path, BW_type="STATN"):
    #     # data = load_calculated_df(load_path)
    #     df_depth_mean = self.calculate_depth_means("BW", BW_type)
    #     self.save_data(save_path=f"{save_path}_{BW_type}_bottom_mean", data=df_depth_mean)

    def calculate_month_mean(self, data: pd.DataFrame, param: list):
        # Calculate mean by station and date
        # Leaves only the columns used in groupby call and values_cols
        group_cols = ["STATN", "REG_ID", "YEAR", "MONTH", "Depth_interval"]
        
        mean_data = data.groupby(group_cols).mean().reset_index().round(3)
        return mean_data
    
    def calculate_season_mean(self, data: pd.DataFrame, param: list, months: list = [1,2,3,4,5,6,7,8,9,10,11,12], suffix = 'yearly', first_month_mean = True):
        
        # Definiera kolumner att gruppera på
        group_cols = ["STATN", "REG_ID", "YEAR", "Depth_interval"]

        # Skapa en dictionary med aggregeringsfunktionerna

        if all([j - i == 1 for i, j in zip(months[:-1],months[1:])]):
            print(f"not winter {months}")

        if all((m in months and m not in [11] for m in [1, 12])):
            suffix = 'vinter'
            mask = data['MONTH'].isin([12])
            data.loc[mask, "YEAR"] = data.loc[mask, "YEAR"] + 1
        
        if any((m in months for m in [4, 5])):
            suffix = 'vår'
        if any((m in months for m in [6,7,8])):
            suffix = 'sommar'
        if any((m in months for m in [9,10,11])):
            suffix = 'höst'

        # välj ut data
        data_selection = data.loc[data['MONTH'].isin(months)][param+group_cols+['MONTH']].copy(deep=True)

        if first_month_mean:
            # print('first calculating month mean....')
            data_selection = self.calculate_month_mean(data_selection, param)
            # print('then calculating season mean on the month mean timeseries...')

        # Initiera en lista för att samla aggregeringsresultaten
        results = []
        # Utför gruppering och aggregering
        for variable in param:
            grouped = data_selection.dropna(subset=[variable]).groupby(group_cols).agg(value=(variable, 'mean'),
                    count=(variable, 'count'),
                    unique=('MONTH', 'unique'))
            # Återställ index för att få en platt DataFrame
            grouped = grouped.reset_index().round(3)
            grouped['depth_desc'] = grouped['Depth_interval'].apply(lambda x: 'Bottenvatten' if '>' in x else x)
            grouped['Mätvariabel'] = variable + " " + grouped['depth_desc'] + " - " + suffix
            grouped['variable'] = variable
            grouped['period'] = suffix
            results.append(grouped)

        # Kombinera alla aggregeringsresultaten till en enda DataFrame
        final_result = pd.concat(results, ignore_index=True)

        return final_result
        # Calculate mean by station and date
        # Leaves only the columns used in groupby call and values_cols
        # print('first calculating month mean....')
        # month_mean = self.calculate_month_mean(data_selection, param)
        # print(month_mean.head(3))
        # print('then calculating season mean on the month mean timeseries...')
        season_mean = (
            data_selection.loc[data_selection].groupby(["STATN", "REG_ID", "YEAR", "Depth_interval"])
            .agg(value=(param, 'mean'),
                count=(param, 'count'),
                unique=('MONTH', 'unique'))
            .reset_index()
        )

        # Add suffix to all values in the specified column
        season_mean['variable'] = season_mean['variable'].apply(lambda x: x + '-' + suffix)

        return season_mean
    
    def calculate_year_mean(self, data: pd.DataFrame, param: list, first_month_mean = True):

        # Definiera kolumner att gruppera på
        group_cols = ["STATN", "REG_ID", "YEAR", "Depth_interval"]

        # filtrera ut endast de kolumner som ska användas:
        data_selection = data[param+group_cols+['MONTH']].copy(deep=True)
        # Skapa dictionary för aggregering

        if first_month_mean:
            # print('first calculating month mean....')
            data_selection = self.calculate_month_mean(data_selection, param)
        # Initiera en lista för att samla aggregeringsresultaten
        results = []
        # Utför gruppering och aggregering
        for variable in param:
            data_selection.dropna(subset=[variable])
            grouped = data_selection.dropna(subset=[variable]).groupby(group_cols).agg(value=(variable, 'mean'),
                    count=(variable, 'count'),
                    unique=('MONTH', 'unique'))
            # Återställ index för att få en platt DataFrame
            grouped = grouped.reset_index().round(3)
            # Skapa en ny kolumn baserat på villkoret med apply och lambda
            grouped['depth_desc'] = grouped['Depth_interval'].apply(lambda x: 'Bottenvatten' if '>' in x else x)
            grouped['Mätvariabel'] = variable + " " + grouped['depth_desc'] + " - " + "helår"
            grouped['variable'] = variable
            grouped['period'] = 'helår'
            results.append(grouped)

        # Kombinera alla aggregeringsresultaten till en enda DataFrame
        final_result = pd.concat(results, ignore_index=True)

        return final_result
        print('first calculating month mean....')
        month_mean = self.calculate_month_mean(data, param)
        print(month_mean.head(3))
        print('then calculating year mean on the month mean timeseries...')
        year_mean = month_mean.groupby(["STATN", "REG_ID", "YEAR", "Depth_interval", "variable"]).agg(year_mean=('month_mean', 'mean'),
                        count=('month_mean', 'count'),
                        unique=('MONTH', 'unique')).reset_index().round(3)
        
        # mean_data = (data.groupby(["STATN", "REG_ID", "YEAR", "Depth_interval"])
        #             .agg(mean=('mean', 'mean'),
        #                 count=('mean', 'count'))
        #             .reset_index()
        #         )

        return year_mean
        
if __name__ == "__main__":
    # dataframe to calculate means on
    today = dt.date.today().strftime("%Y-%m-%d")
    print(today)
    
    folder_data = f"../data"
    file_list = ["Å17_area_1960-2024_formatted_2024-02-22",
                #  "sharkweb_data_20210607_1960_2020_fyskem_formatted_2023-06-07",
                #  "sharkweb_data_20220318_2020-2021_fyskem_formatted_2023-06-07",
                #  "sharkweb_data_20230824_2021-2022_fyskem_formatted_2023-08-24"
                 ]

    for file in file_list:
        # if pathlib.Path(f"{folder_data}/{file}_mean_{today}.txt").exists:
        #     continue
        Means(data = None, result_directory=f"{folder_data}", file_name=f'{file}.txt').calculate_depth_interval_mean(save_path=f"{folder_data}/{file}_mean_{today}", interval = [[0,10], 'BW'], BW_type = 'STATN')
        # calculate_BW_mean(load_path=f"{data_folder}/raw/{file}.txt", save_path=f"{data_folder}/aggregated/{file}_{today}", BW_type="STATN")

    # folder_data = "C:/LenaV/python3/w_annualreport/data/arsrapport/2022"
    # folder_data = f"//winfs-proj/proj/havgem/LenaV/Projekt/SyreH2S/data/2022"
    # file = "sharkweb_data_BY5_namesearch_formatted_2023-05-05"

    # calculate_depth_interval_mean(load_path=f"{folder_data}/{file}.txt", save_path=f"{folder_data}/{file}_{today}", interval = [[50], [80,100]])
    #calculate_BW_mean(load_path=f"{folder_data}/raw/{file}.txt", save_path=f"{folder}/aggregated/{file}_{today}", BW_type="STATN")