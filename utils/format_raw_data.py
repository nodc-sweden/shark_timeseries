# -*- coding: utf-8 -*-
"""
Created on Wed Mar 14 09:26:17 2018

@author: a002087
"""

import re
import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.dates as mpldates
from decimal import Decimal, ROUND_HALF_UP
import pathlib
from utils import calculateparameter as calculateparameter

new_epoch = "1970-01-01T00:00:00"
mpldates.set_epoch(new_epoch)

columns = [
    "DTYPE",
    "DCSTAT",
    "DCBY",
    "MYEAR",
    "MONTH",
    "SHIPC",
    "VISITID",
    "CRUISE_NO",
    "STATN",
    "REP_STATN_NAME",
    "REG_ID",
    "REG_ID_GROUP",
    "PROJ",
    "ORDERER",
    "SDATE",
    "STIME",
    "EDATE",
    "ETIME",
    "SHARKID",
    "LATIT_DM",
    "LONGI_DM",
    "LATIT_DD",
    "LONGI_DD",
    "POSYS",
    "WADEP",
    "WINDIR",
    "WINSP",
    "AIRTEMP",
    "AIRPRES",
    "WEATH",
    "CLOUD",
    "WAVES",
    "ICEOB",
    "SECCHI",
    "Q_SECCHI",
    "ADD_SMP",
    "COMNT_VISIT",
    "DEPH",
    "PRES_CTD",
    "Q_PRES_CTD",
    "TEMP_BTL",
    "Q_TEMP_BTL",
    "TEMP_CTD",
    "Q_TEMP_CTD",
    "SALT_BTL",
    "Q_SALT_BTL",
    "SALT_CTD",
    "Q_SALT_CTD",
    "CNDC_25",
    "Q_CNDC_25",
    "CNDC_CTD",
    "Q_CNDC_CTD",
    "DOXY_BTL",
    "Q_DOXY_BTL",
    "DOXY_CTD",
    "Q_DOXY_CTD",
    "H2S",
    "Q_H2S",
    "PH",
    "Q_PH",
    "PH_LAB",
    "Q_PH_LAB",
    "PH_LAB_TEMP",
    "Q_PH_LAB_TEMP",
    "ALKY",
    "Q_ALKY",
    "ALKY_2",
    "Q_ALKY_2",
    "PHOS",
    "Q_PHOS",
    "PTOT",
    "Q_PTOT",
    "NTRI",
    "Q_NTRI",
    "NTRA",
    "Q_NTRA",
    "NTRZ",
    "Q_NTRZ",
    "AMON",
    "Q_AMON",
    "NTOT",
    "Q_NTOT",
    "SIO3-SI",
    "Q_SIO3-SI",
    "HUMUS",
    "Q_HUMUS",
    "CPHL",
    "Q_CPHL",
    "DOC",
    "Q_DOC",
    "POC",
    "Q_POC",
    "TOC",
    "Q_TOC",
    "PON",
    "Q_PON",
    "CURDIR",
    "Q_CURDIR",
    "CURVEL",
    "Q_CURVEL",
    "LIGNIN",
    "Q_LIGNIN",
    "YELLOW",
    "Q_YELLOW",
    "AL",
    "Q_AL",
    "UREA",
    "Q_UREA",
    "CDOM",
    "Q_CDOM",
    "TURB",
    "Q_TURB",
    "COMNT_SAMP",
    "WATER_CATEGORY",
    "WATER_DISTRICT",
    "SEA_AREA_NAME",
    "SEA_AREA_CODE",
    "WATER_TYPE_AREA",
    "SEA_BASIN",
    "HELCOM-OSPAR_AREA",
    "ALABO",
    "EEZ",
    "COUNTY_NAME",
    "MUNICIPALITY_NAME",
    "VISS_EU_ID",
    "DATA_CENTRE",
    "INTERNET_ACCESS",
    "DATA_SET_NAME",
    "FILE_NAME",
    "SLABO_PHYSCHEM",
    "MPROG",
    "RLABO",
    "PROJ_ENGLISH",
    "RLABO_ENGLISH",
    "ORDERER_ENGLISH",
    "SLABO_ENGLISH",
    "ALABO_ENGLISH",
]


class FormatData:
    def __init__(self, result_directory: str) -> None:
        self.result_directory = result_directory
        self.encoding = "utf-8"
        self.columns_to_keep = [
            "STATN",
            "VISITID",
            "REG_ID",
            "SDATE",
            "MONTH",
            "LATIT_DD",
            "LONGI_DD",
        ]

    def load_raw_data(self, file_path):
        """
        reads textfiles from sharkweb with short headers or from lims export
        """

        file_path  # = pathlib.Path(self.result_directory, file_name)
        print(f"reading file {file_path}\n... ... ...")
        data_columns = pd.read_csv(
            open(file_path, encoding=self.encoding), sep="\t", nrows=1
        )

        # Raise error if data has wrong headers
        if not all(
            column in data_columns.columns
            for column in ["STATN", "SDATE", "MYEAR", "REG_ID"]
        ):
            raise KeyError("wrong header format, use short format header")

        str_columns = {
            col: str
            for col in data_columns.columns
            if col.startswith(("Q_", "QFLAG", "COMNT"))
        }
        int_columns = {col: str for col in ["REG_ID", "REG_ID_GROUP"]}
        str_columns.update(int_columns)
        skip_cols = [
            "ETIME",
            "EDATE",
            "LIGNIN",
            "Q_LIGNING",
            "YELLOW",
            "Q_YELLOW",
            "UREA",
            "Q_UREA",
        ]
        use_cols = [col for col in data_columns.columns if col not in skip_cols]

        data_list = []
        with pd.read_csv(
            open(file_path, encoding=self.encoding),
            sep="\t",
            parse_dates=["SDATE"],
            usecols=use_cols,
            dtype=str_columns,
            chunksize=100000,
        ) as reader:
            for data in reader:
                data_list.append(data)
                print("done reading chunk")

        data = pd.concat(data_list)
        data = self.format_data(data)

        return data

    def format_data(self, data):
        data = self.format_date_columns(data)
        # Convert VISITID to str and pad with zeros
        data["VISITID"] = data["VISITID"].astype(str).apply(lambda row: row.zfill(4))

        # data that is downloaded from shark in row format for datatype Chlorophyll has one column named VALUE that contains all parameters
        if "VALUE" in data.columns and set(data["DTYPE"].unique()) == set(
            ["Chlorophyll"]
        ):
            data = self.format_chl_hose_data(data)
        else:
            # data files from LIMS does not contain DD lat and long
            if "LATIT_DD" not in data.columns:
                data = self.load_lims(data)
            data = self.format_fyskem_data(data)

        if "DEPH" in data.columns:
            data.sort_values(by=["numDATE", "STATN", "DEPH"], inplace=True)
        else:
            data.sort_values(by=["numDATE", "STATN"], inplace=True)

        return data

    def load_lims(self, data):
        """
        Special handling of data export from LIMS
        """
        # data files from LIMS does not contain decimal degree lat and long
        data[["LATIT_DD", "LONGI_DD"]] = data[["LATIT", "LONGI"]].applymap(
            self.degmin_to_decdeg
        )

        # LIMS export contains quality flags in value column, this needs to be split into Q_flag column.
        value_columns = [
            "TEMP_BTL",
            "TEMP_CTD",
            "SALT",
            "SALT_BTL",
            "SALT_CTD",
            "DOXY_BTL",
            "DOXY_CTD",
            "PHOS",
            "PTOT",
            "SIO3-SI",
            "NTRZ",
            "NTRA",
            "NTRI",
            "AMON",
            "NTOT",
            "H2S",
            "CPHL",
            "PH",
            "ALKY",
        ]

        # remove all bad data (flagged with B)
        for column in data.columns:
            if "Q_" in column and "B" in data[column].values:
                # set values in data[column.strip('_Q')] to np.nan where data[column] == 'B'
                data.loc[data[column] == "B", column.strip("_Q")] = np.nan
        data = self.split_qflag_from_values(data, value_columns)

        return data

    def format_chl_hose_data(self, data):
        """
        Formats row data from datatype Chlorophyll to so it can be merged with column data from datatype Physical/Chemical
        Reads file, renames columns, adds a column with the depth interval for the integrated sample
        """
        print("Formatting Chlorophyll hose data")

        # Rename columns, and select desired columns
        data = data.rename(columns={"VALUE": "SLANG"})

        # Convert SLANG to float
        data["SLANG"] = data["SLANG"].astype(float)

        # Add depth interval columns
        data["hose Depth interval"] = data.apply(
            lambda row: f"{int(row['MNDEP'])}-{int(row['MXDEP'])} m", axis=1
        )
        data["Depth_interval"] = data.apply(
            lambda row: "0-10 m"
            if row["hose Depth interval"] in ["0-10 m", "0-14 m"]
            else row["hose Depth interval"],
            axis=1,
        )

        columns_to_keep = ["SLANG", "SMTYP", "MNDEP", "MXDEP", "Depth_interval"]
        columns_to_keep.extend(self.columns_to_keep)
        if not all(True for column in columns_to_keep if column in data.columns):
            print(
                f"not all columns to kepp exist in dataframe. These are the columns in the data {data.columns}"
            )

        return data[columns_to_keep]

    @staticmethod
    def _get_metadata(data: pd.DataFrame):
        print([col for col in data.columns])
        print(
            data.groupby(
                ["STATN", "WATER_CATEGORY", "SEA_AREA_CODE", "REG_ID", "REG_ID_GROUP"]
            ).agg({"YEAR": ["min", "max", "unique"]})
        )

    def format_fyskem_data(self, data):
        """
        Format data from datatype Physical/Chemical and LIMS export
        """
        print("Formatting Physical/Chemical data")
        # data["profile"] = data.groupby(["STATN", "SDATE"], group_keys=False).apply(
        #         lambda grp: grp.STATN + "_" + grp.strDATE
        # )

        data["profile"] = data.groupby(
            ["STATN", "SDATE", "VISITID"], group_keys=False
        ).apply(lambda grp: grp.STATN + "_" + grp.strDATE + "_" + grp.VISITID)
        data.rename(columns={"Silicate SiO3-Si (umol/l)": "SIO3-SI"}, inplace=True)

        data = self.calculate_parameters(data.copy())

        columns_to_keep = [
            "profile",
            "DEPH",
            "TEMP",
            "TEMP_BTL",
            "TEMP_CTD",
            "SALT",
            "SALT_BTL",
            "SALT_CTD",
            "O2",
            "DOXY_BTL",
            "DOXY_CTD",
            "O2sat",
            "O2_H2S",
            "H2S",
            "Q_H2S",
            "H2S_padded",
            "PHOS",
            "PTOT",
            "orgP",
            "SIO3-SI",
            "NTRZ",
            "Q_NTRZ",
            "NTRA",
            "Q_NTRA",
            "NTRI",
            "Q_NTRI",
            "NTOT",
            "AMON",
            "Q_AMON",
            "din_simple",
            "din_complex",
            "orgN",
            "sumNOx",
            "CPHL",
            "PH",
            "ALKY",
        ]
        columns_to_keep.extend(self.columns_to_keep)

        return data[columns_to_keep]

    def correct_station_names(self, data):
        """
        Corrects station names to how they look in fyskem
        """
        print("Correcting station names")
        if "STATN" not in data.columns:
            data = data.rename(columns={"station_name": "STATN"})

        data["org_STATN"] = data.STATN.copy(deep=True)

        replacements = {
            "A13": "F9 / A13",
            "A5": "F3 / A5",
            "RA1": "RÅNEÅ-1",
            "RA2": "RÅNEÅ-2",
            "C14": "MS4 / C14",
            "MS4": "MS4 / C14",
            "GA1": "GAVIK-1",
            "4-CTRY-BP": "BCS III-10 NE",
            "BY29/LL19": "BY29 / LL19",
            "BY31  LANDSORTDJ": "BY31 LANDSORTDJ",
            "BY32 NORRKÖPINGSDJUPET": "BY32 NORRKÖPINGSDJ",
            "BY38  KARLSÖDJ": "BY38 KARLSÖDJ",
            "BY20  FÅRÖDJ": "BY20 FÅRÖDJ",
            "BY2  ARKONA": "BY2 ARKONA",
            "TRÖSKELN": "SLÄGGÖ",
            "ST-12": "SLÄGGÖ",
            "TRÖSKELN (SLÄGGÖ)": "SLÄGGÖ",
            "TRÖSKELN(SLÄGGÖ)": "SLÄGGÖ",
            "SLÄGGÖ (TRÖSKELN)": "SLÄGGÖ",
            "FJ28 TRÖSKELN (SLÄGGÖ)": "SLÄGGÖ",
            "SLÄGGÖ (GULLMAR TRÖSKEL)": "SLÄGGÖ",
            "SLÄGGÖ(GULLMAR TRÖSKEL)": "SLÄGGÖ",
        }

        for wrong_name, correct_name in replacements.items():
            data.loc[data.STATN == wrong_name, "STATN"] = correct_name

        synonyms = {
            "BCS III-10 NE": "BCS III-10",
            "NB1": "B7 & NB1 / B3",
            "NB 1": "B7 & NB1 / B3",
            "NB1 / B3": "B7 & NB1 / B3",
            "B7": "B7 & NB1 / B3",
        }

        for synonym_name, actual_name in synonyms.items():
            data.loc[data.STATN == synonym_name, "STATN"] = actual_name

        # Remove stations that may appear when searching for station name contents
        data = data[
            ~data.STATN.isin(["NB1 (VICINITY)", "NB1 / B3  (1N)", "NB1 / B3  (2.5W)"])
        ]

        return data

    def remove_stations_in_wrong_location(self, data):
        """
        Only relevant for data from sharkweb if the data selection there was very broad.
        """

        # Drop stations with name that occur in multiple locations
        drop_idx = []

        # Station B1
        drop_idx.extend(
            data.index[(data.STATN == "B1") & (data.LATIT_DD < 58.79)].tolist()
        )

        # Station H4
        drop_idx.extend(
            data.index[(data.STATN == "H4") & (data.LONGI_DD < 16)].tolist()
        )

        # Station C3
        drop_idx.extend(
            data.index[(data.STATN == "C3") & (data.LATIT_DD < 62.6)].tolist()
        )

        # Stations F2, F3, F7, F9, F12, F13, SR3, US6, US6B, US7
        drop_idx.extend(
            data.index[
                (
                    data.STATN.isin(
                        [
                            "F2",
                            "F3",
                            "F7",
                            "F9",
                            "F12",
                            "F13",
                            "SR3",
                            "US6",
                            "US6B",
                            "US7",
                        ]
                    )
                )
                & (data.LATIT_DD < 62)
            ].tolist()
        )

        # Drop the stations
        data = data.drop(drop_idx)

        return data

    def round_value(self, value: float, nr_decimals=2) -> str:
        """Calculate rounded value."""

        return str(
            Decimal(str(value)).quantize(
                Decimal("%%1.%sf" % nr_decimals % 1), rounding=ROUND_HALF_UP
            )
        )

    def degmin_to_decdeg(self, degmin):
        """
        Convert position from the format decdegreesdecimalminutes to decimal degrees.
        Example: convert 5815.586 to decimal degrees:
            58 + 15.586/60 = 58.25977
        """

        deg = int(str(degmin)[0:2])
        decdegree = float(self.round_value(float(str(degmin)[2:]) / 60, 4))
        return deg + decdegree

    def decdeg_to_degmin(self, dd):
        """
        Convert position from decimal degrees into degrees and decimal minutes.
        Example: convert 58.25977 (decimal degrees) to 58 15.5862

        """
        mnt, sec = divmod(dd * 3600, 60)
        deg, mnt = divmod(mnt, 60)
        mnt = self.round_value(mnt + sec / 60, 4)
        if np.isnan(deg):
            return ""
        r = "{} {}".format(int(deg), mnt)
        return r

    def split_qflag_from_values(self, data, value_columns):
        for column_name in value_columns:
            if column_name in data.columns:
                data[f"Q_{column_name}"] = data.apply(
                    lambda row: self.extract_qflag(
                        row[column_name], row[f"Q_{column_name}"]
                    ),
                    axis=1,
                )
                data[column_name] = pd.to_numeric(
                    data[column_name].apply(lambda row: self.extract_value(row))
                )

        return data

    def extract_qflag(self, value, q_flag):
        if isinstance(value, str):
            match = re.findall("[BSE<>]+", str(value))
            if match and not isinstance(q_flag, str):
                return match[0]
        return q_flag

    def extract_value(self, x):
        return float(re.sub("[BSE<>]", "", str(x)))

    def format_date_columns(self, data: pd.DataFrame):
        """
        Create additional columns
        for the date in different formats.
        """
        data["strDATE"] = data["SDATE"].dt.strftime("%Y-%m-%d")
        data["dtDATE"] = pd.to_datetime(data["strDATE"], format="%Y-%m-%d")
        data["numDATE"] = data["dtDATE"].apply(lambda x: mpldates.date2num(x))
        data["YEAR"] = data["SDATE"].dt.year
        data["MONTH"] = data["SDATE"].dt.month
        data["DAY"] = data["SDATE"].dt.day

        self.columns_to_keep.extend(["strDATE", "numDATE", "YEAR", "DAY"])

        return data

    def get_season(month: int):
        """
        return a "winter", "spring", "autumn" or "summer" from the given month
        """

        if month < 3 or month == 12:
            return "winter"
        elif month >= 3 and month <= 5:  # changed from 3-5
            return "spring"
        elif month >= 6 and month <= 8:
            return "summer"
        elif month >= 9 and month <= 11:
            return "autumn"
        else:
            return False

    def filter_by_month(
        self, data: pd.DataFrame, months: list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    ):
        return data.loc[data["MONTH"].isin(months)]

    def filter_by_parameter(self, data: pd.DataFrame, parameters: list = []):
        data.sort_values(
            by=["STATN", "REG_ID", "YEAR", "MONTH", "DAY", "SDATE"], inplace=True
        )
        out_parameters = parameters + [
            "profile",
            "DEPH",
            "STATN",
            "REG_ID",
            "YEAR",
            "MONTH",
            "DAY",
            "SDATE",
            "Depth_interval",
        ]
        print(
            f"remove the following columns that are not in data: {[column for column in out_parameters if column not in data.columns]}"
        )
        print([column for column in out_parameters if column in data.columns])

        return data.loc[
            :, [column for column in out_parameters if column in data.columns]
        ]

    def calculate_parameters(self, data):
        """
        Calculates standard parameters
        simple DIN, DIN as sharktoolbox, negative oxugen,
        """

        print("adding combined BTL/CTD columns...")
        data.loc[:, "SALT"] = data.copy().apply(
            lambda row: calculateparameter.get_prio_par(row.SALT_CTD, row.SALT_BTL),
            axis=1,
        )
        data.loc[:, "TEMP"] = data.copy().apply(
            lambda row: calculateparameter.get_prio_par(row.TEMP_CTD, row.TEMP_BTL),
            axis=1,
        )
        data.loc[:, "O2"] = data.copy().apply(
            lambda row: calculateparameter.get_prio_par_oxy(
                row.DOXY_BTL, row.DOXY_CTD, row.Q_DOXY_BTL, row.Q_DOXY_CTD
            ),
            axis=1,
        )
        data.loc[:, "O2sat"] = data.copy().apply(
            lambda row: calculateparameter.calculate_o2sat(
                o2=row.O2, s=row.SALT, t=row.TEMP
            ),
            axis=1,
        )
        print("adding H2S with zero values")
        data.loc[:, "H2S_padded"] = data.copy().apply(
            lambda row: calculateparameter.get_zero_H2S(
                row.DOXY_BTL, row.H2S, row.Q_DOXY_BTL, row.Q_H2S
            ),
            axis=1,
        )
        print("calculating negative oxygen")
        data.loc[:, "O2_H2S"] = data.copy().apply(
            lambda row: calculateparameter.get_O2_H2S(
                row.DOXY_BTL, row.H2S, row.Q_DOXY_BTL, row.Q_H2S
            ),
            axis=1,
        )
        print("adding calculated NOx column...")
        data.loc[:, "sumNOx"] = data.copy().apply(
            lambda row: calculateparameter.get_sumNOx(
                row.NTRZ, row.NTRA, row.NTRI, row.Q_NTRZ, row.Q_NTRA, row.Q_NTRI
            ),
            axis=1,
        )
        print("adding simple DIN...")
        data.loc[:, "din_simple"] = data.copy().apply(
            lambda row: calculateparameter.get_din_simple(row.sumNOx, row.AMON), axis=1
        )
        # no2, no3, nox, nh4, h2s, qh2s, qnh4, qnox, qno3,
        print("adding complex DIN...")
        data.loc[:, "din_complex"] = data.copy().apply(
            lambda row: calculateparameter.get_din_complex(
                row.NTRI,
                row.NTRA,
                row.NTRZ,
                row.AMON,
                row.H2S,
                row.Q_H2S,
                row.Q_AMON,
                row.Q_NTRZ,
                row.Q_NTRA,
                row.Q_NTRI,
            ),
            axis=1,
        )
        print("adding calculated organic N and P ...")
        data.loc[:, "orgN"] = data.copy().apply(
            lambda row: calculateparameter.get_org(
                row.NTOT, row.din_complex, "", row.Q_NTOT
            ),
            axis=1,
        )
        data.loc[:, "orgP"] = data.copy().apply(
            lambda row: calculateparameter.get_org(row.PTOT, row.PHOS, "", row.Q_PTOT),
            axis=1,
        )

        return data


if __name__ == "__main__":
    today = dt.date.today().strftime("%Y-%m-%d")
    print(today)

    folder_data = f"../data"
    file_list = [
        "Å17_area_1960-2024"
        # "sharkweb_data_20210607_1960_2020_fyskem",
        # "sharkweb_data_20210607_1960_2020_cphl",
        #  "sharkweb_data_20220318_2020-2021_fyskem", "sharkweb_data_20220502_2020-2021_cphl",
        #  "sharkweb_data_20230824_2021-2022_cphl", "sharkweb_data_20230824_2021-2022_fyskem"
    ]

    for file in file_list:
        # read data and format
        if pathlib.Path(f"{folder_data}/{file}_formatted_{today}.txt").exists():
            continue
        data = FormatData(result_directory=folder_data).load_raw_data(
            file_path=f"{file}.txt"
        )
        # save the result
        data.to_csv(
            f"{folder_data}/{file}_formatted_{today}.txt",
            sep="\t",
            encoding="utf-8",
            index=False,
        )
