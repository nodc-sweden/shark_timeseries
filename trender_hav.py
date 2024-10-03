from utils import calculateparameter as calculateparameter
from utils.sharkweb import Datasource
from utils.format_raw_data import FormatData
from utils.means import Means
import pathlib
import pandas as pd
import json


def get_data(
    result_directory,
    datatype: str,
    year_interval: list,
    stations: list,
    no_download: bool,
    name: str,
):
    datasource = Datasource(
        result_directory=result_directory,
        datatype=datatype,
        year_interval=year_interval,
        stations=stations,
        no_download=no_download,
        name=name,
    )
    return FormatData(datasource.result_directory).load_raw_data(
        file_path=datasource.filepath
    )


if __name__ == "__main__":
    with open("./settings/trender_HaV_download_specs.json") as file:
        download_specs = json.load(fp=file)

    data_list = []
    for key, value in download_specs.items():
        data = get_data(
            result_directory=pathlib.Path("./data"),
            datatype=value["datatype"],
            year_interval=value["year_interval"],
            stations=value["stations"],
            no_download=value["no_download"],
            name=value["name"],
        )
        data_list.append(data)
    data = pd.concat(data_list)

    mean = Means(data=data, result_directory=pathlib.Path("./data"))
    mean.calculate_depth_interval_mean(
        save_path=pathlib.Path("./data/mean"), interval=[[0, 10], "BW"], BW_type="STATN"
    )

    # save default keyword args
    save_kwargs = {
        "sep": "\t",
        "encoding": "utf-8",
        "index": False,
        "float_format": "%.2f",
    }

    trender_HaV_setup = {
        "yearly": {"variables": ["PTOT", "NTOT", "SALT"]},
        "autumn": {"variables": ["O2_H2S"], "months": [7, 8, 9, 10]},
        "summer": {"variables": ["TEMP", "PHOS"], "months": [7, 8]},
        "winter": {
            "variables": ["TEMP", "NTRZ", "PHOS", "SIO3-SI"],
            "months": [1, 2, 12],
        },
    }

    result = []
    for setup, details in trender_HaV_setup.items():
        if setup == "yearly":
            data = mean.calculate_year_mean(
                mean.df_depth_mean, details["variables"], first_month_mean=False
            )
        else:
            data = mean.calculate_season_mean(
                mean.df_depth_mean,
                param=details["variables"],
                months=details["months"],
                first_month_mean=False,
            )
        data.to_csv(
            pathlib.Path(f"./result/{'_'.join(details['variables'])}_{setup}.txt"),
            **save_kwargs,
        )
        result.append(data)

    final_result = pd.concat(result)
    # remove results that HaV are not interested in
    # ~ tecknet inverterar urvalet så att allt som inte stämmer på valet är det som blir kvar
    # obs negationen på Depth_interval.
    final_result = final_result.loc[
        ~(
            (final_result["Depth_interval"] != "0-10 m")
            & (final_result["period"] == "sommar")
            & (final_result["variable"] == "TEMP")
        )
    ]

    final_result.to_csv("./result/trenderHAV.txt", **save_kwargs)

    # skapa metadata tabell
    metadata_kolumner = {
        "Programområde": "Kust och hav",
        "Delprogram": "Fria vattenmassan",
        "Datakälla": "https://sharkweb.smhi.se/hamta-data/",
        "Vattenkategori": "Utsjövatten",
    }
    rename_columns = {
        "REG_ID": "Provplats ID (Stationsregistret)",
        "STATN": "Provplats",
    }

    metadata = (
        final_result[["STATN", "REG_ID", "Depth_interval"]]
        .drop_duplicates()
        .assign(**metadata_kolumner)
        .rename(columns=rename_columns)
    )
    metadata.to_csv("./result/trenderHAV_metadata.txt", **save_kwargs)

    # Skapa mätvariabler tabell
    metada_variabler = (
        final_result[["Mätvariabel", "variable", "period"]]
        .drop_duplicates()
        .rename(columns={"Mätvariabel": "Mätvariabel-djup-period"})
    )
    variable_lookup = {
        "Enhet": {"Default": "µmol/l", "TEMP": "grader", "SALT": "psu", "DOXY": "ml/l"},
        "Mätvariabelgrupp": {
            "Default": "Näringsämnen",
            "TEMP": "Vattnets egenskaper",
            "SALT": "Vattnets egenskaper",
        },
        "Mätvariabel": {
            "NTOT": "Totalkväve",
            "PTOT": "Totalfosfor",
            "SIO3-SI": "Silikat",
            "PHOS": "Fosfat",
            "NTRZ": "Nitrit + Nitrat",
            "TEMP": "Temperatur",
            "SALT": "Salthalt",
            "DOXY": "Löst syre",
        },
    }

    # Lägg till kolumner baserat på variable_metadata
    metada_variabler["Enhet"] = metada_variabler["variable"].map(
        lambda x: variable_lookup["Enhet"].get(x, variable_lookup["Enhet"]["Default"])
    )
    metada_variabler["Mätvariabelgrupp"] = metada_variabler["variable"].map(
        lambda x: variable_lookup["Mätvariabelgrupp"].get(
            x, variable_lookup["Mätvariabelgrupp"]["Default"]
        )
    )
    metada_variabler["Mätvariabel"] = metada_variabler["variable"].map(
        variable_lookup["Mätvariabel"]
    )

    # Byt namn på kolumner
    rename_columns = {"period": "Beskrivning", "variable": "Mätvariabel (Orig)"}
    metada_variabler = metada_variabler.rename(columns=rename_columns)

    # Skapa och spara mätvariabler tabell
    metada_variabler.to_csv("./result/trenderHAV_mätvariabler.txt", **save_kwargs)

    """
    metadatainnehåll:
    Provplats ID (Stationsregistret): REGID
    Programområde: Kust och hav
    Vattenkategori: WATER_CATEGORY
    Delprogram: Fria vattenmassan
    Datakälla: https://sharkweb.smhi.se/hamta-data/
    """

    """
    mätvariabler:
    Mätvariabel: Totalkväve
    Mätvariabel (Orig): NTOT
    Enhet: umol/l
    Mätvariabelgrupp: Näringsämnen
    Beskrivning: helår, sommar, vinter
    """
