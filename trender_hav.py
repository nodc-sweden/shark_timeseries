from utils import calculateparameter as calculateparameter
from utils.sharkweb import Datasource
from utils.format_raw_data import FormatData
from utils.means import Means
import pathlib
import pandas as pd

def get_data(
    datatype: str, year_interval: list, stations: list, no_download: bool, name: str
):
    print(f"no download is {no_download}")
    datasource = Datasource(
        pathlib.Path("..\data"),
        datatype=datatype,
        year_interval=year_interval,
        stations=stations,
        no_download=no_download,
        name=name,
    )
    return FormatData(datasource.result_directory).load_raw_data(
        file_name=datasource.filename
    )


if __name__ == "__main__":
    download_specs = {
        "Gulf_of_Bothnia": {
            "datatype": "Physical and Chemical",
            "year_interval": [2000, 2023],
            "stations": ["MS4 / C14", "F9 / A13"],
            "no_download": True,
            "name": "GoF",
        },
        "Skagerrak_Kattegat": {
            "datatype": "Physical and Chemical",
            "year_interval": [2000, 2023],
            "stations": ["Å17", "Å13", "FLADEN", "ANHOLT E"],
            "no_download": True,
            "name": "WC",
        },
        "Baltic_Proper": {
            "datatype": "Physical and Chemical",
            "year_interval": [2000, 2023],
            "stations": [
                "BY2 ARKONA",
                "BY5 BORNHOLMSDJ",
                "BY15 GOTLANDSDJ",
                "BY31 LANDSORTSDJ",
                "BY32 NORRKÖPINGSDJ",
            ],
            "no_download": True,
            "name": "BP",
        },
    }
    data_list = []
    for key, value in download_specs.items():
        data = get_data(
            datatype=value["datatype"],
            year_interval=value["year_interval"],
            stations=value["stations"],
            no_download=value["no_download"],
            name=value["name"],
        )
        data_list.append(data)
    data = pd.concat(data_list)

    mean = Means(data = data, result_directory=pathlib.Path('..\data'))
    mean.calculate_depth_interval_mean(save_path=pathlib.Path("..\data\mean"), interval = [[0, 10], 'BW'], BW_type = 'STATN')
    
    # save default keyword args
    save_kwargs = {"sep": "\t", "encoding": "utf-8", "index": False, "float_format": "%.2f"}

    # Total nutrients full year
    # for parameter in ["PTOT", "NTOT"]:
    variables = ["PTOT", "NTOT", "SALT"]
    # FormatData(pathlib.Path('..\data')).filter_by_parameter(mean.df_depth_mean, parameters=variables).to_csv(pathlib.Path(f"..\data/{'_'.join(variables)}_visit.txt"), **save_kwargs)
    # month_mean = mean.calculate_month_mean(mean.df_depth_mean, variables)
    # month_mean.to_csv(pathlib.Path(f"..\data/{'_'.join(variables)}_monthly.txt"), **save_kwargs)
    year_mean = mean.calculate_year_mean(mean.df_depth_mean, variables, first_month_mean=False)
    year_mean.to_csv(pathlib.Path(f"..\data/{'_'.join(variables)}_yearly.txt"), **save_kwargs)

    # Oxygen data only autumn
    variables = ["O2_H2S"]
    
    season_mean = mean.calculate_season_mean(mean.df_depth_mean, variables, months=[7,8,9,10], first_month_mean=False)
    season_mean.to_csv(pathlib.Path(f"..\data/{'_'.join(variables)}_autumn_yearly.txt"), **save_kwargs)

    # Temperature data only summer
    variables = ["TEMP"]
    data = mean.df_depth_mean[mean.df_depth_mean['Depth_interval'] == "0-10 m"]
    summer_mean = mean.calculate_season_mean(data, variables, months=[7,8], first_month_mean=False)
    summer_mean.to_csv(pathlib.Path(f"..\data/{'_'.join(variables)}_summer_yearly.txt"), **save_kwargs)
    
    variables = ["TEMP", "NTRZ", "PHOS", "SIO3-SI"]
    winter_mean = mean.calculate_season_mean(data, variables, months=[1,2,12], first_month_mean=False)
    winter_mean.to_csv(pathlib.Path(f"..\data/{'_'.join(variables)}_winter_yearly.txt"), **save_kwargs)

    final_result = pd.concat([year_mean, summer_mean, winter_mean])
    final_result.to_csv('trenderHAV.txt', **save_kwargs)
    
    # skapa metadata tabell
    metadata_kolumner = {
    'Programområde': 'Kust och hav',
    'Delprogram': 'Fria vattenmassan',
    'Datakälla': 'https://sharkweb.smhi.se/hamta-data/',
    'Vattenkategori': 'Utsjövatten'
    }
    rename_columns = {'REG_ID': 'Provplats ID (Stationsregistret)', "STATN": "Provplats"}

    metadata = (final_result[['STATN', 'REG_ID', 'Depth_interval']].drop_duplicates().assign(**metadata_kolumner).rename(columns=rename_columns))
    metadata.to_csv('trenderHAV_metadata.txt', **save_kwargs)

    # Skapa mätvariabler tabell
    metada_variabler = final_result[['Mätvariabel', 'variable', 'period']].drop_duplicates().rename(columns={"Mätvariabel": "Mätvariabel-djup-period"})
    variable_lookup = {
        'Enhet': {"Default": 'µmol/l', "TEMP": "grader", "SALT": "psu", "DOXY": "ml/l"},
        "Mätvariabelgrupp": {"Default": "Näringsämnen", "TEMP": "Vattnets egenskaper", "SALT": "Vattnets egenskaper"},
        "Mätvariabel": {"NTOT": "Totalkväve", "PTOT": "Totalfosfor", 
                        "SIO3-SI": "Silikat", "PHOS": 'Fosfat', "NTRZ": "Nitrit + Nitrat",
                        "TEMP": "Temperatur", "SALT": "Salthalt", "DOXY": "Löst syre"}
    }

    # Lägg till kolumner baserat på variable_metadata
    metada_variabler['Enhet'] = metada_variabler['variable'].map(lambda x: variable_lookup['Enhet'].get(x, variable_lookup['Enhet']['Default']))
    metada_variabler['Mätvariabelgrupp'] = metada_variabler['variable'].map(lambda x: variable_lookup['Mätvariabelgrupp'].get(x, variable_lookup['Mätvariabelgrupp']['Default']))
    metada_variabler['Mätvariabel'] = metada_variabler['variable'].map(variable_lookup['Mätvariabel'])

    # Byt namn på kolumner
    rename_columns = {'period': "Beskrivning", "variable": "Mätvariabel (Orig)"}
    metada_variabler = metada_variabler.rename(columns=rename_columns)

    # Skapa och spara mätvariabler tabell    
    metada_variabler.to_csv('trenderHAV_mätvariabler.txt', **save_kwargs)

    
    
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


