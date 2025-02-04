import pathlib
import requests
import json
import uuid


class Datasource:
    """Datasource base class, used for fetching and filtering data

    Note: the data files are expected to be tab seperated fields with
    first row holding the headers.

    Data is downloaded from sharkweb with the short headers

    """

    def __init__(
        self,
        result_directory,
        datatype: str,
        year_interval: list,
        stations: list = [],
        basins: list = [],
        bounds: list = [],
        no_download: bool = False,
        name: str = "",
    ):
        self.no_download = no_download
        self.result_directory = result_directory
        print(name)
        self._download(
            datatype, year_interval, stations, basins=basins, bounds=bounds, name=name
        )

    def _download(
        self,
        datatype: str,
        year_interval: list,
        stations: list = [],
        basins: list = [],
        bounds: list = [],
        name: str = "",
    ):
        if not datatype:
            raise AssertionError("ERROR: no download, missing datatype")

        self.filepath = self._get_filepath(name, datatype, year_interval)

        if self.no_download:
            if not self.filepath.exists():
                raise FileNotFoundError(
                    f"{self.filepath} does not exist, choose download"
                )
            return None

        tableview = self._get_tableView(datatype)
        print(tableview)

        payload = {
            "params": {
                "headerLang": "short",
                "encoding": "utf-8",
                "delimiters": "point-tab",
                "tableView": tableview,
            },
            "query": {
                "fromYear": year_interval[0],
                "toYear": year_interval[1],
                "dataTypes": [datatype],
                "projects": ["NAT Nationell miljöövervakning"],
                "bounds": bounds,
                "stationName": stations,
                "seaBasins": basins,
            },
            "downloadId": str(uuid.uuid4()),
        }

        api_server = "https://sharkweb.smhi.se"
        headers = {
            "Content-Type": "application/json",
            "Accept": "text/plain",
        }
        print(payload)
        # self.logger.info("Requesting download of samples for year '%s' id '%s'" % (year, payload['downloadId']))
        with requests.post(
            "%s/api/sample/download" % api_server,
            data=json.dumps(payload),
            headers=headers,
        ) as response:
            response.raise_for_status()

            data_location = response.headers["location"]
            # self.logger.info("Downloading data from location '%s' into filename '%s'" % (data_location , filename))
            with requests.get(
                "%s%s" % (api_server, data_location), stream=True
            ) as data_response:
                data_response.raise_for_status()
                chunk_size = 1024 * 1024
                with open(self.filepath, "wb") as data_file:
                    for chunk in data_response.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        data_file.write(chunk)

    def _get_tableView(self, datatype):
        if datatype == "Physical and Chemical":
            return "sample_col_physicalchemical_columnparams"
        if datatype == "Chlorophyll":
            return "sample_col_chlorophyll"

        return ""

    def _get_filepath(self, name, datatype, year_interval):
        return pathlib.Path(
            self.result_directory,
            f"temp\sharkweb_{name}_{datatype}_{year_interval[0]}-{year_interval[1]}.csv",
        )


{
    "params": {
        "headerLang": "short",
        "encoding": "utf-8",
        "delimiters": "point-tab",
        "tableView": "sample_col_physicalchemical_columnparams",
    },
    "query": {
        "fromYear": 1960,
        "toYear": 2024,
        "dataTypes": "Physical and Chemical",
        "projects": ["NAT Nationell miljöövervakning"],
        "stationName": ["Å17"],
    },
    "downloadId": "e5d62ef8-dd9a-4a87-a10c-bddf3edf123e",
}

{
    "params": {
        "headerLang": "short",
        "encoding": "utf-8",
        "delimiters": "point-tab",
        "tableView": "sample_col_chlorophyll",
    },
    "query": {
        "fromYear": 1960,
        "toYear": 2024,
        "dataTypes": "Chlorophyll",
        "projects": ["NAT Nationell miljöövervakning"],
        "stationName": ["Å17"],
    },
    "downloadId": "ea31bdf8-5028-42ea-b47f-7236154faed7",
}

if __name__ == "__main__":
    Datasource(
        result_directory="..\data",
        datatype="Physical and Chemical",
        year_interval=[1991, 2020],
        name="all_data_1991-2020",
    )
