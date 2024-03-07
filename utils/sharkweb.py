

import pathlib
import requests
import json
import uuid

class Datasource():
    """Datasource base class, used for fetching and filtering data

    Note: the data files are expected to be tab seperated fields with
    first row holding the headers.

    Data is downloaded from sharkweb with the short headers

    """
    def __init__(self, result_directory, datatype: str, year_interval: list, stations: list, 
                 no_download: bool = False):
        
        self.no_download = no_download
        self.result_directory = result_directory
        self._download(datatype, year_interval, stations)

    def _download(self, datatype, year_interval, stations):

        if not datatype:
            raise AssertionError("ERROR: no download, missing datatype")

        self.filename = self._get_filepath(datatype, year_interval)

        if self.no_download:
            if not self.filename.exists():
                raise FileNotFoundError(f"{self.filename} does not exist, choose download")
            return None

        tableview = self._get_tableView(datatype)
        print(tableview)

        payload = {

            'params': {
                'headerLang': 'short',
                'encoding': 'utf-8',
                'delimiters': 'point-tab',
                'tableView': tableview
            },

            'query': {
                'fromYear': year_interval[0],
                'toYear': year_interval[1],
                'dataTypes': [datatype],
                'projects': ["NAT Nationell miljöövervakning"],
                # 'bounds': [[10.4, 58.2], [10.6, 58.3]],
                'stationName': stations
            },

            'downloadId': str(uuid.uuid4()),
        }

        api_server = 'https://sharkweb.smhi.se'
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'text/plain',
        }
        print(payload)
        # self.logger.info("Requesting download of samples for year '%s' id '%s'" % (year, payload['downloadId']))
        with requests.post('%s/api/sample/download' % api_server,
                            data=json.dumps(payload), headers=headers) as response:

            response.raise_for_status()

            data_location = response.headers['location']
            # self.logger.info("Downloading data from location '%s' into filename '%s'" % (data_location , filename))
            with requests.get('%s%s' % (api_server, data_location), stream=True) as data_response:
                data_response.raise_for_status()
                chunk_size = 1024*1024
                with open(self.filename, 'wb') as data_file:
                    for chunk in data_response.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        data_file.write(chunk)
        print(self.filename)

    def _get_tableView(self, datatype):

        if datatype == "Physical and Chemical":
            return 'sample_col_physicalchemical_columnparams'
        if datatype == "Chlorophyll":
            return "sample_col_chlorophyll"
        
        return ""
    
    def _get_filepath(self, datatype, year_interval):

        return pathlib.Path(self.result_directory, f'temp\sharkweb_{datatype}_{year_interval[0]}-{year_interval[1]}.txt')


{
    'params': 
        {
            'headerLang': 'short', 'encoding': 'utf-8', 'delimiters': 'point-tab', 'tableView': 'sample_col_physicalchemical_columnparams'
            }, 
    'query': 
        {
            'fromYear': 1960, 'toYear': 2024, 'dataTypes': 'Physical and Chemical', 'projects': ['NAT Nationell miljöövervakning'], 'stationName': ['Å17']}, 
    'downloadId': 'e5d62ef8-dd9a-4a87-a10c-bddf3edf123e'}

{
    'params': 
        {
            'headerLang': 'short', 'encoding': 'utf-8', 'delimiters': 'point-tab', 'tableView': 'sample_col_chlorophyll'
        }, 
    'query': 
        {
        'fromYear': 1960, 'toYear': 2024, 'dataTypes': 'Chlorophyll', 'projects': ['NAT Nationell miljöövervakning'], 'stationName': ['Å17']
        }, 
        'downloadId': 'ea31bdf8-5028-42ea-b47f-7236154faed7'
}

if __name__ == "__main__":

    Datasource(result_directory = '..\data')._download(datatypes = ['Physical and Chemical'], year_interval = [1960,2024], stations=["Å17",])