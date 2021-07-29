from enum import Enum
from bs4 import BeautifulSoup
from requests.auth import HTTPBasicAuth
from integration_log import LogLevel
import re
import requests
import csv
import os
import zipfile
import datetime


class Integration:

    def __init__(self, file, integration_import, integration_log):
        self.file = file
        self.integration_import = integration_import
        self.integration_log = integration_log

    def start_integration(self):
        self.integration_log.add_log(LogLevel.INFO.log_level_name, 'Starting Integration')

        try:
            self.file.get_csv()
        except Exception as e:
            self.delete_files()
            raise Exception(e)
        
        try:
            self.integration_import.import_process()
        except Exception as e:
            self.file.delete_files()
            raise Exception(e)
        
        self.file.delete_files()
        self.integration_log.add_log(LogLevel.INFO.log_level_name, 'Integration has been completed')


class File:

    def __init__(self, url_fires, integration_log):
        self.url_fires = url_fires
        self.integration_log = integration_log

    def get_csv(self):
        kmz_file = self.download_kmz()
        self.save_kmz(kmz_file)
        self.extract_kml(FileNames.KMZ.value)
        parse_list = self.parse_kml()
        self.create_csv(parse_list)

    def download_kmz(self):
        url = self.url_fires
        answer = requests.get(url)
        if answer.ok:
            self.integration_log.add_log(LogLevel.INFO.log_level_name, 'KMZ file downloaded')
            return answer.content
        else:
            self.integration_log.add_log(LogLevel.ERROR.log_level_name, f'Failed download kmz. Exception [{answer.text}]')
            raise Exception(f'Failed download kmz. Exception [{answer.text}]')

    def save_kmz(self, file):
        open(FileNames.KMZ.value, 'wb').write(file)

    def extract_kml(self, file):
        kml = zipfile.ZipFile(file, 'r')
        if len(kml.filelist) == 1:
            kml_filename = kml.filelist[0].filename
            kml.extract(kml_filename)
            os.rename(kml_filename, FileNames.KML.value)
            self.integration_log.add_log(LogLevel.INFO.log_level_name, 'KML file extracted')
        else:
            self.integration_log.add_log(LogLevel.ERROR.log_level_name, f'Failed extract kml file. Exception [kmz file is stored more than one file]')
            raise Exception(f'Failed extract kml file. Exception [kmz file is stored more than one file]')

    def parse_kml(self):
        parse_list = []
        registration_id = self.get_registration()
        with open(FileNames.KML.value, 'r') as kml:
            parse = BeautifulSoup(kml, 'lxml-xml')

        for placemark in parse.find_all('Placemark'):
            name = placemark.find('name')
            if name is None:
                self.integration_log.add_log(LogLevel.ERROR.log_level_name, f'Failed parse kml. Exception [Field name not found]')
                raise Exception(f'Failed parse kml. Exception [Field name not found]')

            name = name.text

            description = placemark.find('description')
            if description is None:
                self.integration_log.add_log(LogLevel.ERROR.log_level_name, f'Failed parse kml. Exception [Field description not found]')
                raise Exception(f'Failed parse kml. Exception [Field description not found]')

            description = description.text.strip()

            for split in re.split('<br/>', description):
                if re.search('Fire Type', split) is None:
                    continue
                else:
                    fire_type = re.split('</b>', split)[1].strip()
                    break

            if fire_type is None:
                self.integration_log.add_log(LogLevel.ERROR.log_level_name, f'Failed parse kml. Exception [Field fire_type not found]')
                raise Exception(f'Failed parse kml. Exception [Field fire_type not found]')

            try:
                fire_size = re.search(r'\d+', re.search(r'\d+\sacres|\d+acres', description).group()).group()
            except Exception:
                self.integration_log.add_log(LogLevel.ERROR.log_level_name, f'Failed parse kml. Exception [Field fire_size not found]')
                raise Exception(f'Failed parse kml. Exception [Field fire_size not found]')

            coordinates = placemark.find('Point').find('coordinates')
            if coordinates is None:
                self.integration_log.add_log(LogLevel.ERROR.log_level_name, f'Failed parse kml. Exception [Field coordinates not found]')
                raise Exception(f'Failed parse kml. Exception [Field coordinates not found]')

            coordinates = re.split(',', coordinates.text)
            parse_list.append({CSVHeader.REG_ID.value:registration_id, CSVHeader.NAME.value:name, CSVHeader.DESCRIPTION.value:description, \
                                CSVHeader.FIRE_SIZE.value:fire_size, CSVHeader.FIRE_TYPE.value:fire_type, CSVHeader.LATITUDE.value:coordinates[1], \
                                    CSVHeader.LONGITUDE.value:coordinates[0]})

        self.integration_log.add_log(LogLevel.INFO.log_level_name, 'KML file parsed')
        return parse_list

    def get_registration(self):
        current_date = datetime.date.today().strftime('%m/%d/%y')
        return f'Fires-{current_date}'

    def create_csv(self, parse_list):
        with open(FileNames.CSV.value, 'w') as csv_file:
            field_names = [CSVHeader.REG_ID.value, CSVHeader.NAME.value, CSVHeader.DESCRIPTION.value, \
                            CSVHeader.FIRE_SIZE.value, CSVHeader.FIRE_TYPE.value, CSVHeader.LATITUDE.value, \
                                CSVHeader.LONGITUDE.value]
            writer = csv.DictWriter(csv_file, field_names)
            writer.writeheader()
            writer.writerows(parse_list)

        self.integration_log.add_log(LogLevel.INFO.log_level_name, 'CSV file created')

    def delete_files(self):
        file_list = [f for f in os.listdir() if f.endswith(('.kmz', '.kml', '.csv'))]
        for f in file_list:
            os.remove(os.path.join(f))


class Import:

    def __init__(self, url_onevizion, login_onevizion, pass_onevizion, import_name, integration_log):
        self.url_onevizion = url_onevizion
        self.auth_onevizion = HTTPBasicAuth(login_onevizion, pass_onevizion)
        self.import_name = import_name
        self.integration_log = integration_log

    def import_process(self):
        import_id = self.get_import()
        self.start_import(import_id, FileNames.CSV.value)

    def get_import(self):
        import_id = None
        url = f'{self.url_onevizion}/api/v3/imports'
        answer = requests.get(url, headers={'Content-type':'application/json', 'Content-Encoding':'utf-8'}, auth=self.auth_onevizion)
        if answer.ok:
            for imports in answer.json():
                import_name = imports['name']
                if import_name == self.import_name:
                    import_id = imports['id']
                    break

            if import_id is None:
                self.integration_log.add_log(LogLevel.ERROR.log_level_name, f'Import \"{self.import_name}\" not found')
                raise Exception(f'Import \"{self.import_name}\" not found')
            else:
                self.integration_log.add_log(LogLevel.INFO.log_level_name, f'Import \"{self.import_name}\" founded')
        else:
            self.integration_log.add_log(LogLevel.ERROR.log_level_name, f'Failed to receive import. Exception [{str(answer.text)}]')
            raise Exception(f'Failed to receive import. Exception [{str(answer.text)}]')

        return import_id

    def start_import(self, import_id, file_name):
        url = f'{self.url_onevizion}/api/v3/imports/{str(import_id)}/run'
        data = {'action':'INSERT'}
        files = {'file': (file_name, open(file_name, 'rb'))}
        answer = requests.post(url, files=files, params=data, headers={'Accept':'application/json'}, auth=self.auth_onevizion)
        if answer.ok:
            self.integration_log.add_log(LogLevel.INFO.log_level_name, f'Import \"{self.import_name}\" started')
        else:
            self.integration_log.add_log(LogLevel.ERROR.log_level_name, f'Failed to start import. Exception [{str(answer.text)}]')
            raise Exception(f'Failed to start import. Exception [{str(answer.text)}]')


class CSVHeader(Enum):
    REG_ID = 'RegistrationID'
    NAME = 'Name'
    DESCRIPTION = 'Description'
    FIRE_SIZE = 'FireSize'
    FIRE_TYPE = 'FireType'
    LATITUDE = 'Latitude'
    LONGITUDE = 'Longitude'


class FileNames(Enum):
    KMZ = 'fires.kmz'
    KML = 'fires.kml'
    CSV = 'fires.csv'
