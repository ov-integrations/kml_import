from onevizion import LogLevel, OVImport
from bs4 import BeautifulSoup
from datetime import datetime
from enum import Enum
import requests
import zipfile
import csv
import re
import os


class Integration:
    KMZ_FILENAME = 'file.kmz'
    KML_FILENAME = 'file.kml'
    CSV_FILENAME = 'file.csv'

    def __init__(self, kml_file, csv_file, integration_import, integration_log):
        self.kml = kml_file
        self.csv = csv_file
        self.integration_import = integration_import
        self.integration_log = integration_log

    def start_integration(self):
        self.integration_log.add_log(LogLevel.INFO, 'Starting Integration')

        try:
            self.kml.download_kmz_file(Integration.KMZ_FILENAME)
            self.kml.extract(Integration.KMZ_FILENAME, Integration.KML_FILENAME)
            parse_list = self.csv.parse(Integration.KML_FILENAME)
            self.csv.create(parse_list, Integration.CSV_FILENAME)
            self.integration_import.import_process(Integration.CSV_FILENAME)
        finally:
            self.delete_files()

        self.integration_log.add_log(LogLevel.INFO, 'Integration has been completed')

    def delete_files(self):
        file_list = [f for f in os.listdir() if f.endswith(('.kmz', '.kml', '.csv'))]
        for f in file_list:
            os.remove(os.path.join(f))


class KML:

    def __init__(self, url, integration_log):
        self.url = url
        self.integration_log = integration_log

    def download_kmz_file(self, kmz_filename):
        kmz_content = self.download()
        self.save(kmz_content, kmz_filename)

    def download(self):
        url = self.url
        response = requests.get(url)
        if response.ok:
            self.integration_log.add_log(LogLevel.INFO, 'KMZ file downloaded')
            return response.content
        else:
            raise Exception(f'Failed download KMZ. Exception [{response.text}]')

    def save(self, kmz_content, kmz_filename):
        with open(kmz_filename, 'wb') as file:
            file.write(kmz_content)

    def extract(self, kmz_filename, kml_filename):
        kml = zipfile.ZipFile(kmz_filename, 'r')
        len_kml = len(kml.filelist)
        if len_kml == 0:
            raise Exception(f'Failed extract KML file. Exception [KMZ file does not store data]')

        kml_filename_to_extract = None
        for file in kml.filelist:
            file_name = file.filename
            if re.search('.kml$', file_name) is None:
                continue
            else:
                if kml_filename_to_extract is None:
                    kml_filename_to_extract = file_name
                else:
                    raise Exception(f'Failed extract KML file. Exception [KMZ file stores more than one KML file]')

        if kml_filename_to_extract is None:
            raise Exception(f'Failed extract KML file. Exception [KML file not found in KMZ]')
        else:
            kml.extract(kml_filename_to_extract)
            os.rename(kml_filename_to_extract, kml_filename)
            self.integration_log.add_log(LogLevel.INFO, 'KML file extracted')


class CSV:

    def __init__(self, integration_log):
        self.integration_log = integration_log

    def parse(self, kml_filename):
        parse_list = []
        with open(kml_filename, 'r') as kml:
            parse = BeautifulSoup(kml, 'lxml-xml')

        description_with_date = parse.find('description')
        if description_with_date is None:
            raise Exception(f'Failed parse KML. Exception [Description for registration_id not found]')

        date = re.search('\d+-\w+-\d+', description_with_date.text)
        if date is None:
            raise Exception(f'Failed parse KML. Exception [Date for registration_id not found]')
        else:
            registration_id = f'Fires-{datetime.strptime(date.group(), "%d-%b-%Y").strftime("%m/%d/%y")}'

        for placemark in parse.find_all('Placemark'):
            name = placemark.find('name')
            if name is None:
                raise Exception(f'Failed parse KML. Exception [Field name not found]')

            name = name.text

            description = placemark.find('description')
            if description is None:
                raise Exception(f'Failed parse KML. Exception [Field description not found]')

            description = description.text.strip()

            for split in re.split('<br/>', description):
                if re.search('Fire Type', split) is None:
                    continue
                else:
                    fire_type = re.split('</b>', split)[1].strip()
                    break

            if fire_type is None:
                raise Exception(f'Failed parse KML. Exception [Field fire_type not found]')

            fire_size_acres = re.search(r'\d+\sacres|\d+acres', description)
            if fire_size_acres is None:
                raise Exception(f'Failed parse KML. Exception [Field fire_size not found]')

            fire_size = re.search(r'\d+', fire_size_acres.group())
            if fire_size is None:
                raise Exception(f'Failed parse KML. Exception [Number in fire_size field is not found]')

            fire_size = fire_size.group()

            coordinates = placemark.find('Point').find('coordinates')
            if coordinates is None:
                raise Exception(f'Failed parse KML. Exception [Field coordinates not found]')

            coordinates = re.split(',', coordinates.text)
            parse_list.append({CSVHeader.REG_ID.value:registration_id, CSVHeader.NAME.value:name, CSVHeader.DESCRIPTION.value:description, \
                                CSVHeader.FIRE_SIZE.value:fire_size, CSVHeader.FIRE_TYPE.value:fire_type, CSVHeader.LATITUDE.value:coordinates[1], \
                                    CSVHeader.LONGITUDE.value:coordinates[0]})

        self.integration_log.add_log(LogLevel.INFO, 'KML file parsed')
        return parse_list

    def create(self, parse_list, csv_filename):
        with open(csv_filename, 'w') as csv_file:
            field_names = [CSVHeader.REG_ID.value, CSVHeader.NAME.value, CSVHeader.DESCRIPTION.value, \
                            CSVHeader.FIRE_SIZE.value, CSVHeader.FIRE_TYPE.value, CSVHeader.LATITUDE.value, \
                                CSVHeader.LONGITUDE.value]
            writer = csv.DictWriter(csv_file, field_names)
            writer.writeheader()
            writer.writerows(parse_list)

        self.integration_log.add_log(LogLevel.INFO, 'CSV file created')


class Import:

    def __init__(self, url_onevizion, url_onevizion_without_protocol, access_key, secret_key, import_name, import_action, integration_log):
        self.url_onevizion = url_onevizion
        self.url_onevizion_without_protocol = url_onevizion_without_protocol
        self.access_key = access_key
        self.secret_key = secret_key
        self.import_name = import_name
        self.import_action = import_action
        self.integration_log = integration_log

    def import_process(self, csv_filename):
        import_id = self.get_import()
        self.start_import(import_id, csv_filename)

    def get_import(self):
        import_id = None
        url = f'{self.url_onevizion}/api/v3/imports'
        response = requests.get(url, headers={'Content-type':'application/json', 'Content-Encoding':'utf-8', 'Authorization':f'Bearer {self.access_key}:{self.secret_key}'})
        if response.ok:
            for import_data in response.json():
                import_name = import_data['name']
                if import_name == self.import_name:
                    import_id = import_data['id']
                    break

            if import_id is None:
                raise Exception(f'Import \"{self.import_name}\" not found')
            else:
                self.integration_log.add_log(LogLevel.INFO, f'Import \"{self.import_name}\" founded')
        else:
            raise Exception(f'Failed to receive import. Exception [{str(response.text)}]')

        return import_id

    def start_import(self, import_id, file_name):
        OVImport(self.url_onevizion_without_protocol, self.access_key, self.secret_key, import_id, file_name, self.import_action, isTokenAuth=True)


class CSVHeader(Enum):
    REG_ID = 'RegistrationID'
    NAME = 'Name'
    DESCRIPTION = 'Description'
    FIRE_SIZE = 'FireSize'
    FIRE_TYPE = 'FireType'
    LATITUDE = 'Latitude'
    LONGITUDE = 'Longitude'
