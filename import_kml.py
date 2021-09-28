from onevizion import LogLevel, OVImport
from bs4 import BeautifulSoup
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
        self.integration_log.add(LogLevel.INFO, 'Starting Integration')

        try:
            self.kml.download_kmz_file(Integration.KMZ_FILENAME)
            self.kml.extract(Integration.KMZ_FILENAME, Integration.KML_FILENAME)
            parse_list = self.csv.parse(Integration.KML_FILENAME)
            self.csv.create(parse_list, Integration.CSV_FILENAME)
            self.integration_import.import_process(Integration.CSV_FILENAME)
        finally:
            self.delete_files()

        self.integration_log.add(LogLevel.INFO, 'Integration has been completed')

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
            self.integration_log.add(LogLevel.INFO, 'KMZ file downloaded')
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

        kml.extract(kml_filename_to_extract)
        os.rename(kml_filename_to_extract, kml_filename)
        self.integration_log.add(LogLevel.INFO, 'KML file extracted')


class CSV:

    def __init__(self, integration_log):
        self.integration_log = integration_log

    def parse(self, kml_filename):
        parse_list = []
        with open(kml_filename, 'r') as kml:
            parse = BeautifulSoup(kml, 'lxml-xml')

        for placemark in parse.find_all('Placemark'):
            name = placemark.find('name')
            if name is None:
                raise Exception(f'Failed parse KML. Exception [Field name not found]')

            description = placemark.find('description')
            if description is None:
                raise Exception(f'Failed parse KML. Exception [Field description not found]')

            parse_list.append({CSVHeader.NAME.value:name.text, CSVHeader.DESCRIPTION.value:description.text.strip()})

        self.integration_log.add(LogLevel.INFO, 'KML file parsed')
        return parse_list

    def create(self, parse_list, csv_filename):
        with open(csv_filename, 'w') as csv_file:
            field_names = [CSVHeader.NAME.value, CSVHeader.DESCRIPTION.value]
            writer = csv.DictWriter(csv_file, field_names)
            writer.writeheader()
            writer.writerows(parse_list)

        self.integration_log.add(LogLevel.INFO, 'CSV file created')


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

            self.integration_log.add(LogLevel.INFO, f'Import \"{self.import_name}\" founded')
        else:
            raise Exception(f'Failed to receive import. Exception [{str(response.text)}]')

        return import_id

    def start_import(self, import_id, file_name):
        response = OVImport(self.url_onevizion_without_protocol, self.access_key, self.secret_key, import_id, file_name, self.import_action, isTokenAuth=True)
        if response.request.ok:
            self.integration_log.add(LogLevel.INFO, f'Import \"{self.import_name}\" started')
        else:
            raise Exception(f'Failed to start import. Exception [{str(response.request.text)}]')

class CSVHeader(Enum):
    NAME = 'Name'
    DESCRIPTION = 'Description'
