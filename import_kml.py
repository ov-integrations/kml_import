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
    KMZ = 'fires.kmz'
    KML = 'fires.kml'
    CSV = 'fires.csv'

    def __init__(self, kml_file, csv_file, integration_import, integration_log):
        self.kml = kml_file
        self.csv = csv_file
        self.integration_import = integration_import
        self.integration_log = integration_log

    def start_integration(self):
        self.integration_log.add_log(LogLevel.INFO, 'Starting Integration')

        try:
            self.kml.get(Integration.KMZ, Integration.KML)
            parse_list = self.csv.parse(Integration.KML)
            self.csv.create(parse_list, Integration.CSV)
            self.integration_import.import_process(Integration.CSV)
        except Exception as e:
            raise Exception(e)
        finally:
            self.delete_files()

        self.integration_log.add_log(LogLevel.INFO, 'Integration has been completed')

    def delete_files(self):
        file_list = [f for f in os.listdir() if f.endswith(('.kmz', '.kml', '.csv'))]
        for f in file_list:
            os.remove(os.path.join(f))


class KML:

    def __init__(self, url_fires, integration_log):
        self.url_fires = url_fires
        self.integration_log = integration_log

    def get(self, kmz_file, kml_file):
        kmz_content = self.download()
        self.save(kmz_content, kmz_file)
        self.extract(kmz_file, kml_file)

    def download(self):
        url = self.url_fires
        answer = requests.get(url)
        if answer.ok:
            self.integration_log.add_log(LogLevel.INFO, 'KMZ file downloaded')
            return answer.content
        else:
            self.integration_log.add_log(LogLevel.ERROR, f'Failed download kmz. Exception [{answer.text}]')
            raise Exception(f'Failed download kmz. Exception [{answer.text}]')

    def save(self, kmz_content, kmz_file):
        with open(kmz_file, 'wb') as file:
            file.write(kmz_content)

    def extract(self, kmz_file, kml_file):
        kml = zipfile.ZipFile(kmz_file, 'r')
        len_kml = len(kml.filelist)
        if len_kml == 0:
            self.integration_log.add_log(LogLevel.ERROR, f'Failed extract kml file. Exception [kmz file does not store data]')
            raise Exception(f'Failed extract kml file. Exception [kmz file does not store data]')

        kml_filename = None
        for file in kml.filelist:
            file_name = file.filename
            if re.search('.kml$', file_name) is None:
                continue
            else:
                if kml_filename is None:
                    kml_filename = file_name
                else:
                    self.integration_log.add_log(LogLevel.ERROR, f'Failed extract kml file. Exception [kmz file stores more than one kml file]')
                    raise Exception(f'Failed extract kml file. Exception [kmz file stores more than one kml file]')

        if kml_filename is None:
            self.integration_log.add_log(LogLevel.ERROR, f'Failed extract kml file. Exception [KML file not found in KMZ]')
            raise Exception(f'Failed extract kml file. Exception [KML file not found in KMZ]')
        else:
            kml.extract(kml_filename)
            os.rename(kml_filename, kml_file)
            self.integration_log.add_log(LogLevel.INFO, 'KML file extracted')


class CSV:

    def __init__(self, integration_log):
        self.integration_log = integration_log

    def parse(self, kml_file):
        parse_list = []
        with open(kml_file, 'r') as kml:
            parse = BeautifulSoup(kml, 'lxml-xml')

        description_with_date = parse.find('description')
        if description_with_date is None:
            self.integration_log.add_log(LogLevel.ERROR, f'Failed parse kml. Exception [Description for registration_id not found]')
            raise Exception(f'Failed parse kml. Exception [Description for registration_id not found]')

        date = re.search('\d+-\w+-\d+', description_with_date.text)
        if date is None:
            self.integration_log.add_log(LogLevel.ERROR, f'Failed parse kml. Exception [Date for registration_id not found]')
            raise Exception(f'Failed parse kml. Exception [Date for registration_id not found]')
        else:
            registration_id = f'Fires-{datetime.strptime(date.group(), "%d-%b-%Y").strftime("%m/%d/%y")}'

        for placemark in parse.find_all('Placemark'):
            name = placemark.find('name')
            if name is None:
                self.integration_log.add_log(LogLevel.ERROR, f'Failed parse kml. Exception [Field name not found]')
                raise Exception(f'Failed parse kml. Exception [Field name not found]')

            name = name.text

            description = placemark.find('description')
            if description is None:
                self.integration_log.add_log(LogLevel.ERROR, f'Failed parse kml. Exception [Field description not found]')
                raise Exception(f'Failed parse kml. Exception [Field description not found]')

            description = description.text.strip()

            for split in re.split('<br/>', description):
                if re.search('Fire Type', split) is None:
                    continue
                else:
                    fire_type = re.split('</b>', split)[1].strip()
                    break

            if fire_type is None:
                self.integration_log.add_log(LogLevel.ERROR, f'Failed parse kml. Exception [Field fire_type not found]')
                raise Exception(f'Failed parse kml. Exception [Field fire_type not found]')

            try:
                fire_size = re.search(r'\d+', re.search(r'\d+\sacres|\d+acres', description).group()).group()
            except Exception:
                self.integration_log.add_log(LogLevel.ERROR, f'Failed parse kml. Exception [Field fire_size not found]')
                raise Exception(f'Failed parse kml. Exception [Field fire_size not found]')

            coordinates = placemark.find('Point').find('coordinates')
            if coordinates is None:
                self.integration_log.add_log(LogLevel.ERROR, f'Failed parse kml. Exception [Field coordinates not found]')
                raise Exception(f'Failed parse kml. Exception [Field coordinates not found]')

            coordinates = re.split(',', coordinates.text)
            parse_list.append({CSVHeader.REG_ID.value:registration_id, CSVHeader.NAME.value:name, CSVHeader.DESCRIPTION.value:description, \
                                CSVHeader.FIRE_SIZE.value:fire_size, CSVHeader.FIRE_TYPE.value:fire_type, CSVHeader.LATITUDE.value:coordinates[1], \
                                    CSVHeader.LONGITUDE.value:coordinates[0]})

        self.integration_log.add_log(LogLevel.INFO, 'KML file parsed')
        return parse_list

    def create(self, parse_list, csv_file):
        with open(csv_file, 'w') as csv_file:
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

    def import_process(self, csv_file):
        try:
            import_id = self.get_import()
            self.start_import(import_id, csv_file)
        except Exception as e:
            raise Exception(e)

    def get_import(self):
        import_id = None
        url = f'{self.url_onevizion}/api/v3/imports'
        answer = requests.get(url, headers={'Content-type':'application/json', 'Content-Encoding':'utf-8', 'Authorization':f'Bearer {self.access_key}:{self.secret_key}'})
        if answer.ok:
            for import_data in answer.json():
                import_name = import_data['name']
                if import_name == self.import_name:
                    import_id = import_data['id']
                    break

            if import_id is None:
                self.integration_log.add_log(LogLevel.ERROR, f'Import \"{self.import_name}\" not found')
                raise Exception(f'Import \"{self.import_name}\" not found')
            else:
                self.integration_log.add_log(LogLevel.INFO, f'Import \"{self.import_name}\" founded')
        else:
            self.integration_log.add_log(LogLevel.ERROR, f'Failed to receive import. Exception [{str(answer.text)}]')
            raise Exception(f'Failed to receive import. Exception [{str(answer.text)}]')

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

