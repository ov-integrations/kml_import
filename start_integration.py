from jsonschema import validate
from kmz_to_kml import Integration, File, Import
from integration_log import IntegrationLog
import json


with open('settings.json', 'rb') as PFile:
    password_data = json.loads(PFile.read().decode('utf-8'))

with open('settings_schema.json', 'rb') as PFile:
    data_schema = json.loads(PFile.read().decode('utf-8'))

try:
    validate(password_data, data_schema)
except Exception as e:
    raise Exception(f'Incorrect value in the settings file\n{str(e)}')

url_kmz = password_data['urlKMZ']
url_onevizion = password_data['urlOneVizion']
login_onevizion = password_data['loginOneVizion']
pass_onevizion = password_data['passOneVizion']
import_name = password_data['importName']

with open('ihub_parameters.json', "rb") as PFile:
    ihub_data = json.loads(PFile.read().decode('utf-8'))

process_id = ihub_data['processId']

integration_log = IntegrationLog(process_id, url_onevizion, login_onevizion, pass_onevizion)
file = File(url_kmz, integration_log)
integration_import = Import(url_onevizion, login_onevizion, pass_onevizion, import_name, integration_log)
integration = Integration(file, integration_import, integration_log)

integration.start_integration()
