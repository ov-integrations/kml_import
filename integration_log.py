from enum import Enum
from requests.auth import HTTPBasicAuth
import json
import requests


class IntegrationLog(object):

    def __init__(self, process_id, url_onevizion, login_onevizion, pass_onevizion):
        self.url_onevizion = url_onevizion
        self.processId = process_id
        self.auth_onevizion = HTTPBasicAuth(login_onevizion, pass_onevizion)

    def add_log(self, log_level, message, description=''):
        parameters = {'message': message, 'description': description, 'log_level_name': log_level}
        json_data = json.dumps(parameters)
        url_log = f'{self.url_onevizion}/api/v3/integrations/runs/{str(self.processId)}/logs'
        requests.post(url_log, data=json_data, headers={'content-type': 'application/json'}, auth=self.auth_onevizion)


class LogLevel(Enum):
    INFO = (0, 'Info')
    WARNING = (1, 'Warning')
    ERROR = (2, 'Error')
    DEBUG = (3, 'Debug')

    def __init__(self, log_level_id, log_level_name):
        self.log_level_id = log_level_id
        self.log_level_name = log_level_name
