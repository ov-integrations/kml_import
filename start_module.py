import sys
import json
import subprocess


subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'python_dependencies.txt'])
with open('settings.json', 'rb') as PFile:
    settings_data = json.loads(PFile.read().decode('utf-8'))

subprocess.run('python3 module_settings.py', shell=True)
