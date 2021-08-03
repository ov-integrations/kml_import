# kml_import

Downloads KMZ file from the specified location, extracts data to CSV file and loads it with the specified OneVizion import.

Requirements
- python 3
- python [OneVizion](https://github.com/Onevizion/API-v3) library (pip install onevizion)
- python [Requests](https://docs.python-requests.org/en/master/) library (pip install requests)
- python [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/#installing-beautiful-soup) library (pip install beautifulsoup4)
- python [lxml](https://www.crummy.com/software/BeautifulSoup/bs4/doc/#installing-a-parser) library (pip install lxml)
- python [jsonschema](https://python-jsonschema.readthedocs.io/en/stable/) library (pip install jsonschema)

Example of settings.json
```json
{
    "urlKMZ": "https://fsapps.nwcg.gov/data/kml/conus_lg_incidents.kmz",
    "urlOneVizion": "https://test.onevizion.com/",
    "ovAccessKey": "******",
    "ovSecretKey": "******",
    "importName": "Fires Default Data Import",
    "importAction": "INSERT"
}
```