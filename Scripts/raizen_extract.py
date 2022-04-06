###########################################################
# OBJETIVO:  Construção da Classe para Download do XLS
# DATA: 06Abr2022
# OWNER: Silvio Martins
###########################################################

#Importanndo Bibliotecas
import requests
import os

#Class Base_extract
class Base_extraxt:
    def __init__(self):
        self._url = None
        self._file_name = None

    def set_url(self, url):
        self._url = url

    def get_url(self):
        return self._url

    def set_file_name(self, file_name):
        self._file_name = file_name

    def get_file_name(self):
        return self._file_name

    def download_file(self):
        arquivo = requests.get(self._url, allow_redirects=True)
        open(self._file_name, 'wb').write(arquivo.content)

#Função para Download do arquivo xls
def extract_xls_origin(path_bronze_zone, file_dest_name):
    receiver = Base_extraxt()
    receiver.set_url('https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls')
    receiver.set_file_name(file_dest_name)
    receiver.download_file()