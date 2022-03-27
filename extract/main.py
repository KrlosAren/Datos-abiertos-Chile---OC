
import argparse
import logging
import os
from datetime import datetime

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExtractDatosAbiertos:

    def __init__(self, url: str, desde: str, hasta: str = None):
        self._url = url
        self._periodos = []
        self._today = datetime.today()
        self._desde = desde
        self._hasta = hasta

    def _get_datetime(self, date: str):
        return datetime.strptime(date, '%Y-%m')

    def _get_periodos(self):
        self._desde = self._get_datetime(self._desde)
        self._hasta = self._get_datetime(self._hasta)

        self._periodos = pd.date_range(
            self._desde, self._hasta, freq='MS').strftime("%Y-%-m").tolist()

        logger.info(f'Periodos a consultar: {self._periodos}')

    def _download_file(self, url, file_name):
        self._filenaname = file_name
        try:
            logger.info(f'Descargando archivo {file_name}')
            _response = requests.get(url)
        except requests.exceptions.RequestException as e:
            logger.info('Error downloading file')
            logger.info(e)
        else:
            logger.info('File downloaded successfully')
            with open(f'{os.path.normpath(os.getcwd() + os.sep + os.pardir)}/data/{self._filenaname}.zip', 'wb') as f:
                f.write(_response.content)
                logger.info('File downloaded successfully')

    def _get_files(self):
        for periodo in self._periodos:
            logger.info(f'Consultando datos para el periodo {periodo}')
            _url = f'{self._url}/{periodo}.zip'
            self._download_file(_url, periodo)

    def _run(self):
        logger.info('Iniciando extracción de datos')
        self._get_periodos()
        self._get_files()


def main(url, desde, hasta):
    _extract = ExtractDatosAbiertos(url, desde, hasta)
    _extract._run()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'url', help='Ingrese url a consultar de datos abiertos', type=str)
    parser.add_argument(
        'desde', help='Ingrese fecha inicial a consultar YYYY-M', type=str)
    parser.add_argument(
        'hasta', help='Ingrese fecha final a consultar YYYY-M', type=str)

    args = parser.parse_args()
    main(args.url, args.desde, args.hasta)
