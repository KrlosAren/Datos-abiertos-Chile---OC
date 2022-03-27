import argparse
import logging
import os
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(url,desde, hasta=None):
    _extract_data(url,desde, hasta)
    _tranform_files('./data')


def _extract_data(url, desde, hasta):
    logger.info('Iniciando extracción de datos')
    logger.info(f'Desde: {desde}')
    logger.info(f'Hasta: {hasta}')
    subprocess.run(['python3', 'main.py', url, desde, hasta], cwd='./extract')
    logger.info('Fin de extracción de datos')

def _tranform_files(path):
    logger.info('Iniciando transformación de datos')
    subprocess.run(['python3', 'main.py', os.getcwd() + '/data'], cwd='./transform')
    logger.info('Fin de transformación de datos')

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        'Extract data from https://datosabiertos.chilecompra.cl')
    parser.add_argument(
        'url', help='Ingrese url a consultar de datos abiertos',  type=str)
    parser.add_argument(
        'desde', help='Ingrese fecha inicial a consultar YYYY-M', type=str)
    parser.add_argument(
        'hasta', help='Ingrese fecha final a consultar YYYY-M', type=str)

    args = parser.parse_args()
    main(args.url,args.desde, args.hasta)
