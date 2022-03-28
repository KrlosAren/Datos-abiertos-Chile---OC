import argparse
import logging
import os
import subprocess
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL_BASE = 'https://transparenciachc.blob.core.windows.net/oc-da'
TODAY = datetime.now()


def _extract_data(url, desde, hasta):
    logger.info('Iniciando extracción de datos')
    logger.info(f'Desde: {desde}')
    logger.info(f'Hasta: {hasta}')

    subprocess.run(['python3', 'main.py', url, desde, hasta], cwd='./extract')
    logger.info('Fin de extracción de datos')


def _tranform_files(path, desde, hasta):
    logger.info('Iniciando transformación de datos')
    subprocess.run(['python3', 'main.py', os.getcwd() + path, desde, hasta],
                   cwd='./transform')
    logger.info('Fin de transformación de datos')


def _string_to_datetime(date):
    return datetime.strptime(date, '%Y-%m')


def _validate_date(date):

    try:
        bool(datetime.strptime(date, '%Y-%m'))
    except ValueError:
        raise argparse.ArgumentTypeError(
            'Fecha debe tener formato YYYY-M')
    return date


def _options():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--inicio', type=_validate_date,
                        help='Fecha inicial de consulta formato YYYY-M  => ej: 2022-1')
    parser.add_argument('-f', '--fin', type=_validate_date,
                        help='Periodo final de de consulta formato YYYY-M  => ej: 2022-2')
    return parser.parse_args()


def _check_end_date(date):
    _today = TODAY.strftime('%Y-%m')
    message = f'La fecha final debe ser menor o igual que {_today}'
    if _string_to_datetime(date) > TODAY:
        raise Exception(message)

    return date


def _check_inital_date(date):
    _today = TODAY.strftime('%Y-%m')
    message = f'La fecha inicial debe ser igual o menor que {_today}'
    if _string_to_datetime(date) < TODAY:
        raise Exception(message)
    return date


def _run(url, desde, hasta):
    _extract_data(url, desde, hasta)
    _tranform_files('/data', desde, hasta)


def main():
    args = _options()
    if args.inicio and args.fin:
        _check_end_date(args.fin)
        if args.inicio > args.fin:
            raise Exception(
                'La fecha inicial debe ser menor que la fecha final')
        _run(URL_BASE, args.inicio, args.fin)
        return
    if args.inicio:
        _check_inital_date(args.inicio)
        _run(URL_BASE, args.inicio, args.inicio)
        return


if __name__ == '__main__':
    main()
