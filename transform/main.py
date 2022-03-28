import argparse
import logging
import os
import zipfile
from datetime import datetime

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransformFiles:

    def __init__(self, folder_path, initial, ending):
        self._initial = initial.replace('-', '_')
        self._ending = ending.replace('-', '_')
        self._folder_path = folder_path
        self._base_dir = os.path.normpath(os.getcwd() + os.sep + os.pardir)
        self._files = []
        self._df = pd.DataFrame()

        self._config()

    def _config(self):
        np.set_printoptions(suppress=True)

    def _load_files(self, extension):
        self._files = []
        logger.info('Cargando archivos con extensión {}'.format(extension))
        for file in os.listdir(self._folder_path):
            if file.endswith(extension):
                self._files.append({
                    'name': file,
                    'path': f'{self._folder_path}/{file}'
                })

        logger.info(f'Archivos cargados: {self._files}')

    def _unzip_files(self):
        logger.info('Descomprimiendo archivos')
        for file in self._files:
            logger.info(f'Descomprimiendo el archivo {file}')
            with zipfile.ZipFile(file['path'], 'r') as zip_ref:
                zip_ref.extractall(
                    f'{self._folder_path}')
                logger.info(f'Archivo {file} descomprimido')

                os.remove(file['path'])

    def _transform_datetime(self, df):
        df['FechaCreacion'] = pd.to_datetime(df['FechaCreacion'])
        df['FechaEnvio'] = pd.to_datetime(df['FechaEnvio'])
        df['FechaSolicitudCancelacion'] = pd.to_datetime(
            df['FechaSolicitudCancelacion'])
        df['FechaAceptacion'] = pd.to_datetime(df['FechaAceptacion'])

        return df

    def _transform_numeric(self, df):
        df['MontoTotalOC_PesosChilenos'] = pd.to_numeric(
            df['MontoTotalOC_PesosChilenos'].str.replace(',', '.'), errors='coerce')
        df['Impuestos'] = pd.to_numeric(
            df['Impuestos'].str.replace(',', '.'), errors='coerce')
        df['Descuentos'] = pd.to_numeric(
            df['Descuentos'].str.replace(',', '.'), errors='coerce')
        df['TotalNetoOC'] = pd.to_numeric(
            df['TotalNetoOC'].str.replace(',', '.'), errors='coerce')
        df['PorcentajeIva'] = pd.to_numeric(
            df['PorcentajeIva'].str.replace(',', '.'), errors='coerce')
        df['PorcentajeIva'] = df['PorcentajeIva'] / 100
        df['precioNeto'] = pd.to_numeric(
            df['precioNeto'].str.replace(',', '.'), errors='coerce')
        df['totalDescuentos'] = pd.to_numeric(
            df['totalDescuentos'].str.replace(',', '.'), errors='coerce')
        df['totalLineaNeto'] = pd.to_numeric(
            df['totalLineaNeto'].str.replace(',', '.'), errors='coerce')

        return df

    def _clean_data(self, file):
        logger.info('Limpiando archivo {}'.format(file['name']))
        df = pd.read_csv(file['path'], sep=';',
                         encoding='latin1')
        df = df[[
            'ID',
            'Codigo',
            'codigoEstado',
            'codigoEstadoProveedor',
            'EstadoProveedor',
            'FechaCreacion',
            'FechaEnvio',
            'FechaSolicitudCancelacion',
            'FechaAceptacion',
            'TipoMonedaOC',
            'MontoTotalOC_PesosChilenos',
            'Impuestos',
            'TipoImpuesto',
            'Descuentos',
            'Cargos',
            'TotalNetoOC',
            'CodigoUnidadCompra',
            'RutUnidadCompra',
            'OrganismoPublico',
            'sector',
            'CiudadUnidadCompra',
            'RegionUnidadCompra',
            'PaisUnidadCompra',
            'RutSucursal',
            'CodigoSucursal',
            'Sucursal',
            'CodigoProveedor',
            'NombreProveedor',
            'ComunaProveedor',
            'RegionProveedor',
            'PaisProveedor',
            'PorcentajeIva',
            'FormaPago',
            'TipoDespacho',
            'CodigoLicitacion',
            'codigoCategoria',
            'IDItem',
            'Categoria',
            'monedaItem',
            'totalCargos',
            'precioNeto',
            'totalDescuentos',
            'totalImpuestos',
            'totalLineaNeto',
            'Forma de Pago'
        ]]
        df = self._transform_datetime(df)
        df = self._transform_numeric(df)
        df = df.convert_dtypes()
        df = df.dropna()
        df = df.drop_duplicates()
        df = df.reset_index(drop=True)

        self._df = pd.concat([self._df, df], ignore_index=True)
        os.remove(file['path'])

    def _clean_all_files(self):
        for file in self._files:
            self._clean_data(file)

    def _save_merged_df(self):
        logger.info('Guardando archivo de datos')
        _date = datetime.now().strftime('%Y_%m_%d')
        self._df.to_csv(f'{self._base_dir}/clean_data/{self._initial}_{self._ending}_{_date}_clean_.csv',
                        index=False, encoding='latin1')

    def _run(self):
        self._load_files(extension='.zip')
        self._unzip_files()
        self._load_files(extension='.csv')
        self._clean_all_files()
        self._load_files(extension='.csv')
        self._save_merged_df()


def main(path, initial, ending):

    logger.info(f'Path: {path}')
    _transform = TransformFiles(path, initial, ending)
    _transform._run()


if __name__ == '__main__':
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        'path', help='Path to folder with files', type=str)
    argparser.add_argument(
        'initial', help='Initial date', type=str)
    argparser.add_argument(
        'ending', help='Ending date', type=str)
    args = argparser.parse_args()
    main(args.path, args.initial, args.ending)
