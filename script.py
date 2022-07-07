from heapq import merge
import os
import glob
import csv
import pandas as pd
import numpy as np

# read in csv with codes for the Harmonized System (Sistema Armonizado) thus the label `sa`
sa = pd.read_excel('../senaex/files_support/chapters_only.xlsx', dtype={0:'str',1:'str'})
sa['TÍTULO'] = sa['TÍTULO'].str.split(pat = '.', expand=True)[1]

# read in csv for world countries, containing geographical coordinates
cnty_ = pd.read_csv('../senaex/files_support/worldcountries.csv', sep = '\t', usecols=['country', 'latitude', 'longitude'])

# read in csv for world cities, containing geographical coordinates (filter for ecuadorian cities)
cty_ = pd.read_csv('../senaex/files_support/worldcities.csv', usecols=['city', 'lat', 'lng', 'country'])
cty_ = cty_.loc[cty_['country'] == 'Ecuador']
cty_.drop(columns=['country'], inplace=True)


varnames = [
    'TIPO_EXPORTACION', 'FEC_INGRESO', 'DISTRITO', 'SUBPARTIDA', 'PAIS_DESTINO', 'PAIS_ORIGEN', 'FOB'
]

def process(file_paths, year):
    
    # defining list that will contain all the data files for one year
    dfs = []


    for file_path in file_paths:

        # defining the file's delimiter
        sep = ''

        with open(file_path, 'r', encoding = 'utf-8') as rf:
            reader = csv.reader(rf)
            line = next(reader)
            if len(line) == 1:
                sep = '|'
            if len(line) == 18:
                sep = ','

        # reading in the data file and appeding to list
        df_temp = pd.read_csv(file_path, sep=sep, usecols = varnames)
        dfs.append(df_temp)

    # concatenate the dataframes for the same year
    df = pd.concat(dfs, ignore_index=True)

    # create `date` field as a datetime dtype
    df['date'] = pd.to_datetime(df['FEC_INGRESO'])

    # create a dataframe from the column `PAIS_DESTINO` that contains iso codes and countries names for final destination countries
    destino = df['PAIS_DESTINO'].str.split('-', expand=True).rename(columns={0:'iso_destino', 1:'pais_destino'})

    # create a dataframe from the column `PAIS_ORIGEN` that contains iso codes and countries names for countries fo origin
    origen = df['PAIS_ORIGEN'].str.split('-', expand=True).rename(columns={0:'iso_origen', 1:'pais_origen'})   

    # replace values in `DISTRITO` column
    df.replace({
        'DISTRITO' : {
            '028-GUAYAQUIL - MARITIMO' : '028-GUAYAQUIL(MARITIMO)',
            '019-GUAYAQUIL - AEREO' : '019-GUAYAQUIL(AEREO)',
            '109-LOJA - MACARA': '109-LOJA_MACARA',
        }
    }, inplace=True)

    # new data frame containing columns `distr_code` and `distr name`
    distrito = df['DISTRITO'].str.split('-', expand=True).rename(columns={0:'distr_code', 1:'distr_name'})

    # Construct a column with city names to later include latitudes and longitudes for ports of entry and exit
    port_dict = {
        '028-GUAYAQUIL(MARITIMO)' : 'Guayaquil',
        '019-GUAYAQUIL(AEREO)' : 'Guayaquil',
        '055-QUITO' : 'Quito',
        '073-TULCAN' : 'Tulcán',
        '082-HUAQUILLAS' : 'Huaquillas',
        '064-PUERTO BOLIVAR' : 'Machala',
        '037-MANTA': 'Manta',
        '127-LATACUNGA' : 'Latacunga',
        '046-ESMERALDAS' : 'Esmeraldas',
        '145-CEBAF SAN MIGUEL' : 'Nueva Loja',
        '109-LOJA_MACARA' : 'Macará',
    }

    df['distr_ref'] = df['DISTRITO']
    df.replace({'distr_ref':port_dict}, inplace=True)
    
    # new dataframe will contain data from `destino` df and `origen` df
    df = pd.concat([df, destino, origen, distrito], axis=1)

    # GET CODES FOR CHAPTERS IN THE GENERAL CLASSIFICATION OF PRODUCTS: i) Change the 'SUBPARTIDA' column data type\
    # to string. ii) create a new variable that tells us the length of the elements in 'SUBPARTIDA'. iii) \ Whenever
    # 'SUBPARTIDA' is of length 9, add a 0 at the beginning. 'SUBPARTIDA' must be 10-digit. iv) The chapter reference \
    # are the first two characters/digits in 'SUBPARTIDA'
    df['SUBPARTIDA'] = df['SUBPARTIDA'].astype("str")
    df['SUBPART_LENGTH'] = df['SUBPARTIDA'].str.len()
    df['SUBPARTIDA'] = np.where(df['SUBPART_LENGTH'] == 9, '0' + df['SUBPARTIDA'], df['SUBPARTIDA'])
    df['sa_chap'] = df['SUBPARTIDA'].str.slice(stop=2)

    # drop now redudant columns
    df.drop(['DISTRITO', 'PAIS_ORIGEN', 'PAIS_DESTINO', 'SUBPARTIDA', 'SUBPART_LENGTH', 'FEC_INGRESO'], axis=1, inplace=True)

    # merging datasets
    df0 = (pd.merge(df, sa, how = 'left', left_on = 'sa_chap', right_on = 'CAPÍTULO')
            .rename(columns={'TÍTULO': 'tip_merc'})
            .drop(['CAPÍTULO'], axis = 1))
    df1 = (pd.merge(df0, cnty_, how = 'left', left_on = 'iso_destino', right_on = 'country')
            .rename(columns={'latitude' : 'lat_destino', 'longitude' : 'lng_destino'})
            .drop(['country'], axis = 1))
    df2 = (pd.merge(df1, cnty_, how = 'left', left_on = 'iso_origen', right_on = 'country')
            .rename(columns={'latitude' : 'lat_origen', 'longitude' : 'lng_origen'})
            .drop(['country'], axis = 1))
    df3 = (pd.merge(df2, cty_, how='left', left_on='distr_ref', right_on='city')
            .rename(columns={'lat' : 'lat_distr', 'lng' : 'lng_distr'})
            .drop(['city'], axis = 1))

    # aggregating data to monthly frequency
    df = df3.groupby(
        [
            df3['date'].dt.strftime('%Y-%m'), 'TIPO_EXPORTACION', 'sa_chap', 'tip_merc', 
            'distr_code', 'distr_name', 'distr_ref', 'lat_distr', 'lng_distr','iso_destino','pais_destino', 
            'lat_destino', 'lng_destino', 'iso_origen', 'pais_origen', 'lat_origen', 'lng_origen'
        ]
    ).agg({
        'FOB': 'sum'
    }).reset_index()

    df['pais_origen'] = df['pais_origen'].str.title()
    df['pais_destino'] = df['pais_destino'].str.title()

    # set  `date` as the index before saving
    df.set_index('date', inplace=True)

    # save to specified folder
    df.to_csv(f'../senaex/files/exportaciones_{year}.csv')     


if __name__ == '__main__':
    anios = list(range(2013,2023))
    for anio in anios:
        file_paths = glob.glob(os.path.join('../senaex/downloads/', f'senae_exportacion*{anio}.csv'))
        
        process(file_paths=file_paths, year=anio)