# To run:
# python compare.py

'''
Background:
Compare the data I downloaded and processed with the data I obtained by email directly from the water personnel.
'''

import os
import re
import copy
import sys
import pandas as pd
import warnings
import datetime
warnings.simplefilter(action='ignore', category=FutureWarning)

DATA_FILE_DOWN = 'down_mercer_water_data.csv'
DATA_FILE_DIRECT = 'direct_mercer_butler_water_data.csv' 


def main(args):
    script_file = args[0]
    script_directory = os.getcwd()

    down_file = os.path.join(script_directory, 'down', DATA_FILE_DOWN)
    direct_file = os.path.join(script_directory, 'direct', DATA_FILE_DIRECT)

    down_df = pd.read_csv(down_file)
    direct_df = pd.read_csv(direct_file)

    print('Entry down_df\n', down_df.columns.tolist(), '\n', down_df.head(4))
    print('Entry direct_df\n', direct_df.columns.tolist(), '\n', direct_df.head(4))

    # down: ['Sample Location', 'Contaminant ID', 'Analysis Result', 'MCL In Effect', 'Sample Date', 'Sample Type', 'Laboratory ID', 'Analysis Method', 'Analysis Date', 'Sample Received Date', 'SPLIT_PAGES', 'PWSID', 'SYSTEM NAME', 'MAIN_PAGE', 'TOP_MARGIN', 'SYSNAME_PAGE', 'SAMPLE POINT AVAILABILITY', 'SAMPLE POINT NAME', 'CLIENT ID', 'SITE_ID', 'POPULATION SERVED', 'PRIMARY SOURCE', 'DISTRICT', 'REGION', 'COUNTY', 'ACTIVITY CODE', 'PRIMARY FACILITY ID', 'SYSTEM TYPE', 'OWNER TYPE', 'SITE_NAME', 'EPA_SITE_ID', 'ADDRESS1', 'ADDRESS2', 'CITY', 'STATE_CODE', 'ZIP_CODE']
    # direct: ['PWSID', 'CONTAMID', 'CONTNAM', 'RESULT', 'SAMPTYPE', 'SAMPDATE', 'SAMPTIME', 'ANALDATE', 'SYSTYPE', 'LOC_EPID', 'SYSNAME', 'POPL', 'AREACITY', 'SYSOWNAM', 'MAIL_ADDR1', 'MAIL_ADDR2', 'MAIL_ZIP']

    direct_df.rename(columns={
        'CONTNAM': 'Contaminant ID',
        'SAMPDATE': 'Sample Date',
        'ANALDATE': 'Analysis Date',
        'SYSNAME': 'SYSTEM NAME'
        }, inplace=True)   
    print('direct:', direct_df.columns.tolist())

    down_df['PWSID'] = down_df['PWSID'].astype(int)
    direct_df['PWSID'] = direct_df['PWSID'].astype(int)
    down_df['SYSTEM NAME'] = down_df['SYSTEM NAME'].apply(lambda x: x.strip())
    direct_df['SYSTEM NAME'] = direct_df['SYSTEM NAME'].apply(lambda x: x.strip())
    down_df['Contaminant ID'] = down_df['Contaminant ID'].apply(lambda x: x.strip())
    direct_df['Contaminant ID'] = direct_df['Contaminant ID'].apply(lambda x: x.strip())

    print('\n', down_df.dtypes)
    print('\n', direct_df.dtypes)

    # Sample Date down 09/08/2014   direct 8/4/2017      
    # Analysis Date down 8/8/2014   direct 8/4/2017
    # there are some dates with spaces in them
    down_df['Sample Date'] = down_df['Sample Date'].apply(lambda x: x.replace(' ', ''))
    down_df['Sample Date'] = pd.to_datetime(down_df['Sample Date'])
    direct_df['Sample Date'] = pd.to_datetime(direct_df['Sample Date'])

    down_df.loc[((down_df['Analysis Date'] == 'nan') | (down_df['Analysis Date'] == 'NaN') | (down_df['Analysis Date'] == '') | (down_df['Analysis Date'] == '.')), 'Analysis Date'] = '1/9/1999' 
    down_df['Analysis Date'] = pd.to_datetime(down_df['Analysis Date'])
    direct_df['Analysis Date'] = pd.to_datetime(direct_df['Analysis Date'])

    down_df['SAMPTYPE'] = down_df['Sample Type'].apply(lambda x: x[:1])
    direct_df['LOC_EPID'] = direct_df['LOC_EPID'].apply(lambda x: 'n' + str(x))
    down_df['LOC_EPID'] = down_df['Sample Location']

    dfso = down_df.shape[0]
    dfsi = direct_df.shape[0]
    print('\n\nDOWN down load count:', dfso)
    print('down_df\n', down_df.columns.tolist(), '\n', down_df.head(4))
    # print('\n', down_df.dtypes)
    print('\n\nDIRECT direct load count:', dfsi)
    print('direct_df\n', direct_df.columns.tolist(), '\n', direct_df.head(4))
    # print('\n', direct_df.dtypes)

    down_df.reset_index()
    down_df['row_num_down'] = down_df.index
    direct_df.reset_index()
    direct_df['row_num_direct'] = direct_df.index

    down_df.to_csv('down_prep.csv', index=False)  
    direct_df.to_csv('direct_prep.csv', index=False)  

    join_field = ['PWSID', 'Contaminant ID', 'SYSTEM NAME', 'Sample Date', 'Analysis Date', 'SAMPTYPE', 'LOC_EPID']
    compare_df = pd.merge(down_df, direct_df, on=join_field, how='inner', suffixes=('', '_c'))

    compare_df.loc[((compare_df['MAIL_ADDR2'] == 'nan') | (compare_df['MAIL_ADDR2'] == 'NaN')), 'MAIL_ADDR2'] = ''  

    # get rid of both results are equal
    compare_df['RESULT'] = compare_df['RESULT'].astype(float)
    compare_df['Analysis Result'] = compare_df['Analysis Result'].astype(float)    
    
    # use the next if only want a specific subset
    # compare_less_df = compare_df.loc[((compare_df['RESULT'] != compare_df['Analysis Result']) & (compare_df['RESULT'] != '') & (compare_df['Analysis Result'] != ''))]
    compare_less_df = compare_df

    print('\n--DOWN down load count:', dfso)
    print('--DIRECT direct load count:', dfsi)
    dfco = compare_less_df.shape[0]
    print('compare_less_df:', dfco)
    print('\nCOMPARE compare_less_df load count:', dfco)
    print('compare_less_df\n', compare_less_df.columns.tolist(), '\n', compare_less_df.head(4))
    compare_less_df.to_csv('compare_less_down_direct.csv', index=False)  

    return 'Done'


if __name__ == "__main__":
    args = sys.argv
    main(args)
