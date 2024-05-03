# To run:
# python down_water_prep.py pdf
# python down_water_prep.py csv
# python down_water_prep.py join
# when pdf, will parse the data pdfs and turn into csvs
# when csv, will aggregate csvs and prepare two files (site info and data) for joining
# when join, the two files will be joined into one final file

'''
Background for data ETL

GETTING THE DATA via DOWNLOAD
Water Quality Data Obtained from the PA DEP 20240314
https://www.dep.pa.gov/DataandTools/Reports/Pages/Water.aspx
Obtained and transformed as follows:
Scroll down to the Drinking Water Reports section
Select: Drinking Water Reporting System (DWRS)
Select: Selection Criteria
Select radio button: Public Water Systems' Details
Routed to http://www.drinkingwater.state.pa.us/dwrs/HTM/SelectionCriteria.html
System Activity: Active
County/Counties: Mercer County
Information Request: Detailed Sample Results
Click Submit

Routed to http://www.drinkingwater.state.pa.us/dwrsbroker/broker.exe
Select System(s): Select All Systems
Select Detail Sample Results Based On: select Sample Period
---Selected Dates (1/1/2014 to 12/31/2023)
Contaminant Group: select all
Contaminant: select all
Click Submit

Routed to an html with a file for each year:
Data Extraction: From 1JAN2014 to 31DEC2023
YEAR	SAMPLE RECORDS
2014	14854
2015	16096
2016	19471
2017	19323
2018	21966
2019	25443
2020	24738
2021	25479
2022	26370
2023	25293

Click on the year to download each yearly file. 
Download for each Year into a pdf (the whole year as a csv was not an option)
These have pages with labels like *** PWSID = 6430040 | SYSTEM NAME = GROVE CITY BORO WATER DEPT*** 
with headers as follows:
-PWSID
-SYSTEM NAME
-SAMPLE LOCATION
-CONTAMINANT
-ANALYSIS RESULT
-MCL IN EFFECT
-SAMPLE DATE
-SAMPLE TYPE
-LABORATORY	
-ANALYSIS METHOD
-ANALYSIS DATE
-SAMPLE RECEIVED
-DATE

spi_df
['PWSID', 'SYSTEM NAME', 'ACTIVITY', 'SYSTYPE', 'SAMPLE POINT ID', 'SAMPLE POINT AVAILABILITY', 'SAMPLE POINT NAME', 'TTHM MONITORING LOCATION', 'HAA5 MONITORING LOCATION', 'CLIENT ID', 'SITE ID', 'PRMRY_FAC_ID', 'POPULATION SERVED', 'PRIMARY SOURCE', 'DISTRICT', 'REGION', 'COUNTY', 'RTCR MONITORING LOCATION', 'SEASONAL START UP LOCATION']
bi_df
['PWSID', 'SYSTEM NAME', 'ACTIVITY CODE', 'CLIENT ID', 'SITE ID', 'PRIMARY FACILITY ID', 'SYSTEM TYPE', 'OWNER TYPE', 'POPULATION SERVED', 'NONTRANSIENT POPULATION SERVED', 'CONSECUTIVE INDICATOR', 'PRIMARY SELLER', 'REGULATED BY', 'PRIMARY SOURCE', 'PRIMARY SELLER STATE', 'VENDING PWS PERM BY RULE', 'SERVICE CONNECTIONS', 'COUNTY', 'DISTRICT', 'REGION']
sites_df
['SITE_ID', 'SITE_NAME', 'SISSCD_ID', 'EPA_SITE_ID', 'ADDRESS1', 'ADDRESS2', 'CITY', 'STATE_CODE', 'ZIP_CODE']
master_df
['Sample Location', 'Contaminant ID', 'Analysis Result', 'MCL In Effect', 'Sample Date', 'Sample Type', 'Laboratory ID', 'Analysis Method', 'Analysis Date', 'Sample Received Date', 'SPLIT_PAGES', 'PWSID', 'SYSTEM NAME', 'MAIN_PAGE', 'TOP_MARGIN', 300, 'TOC', 4.2, '.', '07/23/2014', 'RAW WATER']


Files: [
'rptinfo2014.pdf', 
'rptinfo2015.pdf', 
'rptinfo2016.pdf', 
'rptinfo2017.pdf', 
'rptinfo2018.pdf', 
'rptinfo2019.pdf', 
'rptinfo2020.pdf', 
'rptinfo2021.pdf', 
'rptinfo2022.pdf', 
'rptinfo2023.pdf'
]

Next, go back to selection criteria at:
http://www.drinkingwater.state.pa.us/dwrs/HTM/SelectionCriteria.html
System Activity: select both Active and Inactive
County/Counties: Mercer
Information Request: Inventory Information 
Click Submit

Routed to http://www.drinkingwater.state.pa.us/dwrsbroker/broker.exe
In Select Systems: select all of them
In Select Inventory Data Report: select Sample Point Information 
Note: this is one for each sample location - which can be more than one for each PWSID
Click Submit

Download each page to a csv (there are 4).

Partial Example:
PWSID	 SYSTEM NAME	            ACTIVITY	 SAMPLE POINT ID	 SAMPLE POINT AVAILABILITY	 SAMPLE POINT NAME  
6430040  GROVE CITY BORO WATER DEPT	ACTIVE	     764	             PERMANENT	                 418 BESSEMER AVE    
6430040  GROVE CITY BORO WATER DEPT	ACTIVE	     765	             PERMANENT	                 214 FRANKLIN PLACE  

Files: [
'csvdispSPIp1.csv',
'csvdispSPIp2.csv',
'csvdispSPIp3.csv',
'csvdispSPIp4.csv',
]

Next, go back to selection criteria at:
http://www.drinkingwater.state.pa.us/dwrs/HTM/SelectionCriteria.html
System Activity: select both Active and Inactive
County/Counties: Mercer
Information Request: Inventory Information 
Click Submit

Routed to http://www.drinkingwater.state.pa.us/dwrsbroker/broker.exe
In Select Systems: select all of them
In Select Inventory Data Report: select Basic Information
Note: this is one for each PWSID
Click Submit

Download each page to a csv (there are 2).

Partial Example:
PWSID	 SYSTEM NAME	            ACTIVITY CODE	 CLIENT ID	 SITE ID	 PRIMARY FACILITY ID    
6430040  GROVE CITY BORO WATER DEPT	ACTIVE	         174	     447196      472793

Files: [
'csvdispBIp1.csv',
'csvdispBIp2.csv',
]

In the above mentioned interface, the SITE ID is an active link. Clicking on the SITEID goes to (Redownloaded 4/26/2024)
https://www.ahs.dep.pa.gov/eFACTSWeb/searchResults_singleSite.aspx?SiteID=447196
From this interface, click on the "Click on Sites by Count/Muni Search" button.
Select Mercer County (no municipality), then Search
This gives a list of all sites, not just water sites (5564 records on the redownload).
There is an Excel export near the top, click it to get an .xlsx file of sites.

File: ResultsSite.xlsx

'''

import re
import copy
import sys
import pandas as pd
import tabula # to get an area as a table
import PyPDF2 # if want the whole page as text
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# if Analysis Date is NOT in the header list, we need to combine the two pages into one dataframe before merge into the combined data frame
REPLACE_TEMP = '.~.'
CHECK_COLUMN2 = 'Analysis' + REPLACE_TEMP + 'Method'
CHECK_COLUMN1 = 'Contaminant' + REPLACE_TEMP + 'ID'
# 70 worked most of the time, but in a few pages, the MCL and Sample (of Sample Received Date) where not picked up
TOP1 = 70
# 45 worked well 
TOP2 = 45
HEADERS_EXPECTED = [
    'Sample' + REPLACE_TEMP + 'Location',
    'Contaminant' + REPLACE_TEMP + 'ID',
    'Analysis' + REPLACE_TEMP + 'Result',
    'MCL' + REPLACE_TEMP + 'In' + REPLACE_TEMP + 'Effect',
    'Sample' + REPLACE_TEMP + 'Date',
    'Sample' + REPLACE_TEMP + 'Type',
    'Laboratory' + REPLACE_TEMP + 'ID',
    'Analysis' + REPLACE_TEMP + 'Method',
    'Analysis' + REPLACE_TEMP + 'Date',
    'Sample' + REPLACE_TEMP + 'Received' + REPLACE_TEMP + 'Date'
]
HEADERS_EXTRA = [
    'SPLIT_PAGES',
    'PWSID',
    'SYSTEM NAME',
    'MAIN_PAGE',
    'TOP_MARGIN',
    'SYSNAME_PAGE'
]
BI_FILES = [
'csvdispBIp1.csv',
'csvdispBIp2.csv',
]
SPI_FILES = [
'csvdispSPIp1.csv',
'csvdispSPIp2.csv',
'csvdispSPIp3.csv',
'csvdispSPIp4.csv',
]
SITE_FILE = 'ResultsSite.xlsx'
DATA_FILES = [
    'rptinfo2014', 
    'rptinfo2015', 
    'rptinfo2016', 
    'rptinfo2017', 
    'rptinfo2018', 
    'rptinfo2019', 
    'rptinfo2020', 
    'rptinfo2021', 
    'rptinfo2022', 
    'rptinfo2023'
]
# here here for testing ONLY
# DATA_FILES = ['rptinfo2022', 
#     'rptinfo2023']

SITE_PREP = 'downprep_site.csv'
DATA_PREP = 'downprep_data.csv'

DATA_FILE_DOWN = 'down_mercer_water_data.csv'


def main(args):
    script_file = args[0]
    pdf_or_csv = args[1]

    if pdf_or_csv == 'pdf':
        for filename_prefix in DATA_FILES:
            filename = filename_prefix + '.pdf'
            master_df = make_df_of_years(filename)

            # replace the interim with a space in the data frame 
            master_df.replace({REPLACE_TEMP: ' '}, regex=True, inplace=True)  
            # replace the interim with a space in the headers 
            master_df.columns = master_df.columns.str.replace(REPLACE_TEMP, ' ', regex=True)      

            # print('master_df after change spacer')
            # print(master_df.columns.tolist())
            # print(master_df)

            save_file_prefix = filename[0: 11]
            filename2 = save_file_prefix + '.csv'  
            master_df.to_csv(filename2, index=False)

    elif pdf_or_csv == 'csv': 
        renamed_data_files = []
        for filename_prefix in DATA_FILES:
            filename = filename_prefix[0: 11] + '.csv'
            renamed_data_files.append(filename)

        spi_df = combine_csvs_into_one_df(SPI_FILES)
        bi_df = combine_csvs_into_one_df(BI_FILES)
        sites_df = pd.read_excel(SITE_FILE)
        data_df = combine_csvs_into_one_df(renamed_data_files) 


        # print('**1) spi_df')
        # print(spi_df.columns.tolist())
        # print('**2) bi_df')
        # print(bi_df.columns.tolist())
        # print('**3) sites_df')
        # print(sites_df.columns.tolist())
        # print('**4) data_df')
        # print(data_df.columns.tolist())   

        # **1) spi_df
        # ['PWSID', 'SYSTEM NAME', 'ACTIVITY', 'SYSTYPE', 'SAMPLE POINT ID', 'SAMPLE POINT AVAILABILITY', 'SAMPLE POINT NAME', 'TTHM MONITORING LOCATION', 'HAA5 MONITORING LOCATION', 'CLIENT ID', 'SITE ID', 'PRMRY_FAC_ID', 'POPULATION SERVED', 'PRIMARY SOURCE', 'DISTRICT', 'REGION', 'COUNTY', 'RTCR MONITORING LOCATION', 'SEASONAL START UP LOCATION']
        # **2) bi_df
        # ['PWSID', 'SYSTEM NAME', 'ACTIVITY CODE', 'CLIENT ID', 'SITE ID', 'PRIMARY FACILITY ID', 'SYSTEM TYPE', 'OWNER TYPE', 'POPULATION SERVED', 'NONTRANSIENT POPULATION SERVED', 'CONSECUTIVE INDICATOR', 'PRIMARY SELLER', 'REGULATED BY', 'PRIMARY SOURCE', 'PRIMARY SELLER STATE', 'VENDING PWS PERM BY RULE', 'SERVICE CONNECTIONS', 'COUNTY', 'DISTRICT', 'REGION']
        # **3) sites_df
        # ['SITE_ID', 'SITE_NAME', 'SISSCD_ID', 'EPA_SITE_ID', 'ADDRESS1', 'ADDRESS2', 'CITY', 'STATE_CODE', 'ZIP_CODE']
        # **4) data_df
        # ['Sample Location', 'Contaminant ID', 'Analysis Result', 'MCL In Effect', 'Sample Date', 'Sample Type', 'SPLIT_PAGES', 'PWSID', 'SYSTEM NAME', 'MAIN_PAGE', 'TOP_MARGIN', 'SYSNAME_PAGE', 'Laboratory ID', 'Analysis Method', 'Analysis Date', 'Sample Received Date']


        # make sure that the data are the same type so the pandas joins will work correctly   
        spi_df['PWSID'] = spi_df['PWSID'].astype(int)
        bi_df['PWSID'] = bi_df['PWSID'].astype(int)
        spi_df['POPULATION SERVED'] = spi_df['POPULATION SERVED'].astype(str)
        bi_df['POPULATION SERVED'] = bi_df['POPULATION SERVED'].astype(str)
        data_df['PWSID'] = data_df['PWSID'].astype(int)
        spi_df['SYSTEM NAME'] = spi_df['SYSTEM NAME'].astype(str)
        bi_df['SYSTEM NAME'] = bi_df['SYSTEM NAME'].astype(str)
        data_df['SYSTEM NAME'] = data_df['SYSTEM NAME'].astype(str)    

        spi_df.rename(columns={'PRMRY_FAC_ID': 'PRIMARY FACILITY ID'}, inplace=True)   

        # send it if you need it
        # data_df.to_csv('data_df.csv', index=False)    

        # join what need - the order of the joins matters
        spi_bi_df = pd.merge(spi_df, bi_df, on=['PWSID', 'SYSTEM NAME', 'SITE ID'], how='outer', suffixes=('', '_y'))
        spi_bi_df.rename(columns={'SITE ID': 'SITE_ID'}, inplace=True)
        
        # send to a file just to examine results of first merge
        # spi_bi_df.to_csv('spi_bi_df.csv', index=False)

        site_spi_bi_df = pd.merge(spi_bi_df, sites_df, on=['SITE_ID'], how='left')

        # issues discovered reviewing the data  
        # site_spi_bi_df['SYSTEM NAME'] = site_spi_bi_df['SYSTEM NAME'].str.strip()
        site_spi_bi_df['SYSTEM NAME'] = site_spi_bi_df['SYSTEM NAME'].apply(lambda x: x.strip())
        site_spi_bi_df.loc[(site_spi_bi_df['CLIENT ID'].isnull()) & (site_spi_bi_df['CLIENT ID_y'].notnull()), 'CLIENT ID'] = site_spi_bi_df['CLIENT ID_y']
        site_spi_bi_df.loc[(site_spi_bi_df['COUNTY'].isnull()) & (site_spi_bi_df['COUNTY_y'].notnull()), 'COUNTY'] = site_spi_bi_df['COUNTY_y']
        site_spi_bi_df.loc[(site_spi_bi_df['DISTRICT'].isnull()) & (site_spi_bi_df['DISTRICT_y'].notnull()), 'DISTRICT'] = site_spi_bi_df['DISTRICT_y']
        site_spi_bi_df.loc[(site_spi_bi_df['POPULATION SERVED'].isnull()) & (site_spi_bi_df['POPULATION SERVED_y'].notnull()), 'POPULATION SERVED'] = site_spi_bi_df['POPULATION SERVED_y']
        site_spi_bi_df.loc[(site_spi_bi_df['PRIMARY SOURCE'].isnull()) & (site_spi_bi_df['PRIMARY SOURCE_y'].notnull()), 'PRIMARY SOURCE'] = site_spi_bi_df['PRIMARY SOURCE_y']
        site_spi_bi_df.loc[(site_spi_bi_df['REGION'].isnull()) & (site_spi_bi_df['REGION_y'].notnull()), 'REGION'] = site_spi_bi_df['REGION_y']
        site_spi_bi_df.loc[(site_spi_bi_df['PRIMARY FACILITY ID'].isnull()) & (site_spi_bi_df['PRIMARY FACILITY ID_y'].notnull()), 'PRIMARY FACILITY ID'] = site_spi_bi_df['PRIMARY FACILITY ID_y']

        site_spi_bi_df.drop_duplicates(inplace=True)        
        site_spi_bi_df['SAMPLE POINT ID'] = site_spi_bi_df['SAMPLE POINT ID'].astype(str) 
        data_df['Sample Location'] = data_df['Sample Location'].astype(str)      
        data_df.loc[data_df['Sample Type'] == 'DISTRIBUTIO N', 'Sample Type'] = 'DISTRIBUTION'     
        # data_df['SYSTEM NAME'] = data_df['SYSTEM NAME'].str.strip()
        data_df['SYSTEM NAME'] = data_df['SYSTEM NAME'].apply(lambda x: x.strip())

        # RUN SOME CHECKS
        df1 = site_spi_bi_df[(site_spi_bi_df['CLIENT ID'] != site_spi_bi_df['CLIENT ID_y']) & (site_spi_bi_df['CLIENT ID_y'].notnull())]
        dfs1 = df1.shape[0]
        df2 = site_spi_bi_df[(site_spi_bi_df['COUNTY'] != site_spi_bi_df['COUNTY_y']) & (site_spi_bi_df['COUNTY_y'].notnull())]
        dfs2 = df2.shape[0]
        df3 = site_spi_bi_df[(site_spi_bi_df['DISTRICT'] != site_spi_bi_df['DISTRICT_y']) & (site_spi_bi_df['DISTRICT_y'].notnull())]
        dfs3 = df3.shape[0]
        df4 = site_spi_bi_df[(site_spi_bi_df['POPULATION SERVED'] != site_spi_bi_df['POPULATION SERVED_y']) & (site_spi_bi_df['POPULATION SERVED_y'].notnull())]
        dfs4 = df4.shape[0]
        df5 = site_spi_bi_df[(site_spi_bi_df['PRIMARY SOURCE'] != site_spi_bi_df['PRIMARY SOURCE_y']) & (site_spi_bi_df['PRIMARY SOURCE_y'].notnull())]
        dfs5 = df5.shape[0]
        df6 = site_spi_bi_df[(site_spi_bi_df['REGION'] != site_spi_bi_df['REGION_y']) & (site_spi_bi_df['REGION_y'].notnull())]              
        dfs6 = df6.shape[0]
        df7 = site_spi_bi_df[(site_spi_bi_df['PRIMARY FACILITY ID'] != site_spi_bi_df['PRIMARY FACILITY ID_y']) & (site_spi_bi_df['PRIMARY FACILITY ID_y'].notnull())]              
        dfs7 = df7.shape[0]

        continue_if_true = True
        if dfs1 > 0:
            print('CLIENT ID DO NOT ALL MATCH')
            continue_if_true = False
        if dfs2 > 0:
            print('COUNTY DO NOT ALL MATCH')
            continue_if_true = False
        if dfs3 > 0:
            print('DISTRICT DO NOT ALL MATCH')
            continue_if_true = False
        if dfs4 > 0:
            print('POPULATION SERVED DO NOT ALL MATCH')
            continue_if_true = False
        if dfs5 > 0:
            print('PRIMARY SOURCE DO NOT ALL MATCH')
            continue_if_true = False
        if dfs6 > 0:
            print('REGION DO NOT ALL MATCH')
            continue_if_true = False
        if dfs7 > 0:
            print('PRIMARY FACILITY ID DO NOT ALL MATCH')
            continue_if_true = False            

        if continue_if_true is True:
            # NOTE: found out from June that the Sample Point ID (SPID) is different than the EPID
            # So, the data can be linked to the system info as follows:
            #     If the sample type is E, then loc_epid = epid
            #     If the sample type is D, then loc_epid = spid
            # unfortunately, in the files I downloaded, there is NO EPID, so I can not do the conditional join 
            site_spi_bi_df.rename(columns={
                'SAMPLE POINT ID': 'Sample Location',
                }, inplace=True)
            
            # send to a file just to examine results
            site_spi_bi_df.to_csv('downprep_site_spi_bi_df.csv', index=False)

            # Get a subset of fields
            fields_to_keep = [
                'PWSID', 
                'SYSTEM NAME', 
                'Sample Location', 
                'SAMPLE POINT AVAILABILITY',	
                'SAMPLE POINT NAME',	
                'CLIENT ID',	
                'SITE_ID',	
                'POPULATION SERVED',
                'PRIMARY SOURCE',	
                'DISTRICT',
                'REGION',	
                'COUNTY',
                'ACTIVITY CODE',
                'PRIMARY FACILITY ID',
                'SYSTEM TYPE',
                'OWNER TYPE',
                'SITE_NAME',
                'EPA_SITE_ID',
                'ADDRESS1',
                'ADDRESS2',
                'CITY',
                'STATE_CODE',
                'ZIP_CODE'
                ]
            
            site_sub_df = site_spi_bi_df[fields_to_keep]      
            print('site_sub_df\n', site_sub_df.columns.tolist(), '\n', site_sub_df.head(4))
            site_sub_df.to_csv(SITE_PREP, index=False)  

            print('data_df\n', data_df.columns.tolist(), '\n', data_df.head(4))
            data_df.to_csv(DATA_PREP, index=False)
            print('DONE - BUT NOTE......') 

            print('\n\nIf want the Lat and Long, the easiest way is to get it NOW using google sheets. ')
            print('Go to the down directory and get the file:', SITE_PREP)
            print('and make a concatenated field of ADDRESS1 ADDRESS2 CITY STATE_CODE ZIP_CODE as a valid address (in X=S1& " " & T1 & ", "&U1&", " &V1& ", "&W1).')
            print('In google drive, New google sheet. Extensions, Add-ons, Geocoding by SmartMonkey.')
            print('Copy in the addess field and fill in the Country usa. Extensions, select Geocoding by SmartMonkey')
            print('Copy the coordinates into the spread sheet then separate them into Latitude and Longitude (use test to columns)')
        else:
            print('Stopped because of mismatch of fields. You should have already gotten a message.')

    elif pdf_or_csv == 'join':      
        # in the site table and the data table, we have PWSID, SYSTEM NAME, Sample Location
        # BUT, Sample Location is often null, and I did not confirm that the SYSTEM NAME always matches
        # We want to do the matching hierarchically, match on all three fields, if no match, match two fields, if no match, match on PWSID only
        # I explored a dictionary, by row way, but it was TOO SLOW, so changing back to finding null fields and removing the extra fields            
        # make dicts with the index of site_sub_df (will always be the latest index that gets saved in the dict)

        data_columns_except = [
            'Sample Location',
            'Contaminant ID',
            'Analysis Result',
            'MCL In Effect',
            'Sample Date',
            'Sample Type',
            'Laboratory ID',
            'Analysis Method',
            'Analysis Date',
            'Sample Received Date',
            'SPLIT_PAGES',
            'PWSID',
            'SYSTEM NAME',
            'MAIN_PAGE',
            'TOP_MARGIN',
            'SYSNAME_PAGE'
        ]

        site_sub_df = pd.read_csv(SITE_PREP)
        data_df = pd.read_csv(DATA_PREP)
        site_sub_df['Sample Location'] = site_sub_df['Sample Location'].astype(str)
        data_df['Sample Location'] = data_df['Sample Location'].astype(str)
        number_rows_data = data_df.shape[0]

        site_sub_df.loc[(site_sub_df['Sample Location'] == 'nan') | (site_sub_df['Sample Location'] == 'NaN') | (site_sub_df['Sample Location'] == ''), 'Sample Location'] = 'X'  
        data_df.loc[(data_df['Sample Location'] == 'nan') | (data_df['Sample Location'] == 'NaN') | (data_df['Sample Location'] == ''), 'Sample Location'] = 'Y'  
        # one of the string transformations was adding a .0 (that I only found by NOT linking on Sample Location and seeing that it was 726.0 instead of 726)
        site_sub_df['Sample Location'] = site_sub_df['Sample Location'].apply(lambda x: x.replace('.0', ''))
        data_df['Sample Location'] = data_df['Sample Location'].apply(lambda x: x.replace('.0', ''))
        # there is an issue with some of the merges thinking Sample Location is an integer - preventative
        site_sub_df['Sample Location'] = site_sub_df['Sample Location'].apply(lambda x: 'n'+x)
        data_df['Sample Location'] = data_df['Sample Location'].apply(lambda x: 'n'+x)

        site_sub_df.reset_index()    
        site_sub_df['PWSID'] = site_sub_df['PWSID'].astype(str)
        site_sub_df['PWSID'] = site_sub_df['PWSID'].apply(lambda x: x.strip())           
        site_sub_df['Sample Location'] = site_sub_df['Sample Location'].apply(lambda x: x.strip())
        site_sub_df['SYSTEM NAME'] = site_sub_df['SYSTEM NAME'].astype(str)
        site_sub_df['SYSTEM NAME'] = site_sub_df['SYSTEM NAME'].apply(lambda x: x.strip())         
        print('site_sub_df\n', site_sub_df.columns.tolist(), '\n', site_sub_df.head(10))
        site_sub_df.to_csv('d3_'+SITE_PREP, index=False)  

        data_df.reset_index()
        data_df['PWSID'] = data_df['PWSID'].astype(str)
        data_df['PWSID'] = data_df['PWSID'].apply(lambda x: x.strip())        
        data_df['Sample Location'] = data_df['Sample Location'].apply(lambda x: x.strip())
        data_df['SYSTEM NAME'] = data_df['SYSTEM NAME'].astype(str)
        data_df['SYSTEM NAME'] = data_df['SYSTEM NAME'].apply(lambda x: x.strip())
        print('data_df\n', data_df.columns.tolist(), '\n', data_df.head(4))
        data_df.to_csv('d3_'+DATA_PREP, index=False)

        # option to get the first => df = oldDf.groupby('value').first().reset_index(), we are using the option in the next line
        site_sub_df_nd3 = site_sub_df.sort_values(by='Sample Location', ascending=False).drop_duplicates(subset=['PWSID', 'SYSTEM NAME', 'Sample Location'])    
        # print('site_sub_df_nd3\n', site_sub_df_nd3.columns.tolist(), '\n', site_sub_df_nd3.head(4))    
        site_sub_df_nd2 = site_sub_df.sort_values(by='Sample Location', ascending=False).drop_duplicates(subset=['PWSID', 'SYSTEM NAME'])      
        site_sub_df_nd1 = site_sub_df.sort_values(by='SYSTEM NAME', ascending=False).drop_duplicates(subset=['PWSID'])     

        # merge the dataframes as if all three were populated              
        new_data3f_df = pd.merge(data_df, site_sub_df_nd3, on=['PWSID', 'SYSTEM NAME', 'Sample Location'], how='left', suffixes=('', '_z3'))  
        # print('new_data3f_df\n', new_data3f_df.columns.tolist(), '\n', new_data3f_df.head(10))  
        # just in case the zip code got changed to something that will prevent recognized as empty 
        new_data3f_df.loc[(new_data3f_df['ZIP_CODE'] == 'nan') | (new_data3f_df['ZIP_CODE'] == 'NaN'), 'ZIP_CODE'] = ''  
        # print('new_data_df\n', new_data_df.columns.tolist(), '\n', new_data_df.head(25))  

        # print('Split the data dataframe into two - those with ZIP_CODE and those without')           
        notnull3f_df =  new_data3f_df[(new_data3f_df['ZIP_CODE'].notnull()) & (new_data3f_df['ZIP_CODE'] != '')]            
        null3f_df = new_data3f_df[(new_data3f_df['ZIP_CODE'].isnull()) | (new_data3f_df['ZIP_CODE'] == '')]

        # print('Remove all the previously merged null fields so they do not duplicate in the next merge')
        null3f_subset_df = null3f_df[data_columns_except]  

        # print('Fill the null address in the sub dataframe')
        null2f_joined_df = pd.merge(null3f_subset_df, site_sub_df_nd2, on=['PWSID', 'SYSTEM NAME'], how='left', suffixes=('', '_z2'))  

        # Are there still nulls?
        notnull2f_df =  null2f_joined_df[(null2f_joined_df['ZIP_CODE'].notnull()) & (null2f_joined_df['ZIP_CODE'] != '')]            
        null2f_df = null2f_joined_df[(null2f_joined_df['ZIP_CODE'].isnull()) | (null2f_joined_df['ZIP_CODE'] == '')]       
        
        # print('Remove all the previously merged null fields so they do not duplicate in the next merge')
        null2f_subset_df = null2f_df[data_columns_except]  

        # print('Fill the null address in the sub dataframe')
        null1f_joined_df = pd.merge(null2f_subset_df, site_sub_df_nd1, on=['PWSID'], how='left', suffixes=('', '_z1'))  

        # merge back together: notnull3f_df notnull2f_df null1f_joined_df
        almost_final_df = pd.concat([notnull3f_df, notnull2f_df])
        final_df = pd.concat([almost_final_df, null1f_joined_df])

        del final_df['Sample Location_z2']
        del final_df['SYSTEM NAME_z1']
        del final_df['Sample Location_z1']
        final_df.reset_index()
        print('final_df\n', final_df.columns.tolist(), '\n', final_df.head(5))  

        number_rows_final = final_df.shape[0]

        if number_rows_data != number_rows_final:
            print('\nWARNING: the number of rows in the data file at the beginning is not the same as the end.')

        final_df.to_csv(DATA_FILE_DOWN, index=False)             
        print('DONE') 

    else:
        print('You forgot your arguments when you called the program.')                

    return 'done'


def combine_csvs_into_one_df(list_of_files):
    list_of_dfs = []
    for filename in list_of_files:
        df = pd.read_csv(filename)
        list_of_dfs.append(df)
    # Combine the DataFrames into one
    combined_df = pd.concat(list_of_dfs, ignore_index=True)
    return combined_df


def make_df_of_years(filename):
    main_page_index_hold = 0
    HEADERS_EXPECTED.sort()
    dfhold = pd.DataFrame()
    master_df = pd.DataFrame()
    dfd = pd.DataFrame()
    top = 0
    concat_it = False

    with open(filename, 'rb') as file:
        # Create a PDF file reader object
        pdf_reader = PyPDF2.PdfReader(file)        
        # Iterate through each page in the PDF  (PyPDF2 starts with 0)        
        # here here for testing ONLY  
        # for 2015: range(534, 537) (works with top 45 and not 70) and range(227, 233) (works with top 70 and not 45)
        # for 2014: range(710, 715) for incomplete header on first page (I think)
        # for 2018: range(1637, 1641) for incomplete header on second page; range(623, 629) incomplete header on first page
        # for 2019: range(632, 639) second page
        # for 2021: range(1293, 1298)
        # for index_page_num in range(1293, 1298):
        for index_page_num in range(0, len(pdf_reader.pages)):
            main_page_index = index_page_num+1
            
            # print(filename, '--ipn:', index_page_num, '--mpn:', main_page_index)

            # PyPDF2 starts with 0      
            page_text = pdf_reader.pages[index_page_num].extract_text()     
            # Split the text into lines
            lines = page_text.split('\n')        
            # Get the first line: *** PWSID = 6430001 | SYSTEM NAME = SCENIC MOBILE HOME PARK***
            first_line = lines[0]  

            print(filename, '(', main_page_index, ')', first_line)
            
            # print('page_text')
            # print(page_text)

            this_text1 = first_line.replace('***', '')
            this_text2 = this_text1.replace(' PWSID = ', '')
            this_text3 = this_text2.replace('SYSTEM NAME = ', '')
            split_text_to_list = this_text3.split(' |')
            sid = int(split_text_to_list[0])
            sn = split_text_to_list[1]
            split_list = sn.split(' ')
            sysname_page = ' ' + str(split_list[len(split_list)-1])
            sn = re.sub(sysname_page, '', sn)
            sysname_page = int(sysname_page)
            # print("sysname_page 1", sysname_page)

            # Try with TOP1, if works, great, else, try with TOP2
            good_to_go, dfhold, dfd, master_df, top, concat_it = make_df_of_years_sub(TOP1, dfhold, filename, main_page_index, sid, sn, sysname_page, master_df)
            if good_to_go is not True:
                good_to_go, dfhold, dfd, master_df, top, concat_it = make_df_of_years_sub(TOP2, dfhold, filename, main_page_index, sid, sn, sysname_page, master_df)           
                if good_to_go is not True:
                    x = input('\n\nDid you see this (There was an error. Did you already get a message? If not, there is an uncaught error.)?')
            
            if concat_it is True:
                # print('dfd columns', len(dfd.columns.tolist()), dfd.columns.tolist())
                # print('dfd ', top)
                # print(dfd)
                master_df = pd.concat([master_df, dfd], ignore_index=True)
                # print('master_df columns', len(master_df.columns.tolist()), master_df.columns.tolist())
                # print('master_df')
                # print(master_df)
                number_columns = len(master_df.columns.tolist())
                if number_columns != 16:
                    message = '\n\nThere were the wrong number of columns in master_df at this point?  There are ' + str(number_columns) + ' and there should be 16.'
                    print(master_df.columns.tolist())
                    x = input(message)


        try:
            file.close()
        except Exception as e:
            print('File was already closed:', e)

    return master_df


def make_df_of_years_sub(top, dfhold, filename, main_page_index, sid, sn, sysname_page, master_df):
    good_to_go = True
    dfd = pd.DataFrame()
    concat_it = False

    # print('TOP1:', TOP1, ' TOP2:', TOP2, '  top:', top)

    # Specify the area of the PDF page containing the table (coordinates are in PDF points - max is 612 x 792 for 8.5x11)
    # from the top, from the left, add the height of the table to the first, add the width of the table to the second)
    area_table = [top, 0, 612, 792]   
    # tabula starts with 1     
    df_list = tabula.read_pdf(filename, pages=main_page_index, area=area_table)
    # what is read goes to a list, but we just want the first one
    df = df_list[0]

    # print('df raw')
    # print(df)
    
    df.replace({'\r': ' '}, regex=True, inplace=True)
    df.replace({'\n': ' '}, regex=True, inplace=True)
    df.replace({'\t': '  '}, regex=True, inplace=True)
    df.replace({'  ': ' '}, regex=True, inplace=True)
    df.replace({' ': REPLACE_TEMP}, regex=True, inplace=True)

    file_headers = df.iloc[0]
    df.rename(columns = file_headers, inplace = True) 
    df = df[1:]
    df.reset_index(drop=True, inplace=True)
    dfz = df.dropna(how='all')
    dfd = dfz.copy() 

    df_column_header_list = dfd.columns.tolist()

    # print('df_column_header_list:', df_column_header_list)
    # print('dfd partially processed')
    # print(dfd)

    # special case encountered when the sample wrapped down 
    var_to_search = 'Sampl' + REPLACE_TEMP + 'e' + REPLACE_TEMP + 'Type'
    var_to_replace = 'Sample' + REPLACE_TEMP + 'Type'
    if var_to_search in df_column_header_list:
        dfd = dfd.rename(columns={var_to_search:var_to_replace})

    # special case where the line wraps are making multiple rows in the data frame
    if 'Sample' in df_column_header_list:
        if top == TOP1:
            good_to_go = False
        else:
            # top == TOP2:
            # have to walk through the rows and concat the split text 
            good_to_go = False

            print('Changing the top margin did NOT fix the header alignment issue. Deal with it. ')            
            print(dfd)
            x = input('Did you see this?')

            # first_line of page  536 : ***  PWSID = 6430049 | SYSTEM NAME = BUHL COMMUNITY WATER COMPANY*** 7
            # df_column_header_list: ['Sample', 'Contaminant', 'Analysis', 'In', 'Sample', 'Sample', 'Laboratory', 'Analysis', 'Analysis', 'Received']
            # dfd partially processed
            #       Sample Contaminant Analysis      In      Sample Sample Laboratory          Analysis    Analysis    Received
            # 0   Location          ID   Result  Effect        Date   Type         ID            Method        Date        Date
            # 1        101    CHLORINE     0.46       .  04/01/2015  ENTRY       BUHL  COLORMTRC,.~.DPD  04/01/2015  05/07/2015
            # 2        NaN         NaN      NaN     NaN         NaN  POINT  COMMUNITY        (CL/NH2CL)         NaN         NaN
            # 3        NaN         NaN      NaN     NaN         NaN    NaN      WATER               NaN         NaN         NaN
            # 4        NaN         NaN      NaN     NaN         NaN    NaN    COMPANY               NaN         NaN         NaN
            # 5        101    CHLORINE     0.53       .  04/02/2015  ENTRY       BUHL  COLORMTRC,.~.DPD  04/02/2015  05/07/2015
        

    if good_to_go is True:  

        what_condition = 0
        if CHECK_COLUMN1 in df_column_header_list and CHECK_COLUMN2 in df_column_header_list:
            # the whole table is in the dataframe, it is okay to merge into master df
            what_condition = 1
            # print(what_condition)
            dfd['SPLIT_PAGES'] = 1 
            dfd['PWSID'] = sid
            dfd['SYSTEM NAME'] = sn  
            dfd['MAIN_PAGE'] = main_page_index
            dfd['TOP_MARGIN'] = top 
            dfd['SYSNAME_PAGE'] = sysname_page
            concat_it = True
            dfhold = dfd    
        elif CHECK_COLUMN2 in df_column_header_list:
            # this is page two of a split page of columns
            what_condition = 3
            # print(what_condition)
            dfd_c = pd.concat([dfhold, dfd], axis=1)
            dfd_cT = dfd_c.T
            # print('dfd_cT', dfd_cT)
            # dfd_cTri = dfd_cT.reset_index()  #this had two index columns 
            # this works well to add a column that contains the column headers in the previous dataframe
            dfd_cTri = dfd_cT.assign(col_header=dfd_cT.index)
            # print('dfd_cTri', dfd_cTri)
            # drop the duplicates that there is a column that contains the col_header
            dfd_cTridd = dfd_cTri.T.drop_duplicates()            
            # print('dfd_cTridd', dfd_cTridd)
            # remove the row with the index "col_header"
            dfd_cTriddx = dfd_cTridd.drop(index='col_header')
            # print('dfd_cTriddx', dfd_cTriddx)
            # make sure got reset correctly after the delete
            dfd_cTriddx.reset_index()
            # print('dfd_cTriddix-a', dfd_cTriddx)
            # rename back to dfd
            dfd = dfd_cTriddx 
            # print('dfd', dfd.columns.tolist())
            # print(dfd)
            concat_it = True
            dfhold = dfd 
        elif CHECK_COLUMN1 in df_column_header_list:
            # this is page one of a split page of columns
            what_condition = 2  
            # print(what_condition) 
            dfd['SPLIT_PAGES'] = 2 
            dfd['PWSID'] = sid
            dfd['SYSTEM NAME'] = sn  
            dfd['MAIN_PAGE'] = main_page_index
            dfd['TOP_MARGIN'] = top     
            dfd['SYSNAME_PAGE'] = sysname_page
            concat_it = False
            dfhold = dfd  
        else:
            # in the 2014 file, on page 714 of the doc, there is carry over from the previous page
            # these are exceptions but have to deal with it            
            # if top is TOP1, we are going to skip the message and try TOP2
            if 'Sample' in df_column_header_list:
                if top == TOP2:
                    good_to_go = False
                    print('\nPAGE HEADER ERROR - Sample is in headers but was not resolved by changing the Top margin (', main_page_index, '):', dfd.columns.tolist(), '\n')
                    print('df_column_header_list:', df_column_header_list)
                    print('dfd')
                    print(dfd)                    
                    x = input('Did you see this?')
                else:
                    #  top is TOP1 and we are going to try TOP2 before deciding it is an error
                    good_to_go = False
            else:
                # these are edge cases where the table went to a second page WITHOUT headers
                # from looking at the data
                # when 6 or 7 fields were present, the carry over was from the first of 3 pages (appears to work with TOP1 - might have to edit if there is a word wrap issue discovered)
                # when 4 or 5 fields were present, the carry over was from the second of 3 pages (will not work with TOP1 due to word wrap)
                # yes, 3 pages, because only ONE of the split 2 acutally carried over
                len_col_list = len(df_column_header_list)
                if len_col_list == 6 or len_col_list == 7:
                    concat_it = False    
                    # specific case where len is 6
                    # df_column_header_list: [300, 'TOC', 4.2, '.', '07/23/2014', 'RAW.~.WATER']
                    # dfd
                    # 300  TOC  4.2  .  07/23/2014  RAW.~.WATER
                    # 0  300  TOC  4.2  .  07/23/2014  RAW.~.WATER
                    # assume what the column headers are from the previous page
                    assumed_column_headers = [
                        'Sample' + REPLACE_TEMP + 'Location',
                        'Contaminant' + REPLACE_TEMP + 'ID',
                        'Analysis' + REPLACE_TEMP + 'Result',
                        'MCL' + REPLACE_TEMP + 'In' + REPLACE_TEMP + 'Effect',
                        'Sample' + REPLACE_TEMP + 'Date',
                        'Sample' + REPLACE_TEMP + 'Type'
                    ]
                    if len_col_list == 7:
                        assumed_column_headers.append('Laboratory' + REPLACE_TEMP + 'ID')

                    stand_dict = {
                        'SPLIT_PAGES': 2, 
                        'PWSID': sid,
                        'SYSTEM NAME': sn,  
                        'MAIN_PAGE': main_page_index,
                        'TOP_MARGIN': top,
                        'SYSNAME_PAGE': sysname_page
                    }


                    # print('df_column_header_list:', dfhold.columns.tolist())
                    # print('dfhold')
                    # print(dfhold)     

                    # print('df_column_header_list:',  dfd.columns.tolist())
                    # print('dfd')
                    # print(dfd)       

                    
                    # remember that dfhold has the previous page's data in it (a first page in the split), so we want to append these carry over rows to it
                    # make the first row dictionary
                    this_dict = {}
                    for ind, each in enumerate(assumed_column_headers):
                        this_dict[each] = [df_column_header_list[ind]]      
                    next_dict = dict(list(this_dict.items()) + list(stand_dict.items()))      
                    df_temp = pd.DataFrame.from_dict(next_dict)
                    # add the row to the dfhold dataframe (which is part one of the two page split)
                    dfhold = pd.concat([dfhold, df_temp], ignore_index=True)

                    # do for each in the rows of the dataframe
                    for index, row in df_temp.iterrows():
                        row_as_list = row.tolist()
                        this_dict = {}
                        for ind, each in enumerate(assumed_column_headers):
                            this_dict[each] = [row_as_list[ind]]                       
                        next_dict = dict(list(this_dict.items()) + list(stand_dict.items()))  
                        df_temp = pd.DataFrame.from_dict(next_dict)
                        # add the row to the dfhold dataframe (which is part one of the two page split)
                        dfhold = pd.concat([dfhold, df_temp], ignore_index=True)  

                    # overwrite the dfd with the dfhold (that we just updated)
                    dfd = dfhold   

                    # print('df_column_header_list:', dfd.columns.tolist())
                    # print('dfd')
                    # print(dfd)                    

                elif len_col_list == 4 or len_col_list == 5:
                    concat_it = True
                    if top == TOP2:
                        # df_column_header_list: ['N', nan, nan, nan, nan]
                        #  dfd
                        #              N                                 NaN                               NaN         NaN         NaN
                        # 0  RAW.~.WATER  MICROBAC.~.LABORATORIES.~.-.~.ERIE  CHROMO/FLUOROGEN.~.(COLILERT/18)  05/19/2018  06/05/2018     
                        # When using tops 70, the wrapped line is not working
                        #  DISTRIBUTIO    MICROBAC LABORATORIES - ERIE CHROMO/FLUOROGEN (COLILERT/18) 05/17/2018 06/05/2018
                        #          N
                        # assume what the column headers are from the previous page (since coming on TOP2, need to use the headers)

                        # print('dfd')
                        # print(dfd)

                        assumed_column_headers = []
                        if len_col_list == 5:
                            assumed_column_headers = [
                                'Sample' + REPLACE_TEMP + 'Type',
                                'Laboratory' + REPLACE_TEMP + 'ID',
                                'Analysis' + REPLACE_TEMP + 'Method',
                                'Analysis' + REPLACE_TEMP + 'Date',
                                'Sample' + REPLACE_TEMP + 'Received' + REPLACE_TEMP + 'Date',
                            ]
                        else:
                            assumed_column_headers = assumed_column_headers[1:]
                        
                        len_of_master_df = len(master_df)
                        len_of_dfd = len(dfd)
                        index_of_row_in_master_df = len_of_master_df - (len_of_dfd + 1)

                        # print(' index_of_row_in_master_df',  index_of_row_in_master_df)

                        # make the first row dictionary
                        this_dict = {}
                        for ind, each in enumerate(assumed_column_headers):
                            this_dict[each] = [df_column_header_list[ind]]  

                        # print('master_df A')
                        # print(master_df)

                        for key, value in this_dict.items():  
                            new_value1 = value[0]
                            new_value2 = 0
                            try:
                                new_value2 = new_value1.replace('DISTRIBUTIO' + REPLACE_TEMP + 'N', 'DISTRIBUTION')
                            except Exception as e:
                                # print('That header is not in this dataframe:', e)
                                new_value2 = new_value1
                            master_df.at[index_of_row_in_master_df, key] = new_value2
                        master_df.at[index_of_row_in_master_df, 'TOP_MARGIN'] = top

                        # one per dictionary
                        index_of_row_in_master_df = index_of_row_in_master_df + 1

                        # do for each in the rows of the dataframe
                        df_temp = dfd
                        for index, row in df_temp.iterrows():
                            row_as_list = row.tolist()
                            this_dict = {}
                            for ind, each in enumerate(assumed_column_headers):
                                this_dict[each] = [row_as_list[ind]]                       
                            for key, value in this_dict.items():  
                                new_value1 = value[0]
                                new_value2 = 0
                                try:
                                    new_value2 = new_value1.replace('DISTRIBUTIO' + REPLACE_TEMP + 'N', 'DISTRIBUTION')
                                except Exception as e:
                                    # print('That header is not in this dataframe:', e)
                                    new_value2 = new_value1
                                master_df.at[index_of_row_in_master_df, key] = new_value2
                            master_df.at[index_of_row_in_master_df, 'TOP_MARGIN'] = top
                        # since replacing IN the master_df, we do not want to concat in the main program, so turn good_to_go to False 
                        good_to_go = True
                        concat_it = False

                        # print('master_df B')
                        # print(master_df)

                    else:
                        #this will not work for TOP1, so move on to try TOP2
                        good_to_go = False
                else:
                    good_to_go = False
                    print('\nPAGE HEADER ERROR - There is a page error not dealt with (', main_page_index, '):', dfd.columns.tolist(), '\n')
                    print('df_column_header_list:', df_column_header_list)
                    print('dfd')
                    print(dfd)                    
                    x = input('Did you see this?')
                

    if good_to_go is True:
        if what_condition in [1 ,3]:

            cols = dfd.columns.tolist()
            cols.sort()

            final_headers = HEADERS_EXPECTED + HEADERS_EXTRA
            final_headers.sort()

            if len(cols) != len(final_headers):
                good_to_go = False
                message = '\nSomething is wrong with the number of columns (' + str(main_page_index) + ')'
                print(message)
                print('top:', top)
                print('EXP headers:', final_headers)
                print('ACT headers:', cols)
                print(dfd)                
                x = input('Did you see this?')
            elif final_headers != cols:
                good_to_go = False
                message = '\nThe headers are not as expected (' + str(main_page_index) + ')'
                print(message)
                print('top:', top)
                print('EXP headers:', final_headers)
                print('ACT headers:', cols)
                print(dfd)                
                x = input('Did you see this?')

        # print(good_to_go)
        # print(dfd)
    
    # print('returning good_to_go:', good_to_go)
    # print('\ndfd')
    # print(dfd)  
    # print('\ndfhold') 
    # print(dfhold) 
    return good_to_go, dfhold, dfd, master_df, top, concat_it


if __name__ == "__main__":
    args = sys.argv
    main(args)
