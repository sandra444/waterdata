# To run:
# python direct_water_prep.py
# Note: for simplicity, put the script in the same directory as the data file

'''
Background:
This script takes data in two tabs of a spreadsheet obtained from a representative at the state and joins the system info with the data.
First, a strict join (plant plus location) was made. If a match could not be found, a loose join was made (plant only).
If the join still could not be made, data rows were omitted from the final data file.

Data Obtained from a Water Program Specialist | PADWIS Section
Department of Environmental Protection | Bureau of Safe Drinking Water
400 Market Street | Harrisburg PA 17101
via email on Fri, Mar 29, 11:35 AM

Attached information:
The first tab includes all community and non-transient non-community water systems in Butler & Mercer counties along with their entry point and sample point information.  Please be aware that the entry points and sample points may not be linked correctly, so please don’t assume that a particular entry point goes to a particular sample point just because they are listed on the same line of the spreadsheet.

The second tab contains violations for these systems for the past 8 years.   

The third tab contains analytical data for these systems for the past 8 years.

Subsequent information (via email Mon, Apr 22, 8:41 AM):
Trust the system info tab as far as which PWS are in the counties requested (Butler and Mercer).  
More restrictions were added when the data set was pulled.  
PWSIDs in the data and/or violations tab (that are not in the system info tab) may indicate systems that are no longer active.

As far as joining the data:
The field loc_epid in the sample data could be ANY type of location ID, whereas epid is only for entry points.  
The system info tab only contains the entry point and distribution sample point information.

So, the data can be linked to the system info as follows:
If the sample type is E, then loc_epid = epid
If the sample type is D, then loc_epid = spid
Other than those two specific associations, the data would have to be considered on a case by case basis . . .

Subsequent information (via email Wed, May 1, 6:51 AM):
Our system does not track concentration units, so instead our reporting instructions tell the user how to report.  
ALL data are reported in mg/L with the following exceptions:
PFAS data are reported in ng/L
Radiological data are reported in pCi/L

Approach to preparing the data is indicated in the program print statements in the code.

TODO -> IMPORTANT NOTE: IF DECIDE TO USE THIS IN MAPPING, NEED TO:
1) Open the info in the System Info tab in sheets and get the lat and long (https://www.youtube.com/watch?v=PX6IDvX6_Z8)
--open in Google Sheets (get the addresses in ONE column)
--Add-ons, Geocode Cells (alternative is eZGeocode), install it
--Select all rows in the column, Add-ons, Geocode Cells, Add Columns
2) Rename fields to Latitude and Longitude

'''

import re
import copy
import sys
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

DATA_FILE = 'WaterDataPA2024.xlsx'
DATA_FILE_DIRECT = 'direct_mercer_butler_water_data.csv' 


def main(args):
    # script_file = args[0]

    print('Read Excel file with multiple sheets')
    xls = pd.read_excel(DATA_FILE, sheet_name=['System Info', 'Violations', 'Data'])

    print('Accessing individual sheets using sheet names')
    system_info_df = xls['System Info']
    violations_df = xls['Violations']
    data_df = xls['Data']

    # check to make sure that there are no unexpected numbers in the joining field
    # EPID should be between 100 and 199
    # SPID should be between 700 and 999

    print('Changing EPIC and SPID to integers')
    system_info_df['EPID'] = system_info_df['EPID'].astype(int)
    system_info_df['SPID'] = system_info_df['SPID'].astype(int)

    print('Checking for EPID range')
    null_df = system_info_df[(system_info_df['EPID'] < 100) | (system_info_df['EPID'] > 199)]
    dfs1 = null_df.shape[0]

    print('Checking for SPID range')
    notnull_df = system_info_df[(system_info_df['SPID'] < 700) | (system_info_df['EPID'] > 999)]
    dfs2 = notnull_df.shape[0]

    print('Printing error messages - if found')
    continue_if_true = True
    if dfs1 > 0:
        print('EPID should be between 100 and 199 - some are out of range')
        print(dfs1)
        continue_if_true = False
    if dfs2 > 0:
        print('SPID should be between 700 and 999 - some are out of range')
        print(dfs2)
        continue_if_true = False
         
    # Continuing if true
    if continue_if_true is True:
        print('No errors found so far - The EPID and SPID are in expected ranges and there is no overlap')
        
        # to make the join easier, duplicate the rows in the system_info_df, with the new field that we will link to the LOC_EPID field
        print('Adding a field to system info')
        system_info_df['LOC_EPID'] = system_info_df['EPID'] 
        
        print('Copying the data frame')
        copy_system_info_df = system_info_df.copy() 
        
        print('Replacing new field content in copied data frame')
        copy_system_info_df['LOC_EPID'] = copy_system_info_df['SPID'] 
        
        print('Concatenating two dataframes back into one')
        combined_system_info_df = pd.concat([system_info_df, copy_system_info_df])
        combined_system_info_df.reset_index()
        # combined_system_info_df.to_csv('a1_combined_system_info_df.csv', index=False)
        
        print('Get the subset of fields we need')
        columns_list = combined_system_info_df.columns.tolist()
        
        print('combined_system_info_df columns:', columns_list)
        sysinfo_subset = ['PWSID', 'SYSTYPE', 'LOC_EPID', 'SYSNAME', 'POPL', 'AREACITY', 'SYSOWNAM', 'MAIL_ADDR1', 'MAIL_ADDR2', 'SYSLOCCY', 'MAIL_ZIP']
        
        print('Remove dups from the combined system info and than save to csv for review')
        combined_system_info_subset_fields_df = combined_system_info_df[sysinfo_subset]
        combined_system_info_subset_fields_nd_df = combined_system_info_subset_fields_df.drop_duplicates(subset=sysinfo_subset)  
        combined_system_info_subset_fields_nd_df.reset_index()
        # combined_system_info_subset_fields_nd_df.to_csv('a2_combined_system_info_subset_fields_nd_df.csv', index=False)   
        
        # We want to join in two ways (PWSID + LOC_EPID, then if not found PWSID), we will do it with dataframes
        print('Turn LOC_EPID in system info to a string to prep for merging with data')
        combined_system_info_subset_fields_nd_df2 = combined_system_info_subset_fields_nd_df.copy()
        combined_system_info_subset_fields_nd_df2['LOC_EPID'] = combined_system_info_subset_fields_nd_df2['LOC_EPID'].astype(str)
        
        print('Merge data with system info and send to csv for review')
        strict_merge_data_df = pd.merge(data_df, combined_system_info_subset_fields_nd_df2, on=['PWSID', 'LOC_EPID'], how='left')  
        # strict_merge_data_df.to_csv('a3_mercer_butler_water_data_strict_join.csv', index=False)  

        print('Prepare to join based on PWSID only (loose)')
        print('Get a dataframe that has one system info row for each PWSID - just getting any one')
        combined_system_info_subset_fields_nd_by_pwsid_df = combined_system_info_subset_fields_nd_df2.drop_duplicates(subset=['PWSID'])  
        # combined_system_info_subset_fields_nd_by_pwsid_df.to_csv('a4_combined_system_info_subset_fields_nd_by_pwsid_df.csv', index=False)  

        print('Split the data dataframe into two - those with address and those without')   
        null_df =  strict_merge_data_df[(strict_merge_data_df['MAIL_ZIP'].isnull())]
        notnull_df =  strict_merge_data_df[(strict_merge_data_df['MAIL_ZIP'].notnull())]
        print('Remove all the previously merged null fields so they do not duplicate in the next merge')
        data_columns_except1 = ['PWSID', 'CONTAMID', 'CONTNAM', 'RESULT', 'SAMPTYPE', 'SAMPDATE', 'SAMPTIME', 'ANALDATE']
        null_subset_fields_df = null_df[data_columns_except1]

        print('Fill the null address in the sub dataframe')
        null_subset_fields_df_f = pd.merge(null_subset_fields_df, combined_system_info_subset_fields_nd_by_pwsid_df, on=['PWSID'], how='left')  
        
        print('Put the two sub dataframes back together in a union')
        notnull_df.to_csv('direct_strict_data_notnull_df.csv', index=False)  
        null_subset_fields_df_f.to_csv('direct_strict_data_null_subset_fields_df_f.csv', index=False)  

        almost_final_df = pd.concat([null_subset_fields_df_f, notnull_df])

        print('Get rid of data rows that did not have a match to the system info (they will not display on the map)')
        final_df =  almost_final_df[(almost_final_df['MAIL_ZIP'].notnull())]
        final_df.reset_index()
        final_df.to_csv(DATA_FILE_DIRECT, index=False)  

        print('DONE') 
    else:
        print('There was a problem. You should have received a message already with more details.')
    return 'done'


if __name__ == "__main__":
    args = sys.argv
    main(args)
