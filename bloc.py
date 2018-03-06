"""
"""
import sys
from caeser import utils
from config import cnx_params
import pandas as pd
import os
from collections import OrderedDict
from datetime import datetime
import argparse
import warnings

warnings.filterwarnings('ignore')
os.chdir('/home/nate/dropbox-caeser/Data/MIDT/bloc_squad/mpd')
engine = utils.connect(**cnx_params.blight)

def load_data(f):
    """
    Args:
        f (str): file name with extension to be loaded
    """
    os.chdir('/home/nate/dropbox-caeser/Data/MIDT/bloc_squad/mpd')
    col_offense = ['caseno', 'incident_date', 'dow', 'report_date', 'address',
                    'ward', 'offense', 'dept_code', 'block', 'street_address', 
                    'ucr_code', 'weapon_code', 'weapon_desc', 'case_status', 
                    'jurisdiction']
    col_victim = ['caseno', 'involvement_type', 'bus_apt', 'name_last', 
                    'name_first', 'name_mid', 'sex', 'race', 'age', 'moniker',
                    'ethnicity', 'address', 'relation_to_offender', 'verbal',
                    'pushed', 'hit', 'kicked', 'struck', 'choked', 
                    'stabbed_cut','shot', 'treatment', 'med_transport']
    col_suspect = ['caseno', 'involvement_type', 'apt', 'name_last', 
                    'name_first', 'name_middle', 'sex', 'race', 'age', 
                    'moniker', 'ethnicity', 'address']
        
    #dict contains parameters for loading worksheet into panda dataframe
    #dict key contains sheet name, val[0] contains list of column names,
    #val[1] contains list of indices for columns to be loaded or skipped 
    tables = OrderedDict([('Offenses', [col_offense, 
                                None]),
                          ('Victims', [col_victim, 
                                [i for i in range(37) if i < 1 or i > 14]]),
                          ('Suspects', [col_suspect,
                                [i for i in range(26) if i < 1 or i > 14]])])
    xls = pd.ExcelFile(f)
    sheets = xls.sheet_names
    for k, v in tables.items():
        if k not in sheets:
            break
        df = pd.read_excel(f, k, names=v[0], usecols=v[1])
        df['city'] = 'Memphis'
        df['state'] = 'TN'
        df['id'] = df.index
        if k != 'Offenses':
            #remove '1-6 days old' values
            mask_sub0 = df.age.str.contains('days', na=False)
            df.loc[mask_sub0, 'age'] = '0'
            mask_over98 = df.age.str.contains('98 years old', na=False)
            df.loc[mask_over98, 'age'] = '98'
            mask_unknown = df.age.str.isnumeric() == False
            df.loc[mask_unknown, 'age'] = pd.np.nan
            #df.age.replace('Unknown', pd.np.nan, inplace=True)
            #df[df.age.str.contains('days') == True].age = '0'
            #df.age = df.age.str.lower().replace('unknown', None)
            df.age = pd.to_numeric(df.age).fillna(999).astype(int)
        match, unmatch = utils.geocode(df, index='id')
        for i in range(1,3):
            clean = df[df.id.isin(unmatch.ResultID)].copy()
            clean.address = clean.address.apply(
                    utils.clean_address,args=(i,))
            rematch, unrematch = utils.geocode(clean, index='id')
            match = match.append(rematch, ignore_index=True)
            unmatch = unrematch.copy()
            del rematch, unrematch
        #append all records to Victims and Suspects tables
        #modified to load all records for all worksheets
        #if k != 'Offenses':
        match_rate = match.shape[0]/float(df.shape[0])
        print "Match rate for {0}: {1}".format(k, str(match_rate))
        match = match.append(unmatch, ignore_index=True)
        rec_check = "Match count: {0}, Original count: {1}"
        print rec_check.format(str(match.shape[0]), str(df.shape[0]))
        f_out = f.split('.')[-2].replace('/','') + '_' + k.lower()
        msng_output = './unmatched_addresses/unlocated_{}.csv'.format(f_out)
        df[df.index.isin(unmatch.ResultID)].to_csv(msng_output, 
                index=True, encoding='utf-8')
        #drop unmatched records and add lat/lon fields
        #match.set_index('ResultID', inplace=True)
        match.rename(index=str, columns={'ResultID':'id'}, inplace=True)
        df = df.merge(match[['lat','lon', 'id']],how='right', on='id')
        pt = "SRID=2274;POINT({0} {1})"
        geom = lambda x: pt.format(x['lon'], x['lat']) \
                if x['lon'] != 'NaN' else None
        df['wkb_geometry'] = df[['lon', 'lat']].apply(geom, axis=1) 
        df.drop(['lat', 'lon'], axis=1, inplace=True)
        df.to_sql('im_p1_violent_crime_{}'.format(k.lower()), engine,
                if_exists='append')

def pull_offenses():

    query = ("select o.caseno, o.incident_date, o.dow, o.report_date, "
                    "o.address, o.ward, o.offense, o.dept_code, o.block," 
                    "o.street_address, o.ucr_code, o.weapon_code, "
                    "o.weapon_desc, o.case_status, o.jurisdiction, "
                    "case "
                    "    when age <= 24 then 'Y' "
                    "    else 'N' "
                    "end as youth, "
                    "extract (quarter from incident_date)::integer quarter, "
                    "case " 
                    "    when st_intersects(o.wkb_geometry, b.wkb_geometry) "
                    "then name "
                    "    else 'Memphis' "
                    "end focal_area "
                "from im_p1_violent_crime_offenses o "
                "left join "
                "    (select caseno, min(age) age "
                "        from im_p1_violent_crime_suspects group by caseno) s "
                "    on s.caseno = o.caseno "
                "left join "
                "    (select name, wkb_geometry "
                "        from geography.boundaries where origin = 'IM') b "
                "     on st_intersects(o.wkb_geometry, b.wkb_geometry) ")

    new_header = ['Case #', 'Incident Date/Time', 'DOW', 'Report Date', 
                 'Address', 'Ward', 'Offense', 'Dept_Code', 'Block', 
                 'Street_Name', 'UCR_Code', 'Weapon_Code', 
                 'Weapon_Description', 'Case_Status', 'Jurisdiction', 'Youth', 
                 'Quarter', 'Focal Area']
    today = datetime.today()
    date_format = today.strftime('%d%m%Y')
    df = pd.read_sql(query, engine)
    df.to_csv('./exports/bloc_squad_update_{}.csv'.format(date_format), 
            index=False, header=new_header, encoding='utf-8')

if __name__ == "__main__":
    
    desc = ("Process and add new mpd records to 901 bloc squad tables")
    parser = argparse.ArgumentParser(description=desc,
            prefix_chars='-', add_help=True)
    parser.add_argument('-i', '--import', action='store', dest='i',
            help=("Import records from spreadsheet."
                "Specify file name with extension."))
    parser.add_argument('-e', '--export', action='store_true', dest='e',
            help="Export full dump of MPD offense table, no parameter required")
    args = parser.parse_args()
    if args.i:
        load_data(args.i)
    if args.e:
        pull_offenses()

