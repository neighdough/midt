#!/usr/bin/env python

"""
Methods for maintaining and updating the Memphis Property Hub

Usage:
    prop_hub (code_enforcement <path> | mlgw <file_name> <path_name> | land_bank | 
              register [--startdate=<YYYYmmdd> --enddate=<YYYYmmdd> <instype> ...] | 
              assessor [--load <year> [--skip <columns>...]| --update] | com_tax <date>)

Options:

    code_enforcement: load Code enforcement request data
        <path>: full path of where new data resides

    mlgw: load new MLGW disonnection data
        <file_name>: name of the csv-like file to be loaded

    land_bank: pull data from land bank website, load into property hub
        and then update combined table
    
    register: scrape data from Shelby County Register's database and load
        into property hub database.
        -s --startdate <YYYYmmdd>: start date in format YYYYmmdd
        -e --enddate <YYYYmmdd>: end date of request in format YYYYmmdd
        <instype> (optional): type of instrument to be pulled, default is "ALL"
            other possible entries include:
                'WD', 'QC', 'MTG', 'PAR', 'ASRL', 'REL', 'MISC', 'MOD', 'ASGN',
                'CRTD', 'STR', 'AFFH', 'LIEN', 'LEAS', 'POA', 'RFS', 'APPT', 'COF',
                'NOC', 'SUBA', 'CONTR', 'RFSC', 'ESMT', 'RFSAS', 'AMD', 'TXPRL',
                'SRES', 'RFST', 'RFSP', 'ASMP', 'RFSAM', 'None'

    assessor: Load new assessor data intot blight_data database or update combined_table
        with new assessor data.
        --load <year>: replace existing assessor tables with new data for specified year
        --skip <columns>: list of tables to skip
        --update: update combined table with previously loaded assessor data

    com_tax: load new data for City of Memphis tax delinquency
        <date>: date in mmddyyyy format


"""
__author__ = 'Nate Ron-Ferguson'
__copyright__ = """Copyright (c) 2016
                Center for Applied Earth Science and Engineering Research,
                University of Memphis"""
__license__= 'GNU GPL V3'

from bs4 import BeautifulSoup
from collections import OrderedDict
from datetime import datetime, date
import getopt
import json
from sqlalchemy import (create_engine, MetaData, Table, Column, 
                        Sequence, text, exc)
from sqlalchemy.types import (BIGINT, TEXT, DATE, FLOAT) 
import geoalchemy2
from geoalchemy2 import Geometry
import numpy as np
import pandas as pd
import re
import requests
from shapely.geometry import Point
from subprocess import call
import sys
from caeser import utils
from config import cnx_params
import os
import subprocess
import StringIO
import shutil
from docopt import docopt
import warnings
warnings.filterwarnings("ignore")
warnings.simplefilter("ignore", category=exc.SAWarning)

engine = utils.connect(**cnx_params.blight)
conn = engine.connect()
meta = MetaData(schema='public')
meta.reflect(bind=engine)

def update_register(startdate, enddate='', instype=None):
    """This function performs the bulk of the work in extracting recent sales
    from the Shelby County Register's Recent and Comparable search page. While
    their search allows users to extract a variety of information, this function
    only extracts recent and comparable sales data. It passes in the instrument
    type and date and then pulls a csv from their download application and
    places it into a panda dataframe for further manipulationa and loading
    into a PostgreSQL database (this feature may be missing).

    Args:
        startdate (string): date string in format YYYYmmdd
        enddate (string, optional): date string in format YYYYmmdd
        instype (list of strings, optional): Register instrument code for type of
            instruments to be returned with a maximum of 3 values. Default value 
            is 'ALL' acceptable values include:
                'WD', 'QC', 'MTG', 'PAR', 'ASRL', 'REL', 'MISC', 'MOD', 'ASGN',
                'CRTD', 'STR', 'AFFH', 'LIEN', 'LEAS', 'POA', 'RFS', 'APPT', 'COF',
                'NOC', 'SUBA', 'CONTR', 'RFSC', 'ESMT', 'RFSAS', 'AMD', 'TXPRL',
                'SRES', 'RFST', 'RFSP', 'ASMP', 'RFSAM', 'None'
    Returns:
        Panda Dataframe
    """
    payload = {'startDate': startdate,
               'endDate': enddate,
               'searchtype': 'ADDR',
               'search.x': '28',
               'search.y': '3',
               'search': 'execute search'}

    if instype:
        if len(instype) > 3:
            print(("You have provided more than 3 instrument types. Please limit your "
                    "query to 3 or less, or delete this parameter to execute a query "
                    "for all instrument types."))
            return 
        
        for i in range(len(instype)):
            #REST parameters for instrument type is itype2, itype3, itype4 so you need
            #to iterate over the list (if it exists) and add an instrument type key
            #for each value provided with a maximum of 3
            k = "itype" + str(i + 2)
            payload[k] = instype[i]
    else:
        payload["itype2"] = "ALL"
    
    session = requests.Session()

    req = requests.post('http://register.shelby.tn.us/p2.php', data=payload)

    soup = BeautifulSoup(req.text, 'lxml')

    #extract all javascript text to locate the sesnum
    s = soup.find_all('script', {'type':'text/javascript'})
    st = r'\"(.+?)\"'
    sesnum = ''
    for child in s[1].children:
        m = re.search(st, child)
        url = m.group().replace('"','')
        sesnum = url.split('sesnum=')[1]

    #parameters for csv get service
    csvparams = OrderedDict([('searchtype', 'ADDR'),
                 ('straddr', 'NONE ENTERED'),
                 ('city', 'NONE ENTERED'),
                 ('zip', 'NONE ENTERED'),
                 ('startDate', startdate),
                 ('endDate',enddate),
                 ('insttypeaddr',instype),
                 ('insttypeaddr2',''),
                 ('insttypeaddr3',''),
                 ('subname', 'NONE ENTERED'),
                 ('sesnum',sesnum),
                 ('fileType', 'csv')])


    csvreq = requests.get('http://register.shelby.tn.us/csvdownload.php',
                params=csvparams, cookies={'PHPSESSID': 
                                            req.cookies['PHPSESSID']})
    #column names and datatypes for final panda dataframe
    cols = OrderedDict([('instno',np.str),
            ('date',datetime),
            ('grantor',np.str),
            ('grantee',np.str),
            ('instype',np.str),
            ('transamt',np.float),
            ('mortgage',np.float),
            ('strno', np.str),
            ('strname',np.str),
            ('city',np.str),
            ('state',np.str),
            ('zip',np.str)])
    #extract text from get response and place into list to load into dataframe
    tbl = [[r.replace('"','').strip() for r in row.split(',')] for row in\
            csvreq.text.split('\n')]
    tbl_df = pd.DataFrame.from_records(tbl, columns=cols.keys())
    #modify column data types for compatibility with final database
    for column in tbl_df.columns:
        tbl_df[column] = tbl_df[column].astype(cols[column])
    
    tbl_df.to_sql('caeser_register', engine, if_exists='append')
    update_metadata(False, "caeser_register")
    #return tbl_df

# def main(argv):
    # """Main method for scraping website set up to run via command line. Command
    # should be run in format:

   # $python register.py -s <Start Date> -e <End Date (optional)> -t <Instrument type (optional)>

    # """
    # start = ''
    # end = ''
    # type = ''
    # opts, args = getopt.getopt(argv, 'h:s:t:', ['startdate=', 'enddate=',
                                              # 'instype='])
    # for opt, arg in opts:
        # if opt == '-h':
            # print 'register.py -s <start> -e <end> -t <type>'
            # sys.exit()
        # elif opt in ('-s', '--startdate'):
            # start = arg
        # elif opt in ('-e', '--enddate'):
            # end = arg
        # elif opt in ('-t', '--instype'):
            # type = arg
    

def get(layer, fields=None, where="1=1", service=None):

    ms = ('https://testuasiportal.shelbycountytn.gov/arcgis/rest/services/'\
            'regis/blight_data_project_2016/MapServer') if not service else \
            service
    req_ms = requests.post(ms, {'f':'json'})
    j_ms = req_ms.json()
    max_rc = j_ms['maxRecordCount']
  
    features = []
    fields_str = '*' if not fields else ', '.join(field for field in fields)
    params = {'f': 'json',
                  'outFields': fields_str,
                 'where': where}
    #NaN geometry in record 261
    while True:
        req_layer = requests.post(ms+'/'+layer+'/query', params)
        j = req_layer.json()
        for record in j['features']:
            if record['geometry']['x'] != 'NaN':
                d = record['attributes']
                p = record['geometry']
                d['wkb_geometry'] = 'SRID=2274;Point({0} {1})'.format(p['x'],
                                                                      p['y'])
                for k in d.keys():
                    d[k.lower()] = d.pop(k)
                features.append(d)
        if 'exceededTransferLimit' in j.keys():
            objectid = features[-1]['OBJECTID']
            where_nxt = ' AND OBJECTID > {}'.format(objectid) 
            get(layer, fields, where + where_nxt)
        else:
            break
    return features

def land_bank():
    """Extracts new landbank records from Shelby County service, loads them
    into caeser_register table in blight_data database, and then updates
    combined_table to reflect new landbank properties.

    Args:
        None

    Returns:
        None
    """
    table_name = 'caeser_landbank'
    service=('https://uasiportal.shelbycountytn.gov/arcgis/rest/services/'\
            'LandBank/LandBank/MapServer')
    lb = get('0',service=service)
    if not table_name in [k.split('.')[-1] for k in meta.tables.keys()]:
        table = Table(table_name, meta,
                        Column('objectid', BIGINT, primary_key=True),
                        Column('taxparcelidno', TEXT),
                        Column('parcelidno', TEXT),
                        Column('streetno', TEXT),
                        Column('streetname', TEXT),
                        Column('dimwidth', BIGINT),
                        Column('dimdepth', BIGINT),
                        Column('dimconfig', TEXT),
                        Column('zipcode', TEXT),
                        Column('sizeinacres', FLOAT),
                        Column('improvement', TEXT),
                        Column('zoning', TEXT),
                        Column('taxsaleno', TEXT),
                        Column('askingprice', FLOAT),
                        Column('latitude', FLOAT),
                        Column('longitude', FLOAT),
                        Column('createdate', DATE),
                        Column('status_1', TEXT),
                        Column('lastdaytobid', DATE),
                        Column('wkb_geometry', Geometry(geometry_type='Point',
                                                srid=2274)))
        table.create(engine)
    else:
        table = Table(table_name, meta)
    #convert time fields from epoch milliseconds to actual date    
    def convert(t):
        import time
        
        if t:
            ct = time.localtime(t/1000.)
            yr, mon, day = ct[:3]
            date_string = '{0}/{1}/{2}'.format(mon, day, yr)
            #date_obj = datetime.date(yr, mon, day)
            return date_string#(date_string, date_obj)
        else:
            return None#(None,None)

    #convert date string to date object
    for row in lb:
        dates = convert(row['createdate'])
        row['createdate'] = dates#[1]
        row['lastdaytobid'] = convert(row['lastdaytobid'])#[1]
    
    current_lb = pd.read_sql("""select * from caeser_landbank""", engine)
    new_lb = pd.DataFrame(lb)
    today = datetime.today()
    new_lb['load_date'] = '{0}-{1}-{2}'.format(today.year, 
                                                today.month,today.day)

    #append new landbank records to caeser_landbank table
    new_lb.to_sql('caeser_landbank', engine, if_exists='append', index=False)
    
    #Update combined_table to refelct new records from landbank
    clean_lb = """update combined_table 
                        set improvement = Null,
                            taxsale = Null,
                            askingprice = Null,
                            acres = Null,
                            load_date = current_date
                    where taxsale is not Null"""
    conn.execute(clean_lb)
    update_lb = """update combined_table
                    set improvement = lb.imp,
                        taxsale = lb.tax,
                        askingprice = lb.price,
                        acres = lb.acres,
                        load_date = lb.today
                    from (select parcelidno parid, improvement imp, taxsaleno tax, 
                            askingprice price, sizeinacres acres,
                            load_date today
                        from caeser_landbank where load_date = current_date) lb
                    where combined_table.parid = lb.parid;"""
    conn.execute(update_lb)
    update_metadata(False, "caeser_landbank")

def permits():
    """
    TODO:
        modify to load new records
    """
    sql_const = ("update combined_table set "
                    "const_type = upd.const_type, "
                    "const_desc = upd.descriptio, "
                    "last_const_permit_date = upd.issued, "
                    "const_permit = upd.permit, "
                    "sq_ft = upd.sq_ft, "
                    "valuation = upd.valuation, " 
                    "num_const_permits = upd.num_const_permits, "
                    "load_date = current_date "
                 "from (select agg.parid, const_type, descriptio, issued, "
                          "permit, sq_ft, valuation, num_const_permits, "
                            "current_date "
                          "from (select parid, count(parid) num_const_permits "
                                "from permits group by parid, const_type) agg "
                 "join (select distinct on (parid) parid, issued, valuation, "
                            "const_type, permit, descriptio, sq_ft "
                        "from permits where const_type = 'new' "
                        "order by parid, issued desc) recent "
                 "on agg.parid = recent.parid) upd "
                 "where combined_table.parid = upd.parid")
    conn.execute(sql_const)
    
    sql_demo = ("update combined_table set"
                   "date = issued, "
                   "permit_des = descriptio, "
                   "type = upd.const_type, "
                   "load_date = current_date "
                 "from (select parid, descriptio, max(issued) issued, const_type "
                        "from permits where const_type = 'demo' "
                        "group by parid, descriptio, const_type) upd "
                        "where combined_table.parid = upd.parid")
    conn.execute(sql_demo)


def demo_permits():
    fields = ['ADDRESS', 'CITYT', 'ZIP', 'DATE', 'DESCRIPTIO', 'LANDUSE',
              'OBJECTID', 'PERMIT', 'PERMIT_DES', 'TYPE'] 
    layer = '1'
    demo = get(layer, fields)
    table_name = 'sc_demo_permits'
    if not table_name in [k.split('.')[-1] for k in meta.tables.keys()]:
        table = Table(table_name,meta,
                    Column('index', BIGINT, Sequence('sc_demo_permits_seq'),
                        primary_key=True),
                    Column('address', TEXT),
                    Column('cityt', TEXT),
                    Column('date', FLOAT),
                    Column('descriptio', TEXT),
                    Column('landuse', TEXT),
                    Column('objectid', BIGINT),
                    Column('permit', TEXT),
                    Column('permit_des', TEXT),
                    Column('type', TEXT),
                    Column('zip', FLOAT),
                    Column('wkb_geometry', Geometry(geometry_type='Point', 
                        srid=2274)))
        table.create(engine)
    else:
        table = Table(table_name, meta)
    load(table, demo)


def const_permits():
    fields = ['Match_addr', 'Att_Acc', 'Att_Port', 'BATHROOMS', 'BEDROOMS',
              'Balcony', 'CONST_TYPE', 'Corner_SB', 'Cu_Ft', 'DESCRIPTIO',
              'Det_Acc', 'Det_Port', 'F1st_Fl', 'F2nd_Fl', 'FY_SB', 'Fire_I',
              'Fire_O', 'Fireplace', 'Fl_Load', 'Fraction', 'HOUSE_CNT',
              'Health_Dep', 'Height', 'Issued', 'LICENSE_NO', 'Lot', 'Map_Pg',
              'NAME', 'Num_Floors', 'Other', 'PRIMARY_NA', 'Permit', 'Phone',
              'RELATIONSH', 'RY_SB', 'SUB_TYPE', 'SY_SB', 'SY_SB2',
              'SPRINKLED', 'Sq_Ft', 'Subdivisio', 'Valuation', 'Zone',
              'OBJECTID']
    table_name = 'sc_const_permits'
    layer = '2'
    permits = get(layer, fields)
    if not table_name in [k.split('.')[-1] for k in meta.tables.keys()]:
        table = Table('sc_const_permits',meta,
                    Column('index', BIGINT, Sequence('sc_const_permits_seq'),
                        primary_key=True),
                    Column('att_acc', BIGINT),
                    Column('att_port', BIGINT),
                    Column('balcony', BIGINT),
                    Column('bathrooms', FLOAT),
                    Column('bedrooms', BIGINT),
                    Column('const_type', TEXT),
                    Column('corner_sb', FLOAT),
                    Column('cu_ft', BIGINT),
                    Column('descriptio', TEXT),
                    Column('det_acc', BIGINT),
                    Column('det_port', BIGINT),
                    Column('f1st_fl', BIGINT),
                    Column('f2nd_fl', BIGINT),
                    Column('fire_i', TEXT),
                    Column('fire_o', TEXT),
                    Column('fireplace', BIGINT),
                    Column('fl_load', BIGINT),
                    Column('fraction', TEXT),
                    Column('fy_sb', FLOAT),
                    Column('health_dep', TEXT),
                    Column('height', TEXT),
                    Column('house_cnt', BIGINT),
                    Column('issued', TEXT),
                    Column('license_no', TEXT),
                    Column('lot', TEXT),
                    Column('map_pg', TEXT),
                    Column('match_addr', TEXT),
                    Column('name', TEXT),
                    Column('num_floors', BIGINT),
                    Column('objectid', BIGINT),
                    Column('other', BIGINT),
                    Column('permit', TEXT),
                    Column('phone', TEXT),
                    Column('primary_na', BIGINT),
                    Column('relationsh', TEXT),
                    Column('ry_sb', FLOAT),
                    Column('sprinkled', TEXT),
                    Column('sq_ft', BIGINT),
                    Column('sub_type', TEXT),
                    Column('subdivisio', TEXT),
                    Column('sy_sb', FLOAT),
                    Column('sy_sb2', FLOAT),
                    Column('valuation', FLOAT),
                    Column('wkb_geometry', Geometry(geometry_type='Point',
                        srid=2274)),
                    Column('zone', TEXT))
        table.create(engine)
    else:
        table = Table(table_name, meta)
    
    load(table, permits)

def load(sql_table, input_list):
    conn.execute(sql_table.insert(input_list))

def com_tax(date):
    import datetime
    """Update method that loads new tax data into com_tax table in blight_data
    database updates sum amount for the combined_table
    
    Args:
        date (string): 8-digit string for date in format of mmddyyyy as 
        recorded in the file name in main data directory for project. 
        e.g. 01232017from tax_delinquency_01232017.
    Returns:
        None   
    """
    os.chdir('/home/nate/dropbox-caeser/Data/MIDT/Data_Warehouse/com_trustee')
    tax_codes = {'R': 'Real Estate',
                 'P': 'Personal Property',
                 '2': 'CBID',
                 'W': 'Weed Cutting',
                 'A': 'Sanitation',
                 'D': 'Demolition',
                 'I': 'Anti-neglect'}
    tax_cols = OrderedDict([('yr', np.int),
			     ('parcelid', np.str),
			     ('tax_type', np.str),
			     ('tax_blight', np.str),
			     ('orig_amt', np.float),
			     ('late_fees', np.float),
                             ('paid', np.float),
                             ('net_due', np.float),
                             ('load_date', np.str)])  
    
    df_tax = pd.read_csv('tax_delinquent_'+date, header=0, names=tax_cols.keys(),
	    dtype=tax_cols)

    df_tax['tax_type'].replace(tax_codes, inplace=True)
    df_tax['load_date'] = datetime.date.today()
    df_tax.to_sql('com_tax', engine, if_exists='append')
    update_metadata(False, "com_tax")

    #remove current city tax information
    clean_tax = ("update combined_table "
                    "set yr = NULL,"
                      "net_due = NULL,"
                      "late_fees = NULL,"
                      "load_date = current_date "
                    "where yr is not NULL;")
    conn.execute(clean_tax)
    #update new tax information
    update_tax = ("update combined_table "
            "set yr = tax.yr, net_due = tax.net_due, "
                "late_fees = tax.late_fees, "
                "load_date = tax.load_date "
            "from (select parcelid, min(yr) yr, sum(net_due) net_due, "
                    "sum(late_fees) late_fees, max(load_date) load_date "
                "from com_tax where "
                "load_date = (select max(load_date) from com_tax) "
                "group by parcelid) tax "
            "where parid = parcelid")
    conn.execute(update_tax)
    update_metadata(False, "com_tax")

def build_sc_trustee(accdb):
    """Loads updated tax delinquency data from Shelby County Trustee into 
    blight_data db (sc_trustee) and then updates combined table to reflect
    changes

    Args:
        accdb (string): name of access database (accdb) with extension
        to be searched
    
    Returns:
        None
    """

    os.chdir('/home/nate/dropbox-caeser/Data/MIDT/Data_Warehouse/sc_trustee')
    table_names = subprocess.Popen(['mdb-tables','-1', accdb],
            stdout=subprocess.PIPE).communicate()[0]
    tables = table_names.split('\n') 
    df = pd.DataFrame(columns={'startyr':np.int,
                               'parid':np.str,
                               'sumrecv':np.float,
                               'sumdue':np.float,
                               'status':np.str})
    cols = {'MinOfTownCntlYearYY':'startyr',
            'Assr Parcel':'parid',
            'SumOfReceivTaxDue':'sumrecv',
            'SumOfTotalDue':'sumdue'}

    status = set(['Active', 'Redemption', 'Eligible'])

    for table in tables:
        if 'Assr' in table:
                       rows = subprocess.Popen(['mdb-export', accdb, table],
                                stdout=subprocess.PIPE).communicate()[0]
                       print table
                       print len(rows.split('\n'))
                       df_tbl = pd.read_table(StringIO.StringIO(rows), sep=',', 
                               header=0, quotechar='"', lineterminator='\n',
                               usecols=cols.keys())
                       df_tbl = df_tbl.rename(columns=cols)
                       df_tbl['status'] = status.intersection(table.split(' ')).pop()
                       df = df.append(df_tbl, ignore_index=True)
    
    today = datetime.today()
    df['load_date'] = '{0}-{1}-{2}'.format(today.year, 
                                                today.month,today.day)
    df.to_sql('sc_trustee', engine, if_exists='append')
    #delete rows that contain tax deliq to only show new records
    clean_tax = """update combined_table \
                    set startyr = NULL,
                        sumdue = NULL,
                        sumrecv = NULL,
                        status = NULL,
                        load_date = current_date
                    where startyr is not NULL;"""
    conn.execute(clean_tax)
    #update new tax information
    update_tax = """update combined_table
            set startyr = tax.startyr, sumdue = tax.sumdue, 
            sumrecv = tax.sumrecv, 
            status = tax.status,
                load_date = tax.load_date
            from (select parid, min(startyr) startyr, sum(sumdue) sumdue, 
                    sum(sumrecv) sumrecv, max(load_date) load_date,
                    status
                from sc_trustee where load_date = current_date
                group by parid, status) tax
            where combined_table.parid = tax.parid"""
    conn.execute(update_tax)

def update_com_tables(file_path):
    """
    Updates code enforcement tables, loads new dat files and updates columns 
    in combined_table. 
    
    CMEM_CE_SR_AUDIT_OUT_yyyymmdd.dat -> AUDIT table   
    CMEM_CE_SR_HDR_OUT_yyyymmdd.dat -> HDR table
    Args:
        file_path (string): name of folder containing new tables to be loaded. 
            Typically in dropbox-caeser/Data/MIDT/Data_Warehouse/com/load
    
    Returns:
        Pandas dataframe
    """
    os.chdir(file_path)
    def load_tables(f):
        """
        helper function to load COM tables into db
        Args:
            f (string): name of .dat file to be loaded
        """
        import re

        if 'HDR' in f:
            tbl_name = 'com_incident'
    	    divisions = ['Memphis Housing Authority', 'Engineering', 
                         'General Services', 'Parks and Neighborhoods', 
                         'Police Services', 'Public Works', 'Shelby County', 
                         '311', 'Housing Community Development', 
                         'City Attorney', 'Executive', 'Fire Services']
            #regex that checks for any line break not immediately followed
            #by a division name
            rex = "((\n)(?!{}))".format("|".join(divisions))
            with open(f, 'r') as c:
                cread = c.read()
                clean_c = re.sub(rex, "", cread)
            with open(f, 'w') as c:
                c.write(clean_c)
        else:
            tbl_name = 'com_servreq'

        df = pd.read_csv(f, delimiter='|', quoting=3)
        df.rename(columns={col: col.lower() for col in df.columns}, 
                   inplace=True)
        if tbl_name == 'com_incident':
            #check if collection_day is string, convert if not
            if df.collection_day.dtype == np.float:
                df.collection_day = df.collection_day.astype(np.str)
            df.drop('swm_code', axis=1, inplace=True)
            field_remap = {'collection_day':{'1.0':'M','1':'M',
                                                '2.0':'T','2':'T',
                                                '3.0':'W','3':'W',
                                                '4.0':'R','4':'R',
                                                '5.0':'F','5':'F',
                                                '0.0':'N/A','0':'N/A',
                                                '9.0':'N/A','9':'N/A'},
                             'mlgw_status':{'I':'Inactive',
                                                'A':'Active',
                                                'F': 'Final',
                                                'N':'New'}}
            df = df.replace(field_remap)         
        for col in df.columns:
            name = set(col.split('_'))
            if 'date' in name and len(name.intersection(['date', 'flag'])) == 1:
                df[col] = pd.to_datetime(df[col]) 
        print '\tPushing {} to postgresql'.format(tbl_name)
        df.to_sql(tbl_name, engine, if_exists='append')
        update_metadata(False, tbl_name)

    for f in os.listdir('.'):
        print f
        load_tables(f)
        if 'HDR' in f:
            print '\tUpdating data from {}'.format(f)
            df = pd.read_sql('select * from com_incident', engine)
            skip_fields = ['index', 'incident_id', 'incident_number', 
                      'incident_type_id', 'created_by_user', 'resolution_code', 
                      'last_modified_date','followup_date','next_open_task_date', 
                      'owner_name','street_name', 'address1', 'address2', 
                      'address3', 'city', 'state', 'postal_code', 'district', 
                      'sub_district','target_block', 'map_page', 'area', 'zone',
                      'swm_code file_data', 'parcel_id']
    
            #sql query to select only fields wanted in update
            sql_cols = ("SELECT array_to_string("
                            "ARRAY(SELECT '{table_name}' || '.' || "
                            "c.column_name FROM information_schema.columns As c "
                                "WHERE table_name = '{table_name}' "
                                "AND  c.column_name NOT IN ('{fields}') "
                                "), ',') as sqlstmt")


            str_skip_fields = "','".join(f for f in skip_fields)
            
            tbl_dict = {'table_name':'com_incident','fields':str_skip_fields}
            com_incident_fields = conn.execute(sql_cols\
                                        .format(**tbl_dict))\
                                        .fetchall()[0][0]

            combined_table_fields = com_incident_fields\
                                    .replace('com_incident.', '')

            update_params = {'combined_table':'combined_table',
                            'combined_table_fields':combined_table_fields,
                            'com_incident_fields':com_incident_fields,
                            'com_incident':'com_incident'}

            sql_update = ("update {combined_table} "
                            "set load_date = current_date, "
                            "({combined_table_fields}) = ({com_incident_fields})"
                          " from (select distinct on (parcel_id) parcel_id, " 
                                  "{com_incident_fields} "
                                "from {com_incident} "
                                "order by "
                                    "{com_incident}.parcel_id, "
                                    "{com_incident}.reported_date desc) {com_incident} "
                            "where {combined_table}.parid = {com_incident}.parcel_id")
            #udpate rows with new request information
            conn.execute(sql_update.format(**update_params))

            #update total count for number of requests (numreqs)
            conn.execute(
                ("update combined_table "
                    "set load_date = current_date, "
                        "numreqs = q.count "
                    "from (select count(parcel_id) count, parcel_id "
                          "from com_incident "
                            "group by parcel_id) q "
                        "where q.parcel_id = parid"
                ))                                                    
            update_metadata(False, "combined_table")
    shutil.move(f, "../"+f)

def mlgw(table_name):
    """
    updates columns mtrtyp, ecut, and gcut in combined_table using most
    recent dump provided by MLGW

    Args:
	input:
	    table_name: full path with extension to most recent mlgw table
    Returns:
	None
    """
    mlgw_cols = OrderedDict([('mtrtyp', np.str),
			     ('address', np.str),
			     ('ecut', np.str),
			     ('gcut', np.str),
			     ('prem', np.str),
			     ('parid', np.str),
			     ('lat', np.float),
			     ('lon', np.float)])  
    df_mlgw = pd.read_csv(table_name, header=0, names=mlgw_cols.keys(),
	    dtype=mlgw_cols)
    df_mlgw['ecut'] = pd.to_datetime(df_mlgw['ecut'],format='%m/%d/%Y')
    df_mlgw['gcut'] = pd.to_datetime(df_mlgw['gcut'],format='%m/%d/%Y')
    missing_parids = list()
    for idx in df_mlgw[(df_mlgw.parid.isnull())&(df_mlgw.lat.notnull())].index:
        coord = df_mlgw.loc[idx][['lat', 'lon']].to_dict()
        sql_geom = ("select parcelid from sca_parcels p "
                "where st_intersects "
                    "(st_transform(st_setsrid(st_point("
                        "{lon},{lat}),4269),2274),p.wkb_geometry)")
        parid = conn.execute(sql_geom.format(**coord)).fetchone() 
        if parid:
            df_mlgw.set_value(idx, 'parid', parid[0])
    df_mlgw['load_date'] = pd.to_datetime('today')
    df_mlgw.to_sql('mlgw_disconnects', engine, if_exists='append')
    update_metadata(False, "mlgw_disconnects")

    #clean previous columns to remove mlgw designation
    sql_delete = ("update combined_table "
                      "set mtrtyp = Null, ecut = Null, gcut = Null, "
                        "load_date = Null "
                    "where mtrtyp is not null")
    conn.execute(sql_delete)
    sql_update = ("update combined_table set "
                        "mtrtyp = upd.mtrtyp, "
                        "ecut = upd.ecut, "
                        "gcut = upd.gcut, "
                        "load_date = upd.load_date "
                    "from (select parid, mtrtyp, ecut, gcut, load_date "
                "from mlgw_disconnects where parid is not null "
                "and load_date = (select max(load_date) from mlgw_disconnects)) upd "
                "where combined_table.parid = upd.parid")
    conn.execute(sql_update)

def update_assessor():
    """Updates combined_table with new Assessor data. It assumes that all 
    Assessor tables (sca_*) have been updated with new data.
    """

    ct = pd.read_sql("select parid from combined_table", engine)
    par = pd.read_sql("select parcelid from sca_parcels", engine)
    missing_parid = ct[ct.parid.isin(par.parcelid) == False].parid.tolist()
    
    assessor = {'sca_asmt':['aprland','aprbldg', 'class', 'rtotapr'],
                'sca_comintext':['extwall'],
                'sca_dweldat':['rmbed', 'fixbath', 'sfla', 'extwall', 'yrblt'],
                'sca_legdat':['subdiv'],
                'sca_owndat':[['own1','own1'],
                              ['ownadr','adrno'],
                              ['owndir','adrdir'],
                              ['ownstr','adrstr'],
                              ['ownsuf','adrsuf'],
                              ['cityname','cityname'],
                              ['statecode','statecode'],
                              ['ownzip','zip1']],
                'sca_pardat': ['adrno', 'adradd', 'adrdir', 'adrstr', 'adrsuf',
                             'zip1', 'zoning'],
                'sca_comdat': ['yrblt']}
    engine.execute(("alter table combined_table "
                    "drop column if exists geom;"
                    "select addgeometrycolumn('combined_table', 'geom', "
                    "2274, 'point', 2);"
                    "update combined_table set geom = "
                    "st_transform(st_setsrid(st_point(coord[1],coord[2]),"
                        "4326), 2274);"
                    "create index gix_combined_table on combined_table "
                    "using gist (geom)"))
    
    for tbl, cols in assessor.iteritems():
        #build strings to be used in set clause and column selection in subquery
        if tbl != 'sca_owndat':
            new_vals = ', '.join("{0} = {1}.{0}".format(col, tbl) for col in cols)
            col_select = ', '.join(col for col in cols)
        else:
            new_vals = ', '.join("{0} = {1}.{2}".format(col[0],
                                    tbl, col[1]) for col in cols)
            col_select = ', '.join(col[1] for col in cols)
        missing = "', '".join(par for par in missing_parid)
        update_vals = {"new_vals": new_vals,
                       "col_select": col_select,
                       "table": tbl,
                       "missing": missing ,
                       "where_clause": 
                            {"existing_clause": "ct.parid = {}.parid".format(
                                tbl),
                             "missing_clause":  ("ct.parid in ('{0}') and "
                                "st_within(geom, {1}.wkb_geometry)").format(
                                    missing, tbl)
                             }}

        update = ("update combined_table ct set load_date = current_date, "
                  "{new_vals} from (select parid, wkb_geometry, {col_select} "
                     "from {table}, sca_parcels where parcelid=parid) {table} "
                  "where {where_clause}")
        update_aggregate = ("update combined_table ct "
                            "set load_date = current_date, "
                                "mdnyrblt = {table}.mdnyr, numbldgs = num "
                                "from (select parid, count(parid) num, "
                                "median(yrblt)::integer mdnyr, wkb_geometry "
                                "from {table}, sca_parcels where "
                                "parid = parcelid group by parid, wkb_geometry) " 
                                "{table} where {where_clause}")
        #drop end of update string and add nested dictionary key to run each
        #where clause seperately
        engine.execute((update[:-1]+"[existing_clause]}").format(**update_vals))
        engine.execute((update[:-1]+"[missing_clause]}").format(**update_vals))
        if tbl == 'sca_comdat':
            engine.execute((update_aggregate[:-1]+"[existing_clause]}").format(
                **update_vals))
            engine.execute((update_aggregate[:-1]+"[missing_clause]}").format(
                **update_vals))

        
    engine.execute("alter table combined_table drop column geom")


def load_assessor(year, skip=[]):
    """
    Replaces all assessor tables (sca_*) in the blight_data database with 
    new assessor data, using the year to switch directories

    Args:
        year (str): year string representing the Assessor directory in 
            sharedworksapce that is to be searched.
    Returns:
        None
    """
    os.chdir("/home/nate/sharedworkspace/Data/Assessor/"+year)
    tables = ([t.split('.')[-1] for t in meta.tables.keys() 
                    if "sca" in t.split(".")[-1]
                    and t.split(".")[-1] != "sca_parcels"])
    print("Updating all Assessor Tables:")
    drop_table = ("drop table if exists {} cascade")
    for table in [table for table in tables if table not in skip]:
        print("\t\t " + table)
        df_cur = pd.read_sql("select * from {} limit 1".format(table), engine)
        fn = table.split("_")[-1].upper() + ".txt"
        df = pd.read_csv(fn, 
                         header=0, 
                         names=[col for col in df_cur.columns if col != "id"],
                         dtype={k:v for k, v in df_cur.dtypes.iteritems()},
                         encoding="cp1252")
        engine.execute(drop_table.format(table))
        df.to_sql(table, engine, if_exists="replace", index=False)

        if table != "sca_aedit":
            engine.execute("create index idx_parid_{0} on {0}(parid) ".format(table))
    
    #Update Parcel data
    print("\nUpdating Parcel data, may take a few minutes")
    if "Parcels{}.shp".format(year) not in os.listdir("."):
        msg = ("\n\nEnter the name of the parcel shapefile without any extension: ")
        parcels = raw_input(msg)
    else:
        parcels = "Parcels{}.shp".format(year)

    q_drop = "drop table sca_parcels cascade"
    engine.execute(q_drop)
    shp2pgsql = ["shp2pgsql", "-I", "-s 2274", parcels, "public.sca_parcels"]
    psql = ["psql -U {user} -d {db} -h {host} -p 5432".format(**cnx_params.blight)]
    process_shp2pgsql = subprocess.Popen(shp2pgsql, stdout=subprocess.PIPE)
    process_psql = subprocess.Popen(psql, stdin=process_shp2pgsql.stdout,
                                    env={"PGPASSWORD": cnx_params.blight["password"]},
                                    shell=True)
    process_shp2pgsql.stdout.close()
    process_psql.communicate()[0]
    q_index = "create index idx_parcelid_sca_parcels on sca_parcels(parcelid);"
    engine.execute(q_index)
    q_alter = "alter table sca_parcels rename geom to wkb_geometry"
    engine.execute(q_alter)

def update_metadata(update_all=True, update_table=""):
    
    md_tables = [t[0] for t in engine.execute("select tbl_name from info.tbls").fetchall()]
    
    #update assessor (sca_* first)
    yr = engine.execute("select taxyr from sca_pardat limit 1").fetchone()[0]

    tables = {"com_incident": "reported_date",
              "permits": "year",
              "bcs_property": "start_date",
              "com_tax": "yr",
              "com_servreq": "creation_date",
              "sc_trustee": "startyr",
              "mlgw_disconnects": "ecut",
              "caeser_register": "date",
              "caeser_landbank": "createdate",
             }
    q_time = "select {0}({1}) from {2}"
    if not update_all:
        q_update = ("update info.tbls "
                    "set last_update = to_date('{0}', 'YYYY-MM-DD') "
                    "where tbl_name = '{1}'")
        to_date = date.today().strftime("%Y-%m-%d")
        engine.execute(q_update.format(to_date, update_table))
    else:
        q_update = ("update info.tbls "
                    "set first_yr = {0}, "
                        "last_yr = {1}, "
                        "last_update = to_date('{2}', 'YYYY-MM-DD') "
                    "where tbl_name = '{3}'")
        for tbl in md_tables:
            print(tbl)
            try:
                last_update = (engine.execute(
                                q_time.format("max","load_date", tbl))
                                      .fetchone()[0])
            except:
                last_update = datetime(1900, 1, 1)
            if tbl[:4] == "sca_":
                engine.execute(q_update.format(yr, yr, yr, tbl))
            elif tbl in tables.keys():
                min_yr = engine.execute(q_time.format("min", tables[tbl], tbl)).fetchall()[0][0]
                max_yr = engine.execute(q_time.format("max", tables[tbl], tbl)).fetchall()[0][0]
                if type(max_yr) in (datetime, date):
                    engine.execute(q_update.format(min_yr.year, max_yr.year,
                                                    last_update, tbl))
                else:
                    engine.execute(q_update.format(min_yr, max_yr,
                                                    last_update, tbl))

def main(args):
    if args["code_enforcement"]:
        path = args["<path>"]
        try:
            os.chdir(path)
        except:
            print("Path not recognized. Check location and try again.")
            return
        update_com_tables(path)
    if args["land_bank"]:
        land_bank() 
    if args["register"]:
        params = {}
        if args["--startdate"]:
            params["startdate"] = args["--startdate"]
        if args["--enddate"]:
            params["enddate"] = args["--enddate"]
        if args["<instype>"]:
            params["instype"] = args["<instype>"]
    if args["mlgw"]:
        if args["<path_name>"]:
            try:
                os.chdir(args["<path_name>"])
            except:
                print("Path not recognized. Check location and try again.")          
        else:
            os.chdir('/home/nate/dropbox-caeser/Data/MIDT/Data_Warehouse/mlgw')
        mlgw(args["<file_name>"])
    if args["assessor"]:
        if args["--update"]:
            update_assessor()
        elif args["--load"]:
            if args["--skip"]:
                load_assessor(args["--load"], args["--skip"][0].split(","))
            else:
                load_assessor(args["--load"])
    if args["com_tax"]:
        com_tax(args["<date>"])


    
if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)
    
    
