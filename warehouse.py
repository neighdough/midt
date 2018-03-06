'''
Created on Oct 21, 2015

@author: Nate Ron-Ferguson

TODO:
    add method to update date field in combined table
    add call to method for all update methods
'''

from collections import OrderedDict, defaultdict
import os
import pandas as pd
import numpy as np
from sqlalchemy import (create_engine, MetaData, Table, Column,
                        String, Numeric, Integer, Boolean, Date,
                        join, func, Index, ForeignKey)
from sqlalchemy.sql import select, expression, and_
import re
from datetime import datetime
import itertools
import math
import geopandas as gpd
import subprocess
import StringIO
from config import cnx_params
from caeser import utils

engine = utils.connect(**cnx_params.blight)
conn = engine.connect()
meta = MetaData(schema='public')
meta.reflect(bind=engine)

bcs_tbl = Table('bcs_property', meta, autoload=True, autoload_with=engine)
bcs_vals = {'rating':['1-Excellent','2-Good','3-Fair',
                      '4-Bad','5-Severely Dilapidated'],
            'occupancy':['No Structure','Structure-Occupied ',
                         'Structure-Partially Occupied',
                         'Structure-Possibly Unoccupied',
                         'Structure-Unoccupied'],
            'litter':['Litter-High','Litter-Low','Litter-Medium',
                      'Litter-None'],
            'land_use':['Government','Industrial','Mixed-Use',
                        'Mobile Home Park','Office','Other ',
                        'Park/Garden','Parking Lot','Religious',
                        'Residential-Multi Family','Residential-Single Family',
                        'Restaurant/Bar','Retail',
                        'School','Unknown','Utility/Rail', 'Vacant Lot'],
            'fire':['Fire Damage-Collapsed','Fire Damage-Major',
                    'Fire Damage-Minor'],
            'roof':['Damaged Roof-Major','Damaged Roof-Minor']}

bool_fields = set([col.name for col in bcs_tbl.columns if not col.name in \
                    bcs_vals.keys() + ['parcelid','start_date', 
                                        'resolution_date', 'createdby']])

def build_assessor_tables(year):
    
    tables = {'ASMT': {'PARID': np.str, 'APRLAND':np.double, 'APRBLDG': np.double,
                        'RTOTAPR':np.double, 'LUC':np.str, 'CLASS':np.str}, 
              'PARDAT': {'PARID': np.str, 'ADRNO': np.str, 'ADRADD': np.str, 
                         'ADRDIR':np.str, 'ADRSTR': np.str, 'ADRSUF': np.str,
                         'ZIP1': np.str, 'ZONING': np.str},
              'OWNDAT': {'PARID': np.str, 'OWN1': np.str, 'ADRNO': np.str, 
                         'ADRDIR': np.str, 'ADRSTR': np.str, 'ADRSUF': np.str,
                         'CITYNAME': np.str,'STATECODE': np.str, 'ZIP1': np.str}, 
              'LEGDAT': {'PARID': np.str, 'SUBDIV': np.str},
              'DWELDAT': {'PARID': np.str, 'YRBLT': np.int64, 'EXTWALL': np.str, 
                          'RMBED': np.double, 'FIXBATH': np.double, 'SFLA': np.int64},
              'COMDAT': {'PARID': np.str,'YRBLT': np.int64, 'AREASUM': np.int64},
              'SALES': {'PARID': np.str, 'TRANSNO': np.str}        
      #dropped comintext since bldg square footage is calculated using areasum from comdat
              #'COMINTEXT': {'PARID': np.str, 'EXTWALL': np.str, 'SF': np.double}
              }
    
    os.chdir('/home/nate/sharedworkspace/DATA/Assessor/{}/'.format(year))
    
    for table, fields in tables.iteritems():
        print table
        new_table_name = 'sca_'+table.lower()
        cols = OrderedDict(fields)
        tdf = pd.read_csv(table+'.csv', usecols=cols.keys(), dtype = cols)
        tdf.rename(columns={field: field.lower() for field in cols.keys()},
                    inplace=True)  
        print tdf.head(), '\n'  
        tdf.to_sql(new_table_name, engine)
        psql_table = Table(new_table_name,meta)
        ix_name = 'ix_'+table.lower()+'_parid'
        ix = Index(ix_name, psql_table.c.parid)
        ix.create(engine)
         
def build_audit_tables(file_path):
     
    'CMEM_CE_SR_AUDIT_OUT_20151030.dat'
    servreq = pd.read_csv(file_path, delimiter = '|')   
    servreq.rename(columns={col: col.lower() for col in servreq.columns}, 
                   inplace=True)
     
    for col in servreq.columns:
        name = set(col.split('_'))
        if 'date' in name and len(name.intersection(['date', 'flag'])) == 1:
            servreq[col] = pd.to_datetime(servreq[col])    
 
    print '\tPushing audit table to postgresql'
    servreq.to_sql('com_servreq', engine, if_exists='append')            

def build_hdr_tables(file_path):
    """CMEM_CE_SR_HDR_OUT.dat"""
    #clean_incident = clean_com_tables(file_path)
    incident_audit = pd.read_csv(file_path, delimiter='|')
    incident_audit.rename(columns={col: col.lower() for col in incident_audit.columns}, 
                          inplace=True)
    incident_audit = incident_audit.drop('swm_code', axis=1)
        
    for col in incident_audit.columns:
            name = set(col.split('_'))
            if 'date' in name and len(name.intersection(['date', 'flag'])) == 1:
                incident_audit[col] = pd.to_datetime(incident_audit[col])

    print '\tPushing hdr table to postgresql'
    incident_audit.to_sql('com_incident', engine, if_exists='append')

def prep_hdr_table(df_in=None):
    """
    helper function to select and clean records for the hdr (com_incident)
    table
    """
    if not df_in:
        tbl_incident = Table('com_incident', meta)
        df_incident = pd.read_sql(select([tbl_incident]), engine) 
        """subset incident table by identifying index number of the most
        recent incident for each parcel"""
        df_incident = df_incident.iloc[df_incident.groupby(['parcel_id'], 
                                                       sort=False)['creation_date'].idxmax()]
    else:
        df_incident = df_in
        df_incident.rename(columns={'parcel_id': 'parid'}, inplace=True)
    collection_remap = {'collection_day':{'1.0':'M','1':'M',
                                          '2.0':'T','2':'T',
                                          '3.0':'W','3':'W',
                                          '4.0':'R','4':'R',
                                          '5.0':'F','5':'F',
                                          '0.0':'N/A','0':'N/A',
                                          '9.0':'N/A','9':'N/A'}}
    df_incident = df_incident.replace(collection_remap)
    mlgw_status_remap = {'mlgw_status':{'I':'Inactive',
                                        'A':'Active',
                                        'F': 'Final',
                                        'N':'New'}}
    df_incident = df_incident.replace(mlgw_status_remap)

    df_incident.drop(['index', 'incident_id', 'incident_number', 
                      'incident_type_id', 'created_by_user', 'resolution_code', 
                      'last_modified_date','followup_date', 
                      'next_open_task_date', 'owner_name','street_name', 
                      'address1', 'address2', 'address3', 'city', 'state', 
                      'postal_code', 'district', 'sub_district','target_block', 
                      'map_page', 'area', 'zone','swm_code file_data'], 
                      inplace=True, axis=1)
    

    return df_incident

def update_hdr_fields():
    skip_fields = ['index', 'incident_id', 'incident_number', 
                      'incident_type_id', 'created_by_user', 'resolution_code', 
                      'last_modified_date','followup_date', 
                      'next_open_task_date', 'owner_name','street_name', 
                      'address1', 'address2', 'address3', 'city', 'state', 
                      'postal_code', 'district', 'sub_district','target_block', 
                      'map_page', 'area', 'zone','swm_code file_data', 'parcel_id']
    
    #sql query to select only fields wanted in update
    sql_cols = """SELECT array_to_string(ARRAY(SELECT '{table_name}' || '.' || \
                  c.column_name FROM information_schema.columns As c\
                        WHERE table_name = '{table_name}' \
                        AND  c.column_name NOT IN ('{fields}')\
                        ), ',') as sqlstmt"""
                        #|| ' FROM {table_name} As o' As sqlstmt"""

    str_skip_fields = "','".join(f for f in skip_fields)
    
    com_incident_fields = conn.execute(sql_cols.format(**{'table_name':'com_incident',
        'fields':str_skip_fields})).fetchall()[0][0]

    combined_table_fields = com_incident_fields.replace('com_incident.', 
            '')

    update_params = {'combined_table':'combined_table',
                     'combined_table_fields':combined_table_fields,
                     'com_incident_fields':com_incident_fields,
                     'com_incident':'com_incident'}

    sql_update = """update {combined_table} set load_date = current_date,
    ({combined_table_fields}) = ({com_incident_fields}) \
        from (select distinct on (parcel_id) parcel_id,  {com_incident_fields} \
            from {com_incident} \
            order by {com_incident}.parcel_id, {com_incident}.reported_date desc \
            ) {com_incident} \
            where {combined_table}.parid = {com_incident}.parcel_id"""
    
    conn.execute(sql_update.format(**update_params))

    conn.execute("""update combined_table set load_date = current_date, \
                    numreqs = q.count \
                    from (\
                    select count(parcel_id) count, parcel_id from com_incident\
                    group by parcel_id) q where q.parcel_id = parid""")

def update_mlgw(table_name):
    """
    updates columns mtrtyp, ecut, and gcut in combined_table using most
    recent dump provided by MLGW
    
    Args:
        input:
            table_name: full path with extension to most recent mlgw table
    Returns:
        None
    """
    mlgw_disconnects(table_name)
    sql = """update combined_table set (mtrtyp, ecut, gcut, load_date) = \
    (upd.mtrtyp, upd.ecut, upd.gcut, current_date) \
             from (select mtrtyp, ecut, gcut from mlgw_disconnects \
             where parid is not null) upd \
             where combined_table.parid = parid"""
    conn.execute(sql)

def update_register(startdate):
    import sys
    sys.path.append('home/nate/dropbox/dev/midt')
    import updates
    
    df = updates.scrape(startdate)
    updates.load('caeser_register', df)


def build_com_indexes():
    com_incident = Table('com_incident', meta, autoload=True, autoload_with=engine)
    com_servreq = Table('com_servreq', meta, autoload=True, autoload_with=engine)
    
    cols = [com_incident.c.incident_id, com_incident.c.parcel_id,
            com_incident.c.creation_date, com_incident.c.incident_type_id,
            com_servreq.c.incident_id, com_servreq.c.resolution_code,
            com_servreq.c.incident_number]
    
    for col in cols:
        print col.name
        i = Index('idx_'+col.name+'_'+col.table.name, col)
        i.create(engine)

   


def clean_com_tables(tbl):
    #from StringIO import StringIO
    with open(tbl, 'r') as f:
        return f.read().replace('\r\n','')
        #return StringIO(f.read().replace('\r\n', ''))

def bcs_property():
    """creates table for Bluff City Snapshot property data if it doesn't exist,
    otherwise, it returns a sqlalchemy table object for use in other methods.
    """
    property = Table('bcs_property', meta,
                         Column('start_date', Date,
                                doc='start date'),
                         Column('resolution_date', Date,
                                doc='resolution date'),
                         Column('createdby', String(10),
                                doc='creator name'),
                         Column('parcelid', String(15), primary_key=True,
                                doc='assessor parcelid'),
                         Column('rating', Integer, server_default=0,
                                doc='''property condition rating, 1: Excellent \
                                to 5: Severely Dilapidated; 0 = missing'''),
                         Column('occupancy', String(30), server_default='Not Collected',
                                doc='''structure occupancy: occupied, partially occupied, \
                                possibly unoccupied, unoccupied, no structure'''),
                         Column('litter', String(6), server_default='None',
                                doc='litter index, none, low, medium, high'),
                         Column('land_use', String(30), server_default='',
                                doc='land use type'),
                         Column('vegetation', Integer, default='0',
                                doc='overgrown vegetation, 1 yes, 0 no'),
                         Column('trash', Integer, default='0',
                                doc='trash/debris, 1 yes, 0 no'),
                         Column('dumping', Integer, default='0',
                                doc='illegal dumping, 1 yes, 0 no'),
                         Column('tree', Integer, default='0',
                                doc='fallen tree, 1 yes, 0 no'),
                         Column('construction', Integer, default='0',
                                doc='active construction, 1 yes, 0 no'),
                         Column('rent', Integer, default='0',
                                doc='for rent/sale sign, 1 yes, 0 no'),
                         Column('vehicle', Integer, default='0',
                                doc='abandoned vehicle, 1 yes, 0 no'),
                         Column('siding', Integer, default='0',
                                doc='damaged siding, 1 yes, 0 no'),
                         Column('painting', Integer, default='0',
                                doc='needs painting, 1 yes, 0 no'),
                         Column('fire', String(10), server_default = 'None',
                                doc='fire damage, minor, major, collapsed'),
                         Column('roof', String(5), server_default='None',
                                doc='damaged roof, major, minor'),
                         Column('windows', Integer, default='0',
                                doc='broken windows, 1 yes, 0 no'),
                         Column('shed', Integer, default='0',
                                doc='damaged shed/garage, 1 yes, 0 no',),
                         Column('graffiti', Integer, default='0',
                                doc='graffiti, 1 yes, 0 no'),
                         Column('porch', Integer, default='0',
                                doc='damaged porch, 1 yes, 0 no'),
                         Column('foundation', Integer, default='0',
                                doc='visible cracks in foundation, 1 yes, 0 no'),
                         Column('fences', Integer, default='0',
                                doc='damaged fence, 1 yes, 0 no'),
                         Column('entry', Integer, default='0',
                                doc='Open to casual entry, 1 yes, 0 no'),
                         Column('boarded', Integer, default='0',
                                doc='boarded, 1 yes, 0 no'),
                         Column('other', Integer, default='0',
                                doc='other issue, 1 yes, 0 no'), 
                     extend_existing=True)
    
    if not property.exists(engine):
        Index('idx_property_parcelid', property, unique=True)
        meta.create_all(engine,[property])        
        add_comments = "comment on {0} {1} is '{2}';"
        c = add_comments.format('table','bcs_property', 
                                'parcel inventory for bluff city snapshot')
        conn.execute(c)
        for col in property.columns:
            c = add_comments.format('column','bcs_property.'+col.name, col.doc)
            engine.execute(c).execution_options(autocommit=True)
            
            print c
        
    return property

def bcs_photos():
    """creates table for Bluff City Snapshot photo data if it doesn't exist,
    otherwise, it returns a sqlalchemy table object for use in other methods.
    """
    photos = Table('bcs_photos', meta,
                      Column('photo_id', Integer, primary_key=True),
                      Column('parcelid', String(15), 
                             ForeignKey('bcs_property.bcs_property_pkey')),
                      Column('url', String(255)),
                      Column('created', Date),
                      Column('createdby', String(10)),
                      extend_existing=True)
    if not photos.exists(engine):
        Index('idx_photo_parcelid', photos)
        meta.create_all(engine,[photos])
    return photos
    
def get_bcs_value(key, value):
    """Returns the text value from the current BCS property record in order to 
    determine which column the value should be placed in. 
    
    Args:
        key: key derived from bcs_vals, module-level dictionary that contains a
        match between possible values from the BCS survey (values) and their 
        matching column (key) 
        
        value: text split from current row of survey data that determines what 
        the final value will be
    Returns:
        Modified version of input value
        
    """
    if key == 'rating':
        return int(value.split('-')[0])
    elif key == 'land_use':
        return value
    else:
        return value.split('-')[1] if '-' in value else value      

def get_boolean(text):
    """Tests whether current text value belongs to one of the boolean fields
    
    Args:
        text: text split from current row of survey data that determines what 
        the final value will be
    Returns:
        list containing the boolean field name and positive value (1)
    """
    text_format = set(['graffiti']) if text == 'Grafitti' else set(clean_text(text))    
    field = text_format.intersection(bool_fields)
    if field:
        return [field.pop(), 1]

def update_bcs_property(bcs, parcels):
   
    bcs_prop = bcs_property()
    #parcels = bcs.parcel_id_formatted.unique()
    #bcs_all = list()
    parcel_count = len(parcels)
    current_count = 1
    for parcel in parcels:
        #dictionary to hold transformed value from survey
        bcs_etl = defaultdict(lambda: defaultdict())
        bcs_etl['parcelid'] = parcel
        start_date = bcs[bcs.parcel_id_formatted == parcel].start_date.unique()[0]
        resolution_date = bcs[bcs.parcel_id_formatted == parcel].resolution_date.unique()[0]
        createdby =  bcs[bcs.parcel_id_formatted == parcel].createdby.unique()[0]
        bcs_etl['start_date'] = start_date         
        bcs_etl['resolution_date'] = resolution_date if not \
                math.isnan(resolution_date) else 'infinity'
        bcs_etl['createdby'] =  createdby                      
        full_text = bcs[bcs.parcel_id_formatted == parcel].condition_txt
        print '\nProperty record ', current_count, ' of ', parcel_count
        current_count += 1
        for text in full_text:
            text_set = set(clean_text(text))
            if 'damaged' in text_set:
                text_set.remove('damaged')
            for key, val in bcs_vals.iteritems():
                val_set = set(itertools.chain.from_iterable(clean_text(v) for v in val))
                if text_set.intersection(val_set):
                    bcs_etl[key] = get_bcs_value(key, text)
                    break
            else:   
                field, result = get_boolean(text)
                bcs_etl[field] = result
        conn.execute(bcs_prop.insert(), bcs_etl) 

def update_bcs_tables():
    """Updates BCS property survey table (bcs_property) by reading in new 
    records from the memparcel service. The BCS survey application utilizes a 
    JSON object that collects multiple records for each parcel, each of which 
    has the same structure, for example, one record looks like:
    
    {
        {'parcel_condition_id': 391186,
        'organization_id': 'bcs',
        'parcel_id_formatted': '021041  00042',
        'condition_txt': 'Residential-Single Family',
        'start_date': '2015-09-23',
        'resolution_date': '',
        'createdby': bcs070
        },
        {'parcel_condition_id': 391185,
        'organization_id': 'bcs',
        'parcel_id_formatted': '021041  00042',
        'condition_txt': 'Structure-Occupied',
        'start_date': '2015-09-23',
        'resolution_date':'',
        'createdby': 'bcs070'
        {
    }
    
    Once the complete survey has been read in, parcels that had already been 
    added to the db are culled (current_parcels()) so that only new parcels are 
    added.
    
    Args:
        None
    Return:
        None
    """
    bcs_service = pd.read_csv('https://memparcel.com/api/rawtable_conditionlog', 
                             dtype={'start_date': datetime,'resolution_date': datetime}) 
    bcs_service = bcs_service[bcs_service.organization_id == 'bcs'] 

    parcels = current_parcels()
    new_parcels = set(bcs_service.parcel_id_formatted.unique()).difference(parcels.parcelid)
    print 'Updating photos...\n'
    update_bcs_photos()
    update_bcs_property(bcs_service, new_parcels)
    
def update_bcs_photos():
    """
    """
    photos = pd.read_csv('https://memparcel.com/api/rawtable_photos', 
                         dtype={'created': datetime})
     
    bcs_pics = photos[(photos.organization_id == 'bcs')&(photos.created >= '2015-10-01')]
    bcs_pics.drop('organization_id', axis=1,inplace=True)
    pics_psql = bcs_photos()
    surveyed_parcels = current_parcels()
    existing_pic_ids = pd.read_sql(select([pics_psql.c.photo_id]).select_from(pics_psql), 
                                                                            engine)
    bcs_pics.columns = [col.name for col in pics_psql.columns]  
    #only keep photo records that have corresponding parcelid in property table
    #and whose photo_id isn't already in the table
    bcs_pics = bcs_pics[(bcs_pics['parcelid'].isin(surveyed_parcels.parcelid)) &
                        (~bcs_pics['photo_id'].isin(existing_pic_ids['photo_id']))]
                         
    bcs_pics.to_sql(pics_psql.name, engine, index=False, if_exists='append')  
                             
def clean_text(text):
    regex = '[^A-Za-z]|[\s{2,}]+'
    return [word for word in re.sub(regex, ' ', text).lower().strip().split(' ') if word != '']

def current_parcels(): 
    """
    Returns a panda dataframe containing all parcels currently in the blight_data
    database
    """
    
    bcs_psql =  Table('bcs_property', meta, autoload=True, autoload_with=engine)
    return pd.read_sql(select([bcs_psql]).select_from(bcs_psql), engine)

def combined_table():
    """builds final table by merging everything together
    need to update the 
    """
    
    tbl_photos = Table('bcs_photos', meta)
    tbl_property = Table('bcs_property', meta)
    tbl_incident = Table('com_incident', meta)
    tbl_servreq = Table('com_servreq', meta)
    tbl_asmt = Table('sca_asmt', meta)
    tbl_aedit = Table('sca_aedit', meta)
    tbl_comdat = Table('sca_comdat', meta)
    #tbl_comintext = Table('sca_comintext', meta)
    tbl_dweldat = Table('sca_dweldat', meta)
    tbl_legdat = Table('sca_legdat', meta)
    tbl_owndat = Table('sca_owndat', meta)
    tbl_pardat = Table('sca_pardat', meta)
    
    tbl_com_tax = Table('com_tax', meta)
    tbl_caeser_register = Table('caeser_register', meta)
    tbl_caeser_landbank = Table('caeser_landbank', meta)
    tbl_mlgw_disconnects = Table('mlgw_disconnects', meta)
    tbl_sc_const_permits = Table('sc_const_permits', meta)
    tbl_sc_demo_permits = Table('sc_demo_permits', meta)
    tbl_sca_sales = Table('sca_sales', meta)

    res_lucs = ['058', '059', '061', '062', '063']

    #select only most recent photo to limit number of returned recs to 1                                 
    df_photos = pd.read_sql(select([tbl_photos]), engine)
    df_photos = df_photos.iloc[df_photos.groupby(['parcelid'],
                                                 sort=False)['photo_id'].idxmax()]
    df_photos.rename(columns={'parcelid': 'parid'}, inplace=True)
    df_photos.drop('createdby', inplace=True, axis=1)
     
    df_property = pd.read_sql(select([tbl_property]), engine)
    skip = ['level_0', 'start_date', 'resolution_date', 'parcelid', 'rating',
            'occupancy','litter', 'land_use', 'fire', 'roof', 'createdby',
            'index']
    boolean_remap = {col:{'0':'no', 
        '1':'yes'} for col in df_property.columns if col not in skip}
    for col in df_property.columns:
        if col not in skip:
           df_property[col] = df_property[col].astype(str)
    df_property.replace(boolean_remap, inplace=True)

    df_property.rename(columns={'parcelid': 'parid'}, inplace=True)
    df_property.drop(['level_0','createdby', 'index'], inplace=True, axis=1)
    
    df_incident = prep_hdr_table()

    """remove com_servreq because no need for timeline at this point"""
    #df_update_bcs_tablesservreq = pd.read_sql(select([tbl_servreq]), engine)
    df_asmt = pd.read_sql(select([tbl_asmt]), engine)
    df_aedit = pd.read_sql(select([tbl_aedit]).where("tble = 'ASMT' and fld = 'LUC'"), engine)
    rep = lambda x: x.replace('- ', '')
    df_aedit['msg'] = df_aedit.msg.apply(rep)
    df_asmt = df_asmt.merge(df_aedit, left_on='luc', right_on='val')
    df_asmt.drop(['index_x', 'index_y', 'luc', 'val', 'tble', 'fld'], 
            inplace=True, axis=1)
    df_asmt.rename(columns={'msg': 'luc'}, inplace=True)
    
    df_comdat = pd.read_sql(select([tbl_comdat]), engine)
    func_comdat = {'areasum': 'sum', 'yrblt': ['median', 'count']}    
    df_comdat = df_comdat.groupby(['parid']).agg(func_comdat)
    df_comdat['parid'] = df_comdat.index

    """removed comintext because Assessor reports total SF using SUMAREA field in COMDAT"""
    #df_comintext = pd.read_sql(select([tbl_comintext]), engine)
    
    df_dweldat = pd.read_sql(select([tbl_dweldat]), engine)
    df_dweldat.drop(['index'], inplace=True, axis=1)
    #remove duplicate residential records need to modify to calculate
    #median yearbuilt and add to comintext mdnyrblt
    df_dweldat.drop_duplicates(subset=['parid'],inplace=True)

    df_legdat = pd.read_sql(select([tbl_legdat]), engine)
    df_legdat.drop(['index'], inplace=True, axis=1)
    
    df_owndat = pd.read_sql(select([tbl_owndat]), engine)
    df_owndat.rename(columns={'adrno':'ownadr', 'adrdir':'owndir',
                              'adrstr':'ownstr', 'zip1':'ownzip',
                              'adrsuf': 'ownsuf'},
                     inplace=True)
    df_owndat.drop(['index'], inplace=True, axis=1)

    df_pardat = pd.read_sql(select([tbl_pardat]), engine)
    df_pardat.drop(['index'], inplace=True, axis=1)
    
    ################## New Additions #################
    df_com_tax = pd.read_sql(select([tbl_com_tax]), engine)
    func_tax = {'net_due':'sum', 'late_fees':'sum', 'yr':'min'}
    df_com_tax = df_com_tax.groupby('parcelid').agg(func_tax)
    df_com_tax.rename(columns={('net_due', 'sum'): 'tax_due',
                               ('late_fees', 'sum'): 'late_fees',
                               ('yr', 'min'): 'start_yr'}, inplace=True)
    df_com_tax['parid'] = df_com_tax.index
#    df_com_tax.rename(columns={'parcelid': 'parid'}, inplace=True)

    df_caeser_landbank = pd.read_sql(select([tbl_caeser_landbank]), engine)
    df_caeser_landbank.drop(['index', 'streetno', 'street', 'dimensions',
                             'zip', 'zoning', 'wkb_geometry'], inplace=True,
                             axis=1)
    df_caeser_landbank.rename(columns={'parcelid':'parid'}, inplace=True)

    df_mlgw_disconnects = pd.read_sql(select([tbl_mlgw_disconnects]), engine)
    df_mlgw_disconnects = df_mlgw_disconnects.sort_values('ecut', 
            ascending=False).groupby('parid', as_index=False).first()
    df_mlgw_disconnects.drop(['index', 'prem', 'lat', 'lon', 'address'], 
                             inplace=True, axis=1)

       
        #********** Register and Sales Join ***********
    df_caeser_register = pd.read_sql(select([tbl_caeser_register]), engine)
    df_caeser_register.drop(['index', 'strno', 'strname', 'city', 'state',
                             'zip'], inplace=True, axis=1)
    df_sca_sales = pd.read_sql(select([tbl_sca_sales]), engine)
    df_sca_sales.drop(['index'], inplace=True, axis=1)
    df_register = pd.merge(df_caeser_register, df_sca_sales,left_on='instno',
                           right_on='transno')
    func_register = {'date':['max', 'min'], 
                     'parid':'count'}
    df_register_grp = df_register.groupby('parid').agg(func_register)
    df_register_grp.columns = ['_'.join(col).strip() for col in \
                                df_register_grp.columns.values]
    df_register_grp['sales_range'] = df_register_grp.date_max.dt.year - \
                                df_register_grp.date_min.dt.year
    df_register_grp['num_sales'] = df_register_grp['parid_count']
    df_register_grp.drop(['date_max', 'date_min', 'parid_count'], inplace=True,
                         axis=1)
    df_register_grp['parid'] = df_register_grp.index 
        #************ End Register and Sales Join **********

    df_sc_const_permits = pd.read_sql(select([tbl_sc_const_permits]), engine)
     
    func_const = {'parid':'count','issued':'max'}
    const_permits_max_date = df_sc_const_permits.sort_values('issued', 
                            ascending=False).groupby('parid', as_index=False).first()
    const_permits_grp = df_sc_const_permits.groupby('parid')['parid'].count()        
    df_sc_const_permits = const_permits_max_date.join(const_permits_grp, 
                                on='parid',rsuffix='grp')
    df_sc_const_permits['parid'] = df_sc_const_permits.index
    df_sc_const_permits.rename(columns={'permit':'const_permit',
                                        'descriptio':'const_desc',
                                        'paridgrp':'num_const_permits',
                                        'issued':'last_const_permit_date'}, 
                                        inplace=True)
    const_cols_keep = ['const_permit', 'sq_ft', 'sub_typ', 'valuation',
                        'const_desc', 'const_type', 'parid','num_const_permits',
                        'last_const_permit_date']   
    const_cols_drop = [col for col in df_sc_const_permits.columns if col \
                        not in const_cols_keep]
    df_sc_const_permits.drop(const_cols_drop, inplace=True, axis=1)

    df_sc_demo_permits = pd.read_sql(select([tbl_sc_demo_permits]), engine)
    demo_permits_keep = ['date', 'permit', 'permit_des', 'type', 'descriptio',
                         'parid']
    df_sc_demo_permits.rename(columns={'permit':'demo_permit', 
                                       'descriptio':'demo_desc'}, inplace=True)
    df_sc_demo_permits.drop([col for col in df_sc_demo_permits.columns if col\
                            not in demo_permits_keep], inplace=True, axis=1)
    
    df_sc_demo_permits.drop_duplicates('parid',inplace=True)
    ################# End New Additions #################
    df_list = [df_photos, df_property, df_incident,df_asmt,
               df_comdat, df_dweldat, df_legdat, df_owndat, df_com_tax,
               df_caeser_landbank,df_register_grp, df_sc_const_permits,
               df_sc_demo_permits, df_mlgw_disconnects]
    
    df_combined = df_pardat
    for df in df_list:
        df = df.set_index('parid')
        df_combined = df_combined.join(df, on='parid')
        print df_combined.shape
        
    df_combined.rename(columns={('areasum', 'sum'): 'area',
                          ('yrblt', 'median'): 'mdnyrblt',
                          ('yrblt', 'count'): 'numbldgs'}, inplace=True)



    today = datetime.today()    
    df_combined['load_date'] = today.strftime('%m/%d/%Y')
    
    print 'Pushing table to Postgres...'
    df_combined.to_sql('combined_table', engine, if_exists='replace')    

def print_unique(col):
    print '\n'.join(i for i in col.unique() if not i is None)

def trash_index():
    '''TODO:
    map values by average trash score, not total
    '''
    
    ws = '/home/nate/dropbox-caeser/Data/MIDT/Data_Warehouse/medical_dist'
    #225' grid derived from sqrt of parcel area standard deviation
    grid = gpd.GeoDataFrame.from_file(ws+'/grid_225ft.shp')
    trash = pd.read_csv(ws+'/trash_summary.csv')  
   
    grid.litter_ind = (grid.SUMlitter_.max())
    trash['index'] = index
    trash['trash_index'] = (index-index.min())/(index.max()-index.min())*100
    
    trash_sort = trash.sort_values('trdivpars', ascending=False)
    for i in range(trash_sort.shape[0]):
        print '\t'.join(str(t) for t in [trash_sort.ix[i].npars, 
                                         trash_sort.ix[i].sumtrash, 
                                         trash_sort.ix[i].trash_index])
    
    trash.to_csv(ws+'/trash_index.csv')

def mlgw_disconnects(table_name):
    os.chdir('/home/nate/dropbox-caeser/Data/MIDT/Data_Warehouse/mlgw')
    mlgw_cols = OrderedDict([('mtrtyp', np.str),
                             ('address', np.str),
                             ('ecut', datetime),
                             ('gcut', datetime),
                             ('prem', np.str),
                             ('parid', np.str),
                             ('lat', np.float),
                             ('lon', np.float)])  
    df_mlgw = pd.read_csv(table_name, header=0, names=mlgw_cols.keys(),
            dtype=mlgw_cols)
    df_mlgw['ecut'] = pd.to_datetime(df_mlgw['ecut'],format='%m/%d/%Y')
    df_mlgw['gcut'] = pd.to_datetime(df_mlgw['gcut'],format='%m/%d/%Y')
    df_mlgw.to_sql('mlgw_disconnects', engine, if_exists='replace')

if __name__ == '__main__':# 
    combined_table()
       
    
    


