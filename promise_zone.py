'''
Created on Feb 18, 2016

@author: nate
'''
import os
import pandas as pd
import geopandas as gpd
import requests
import numpy as np
from datetime import datetime
import json
from shapely.geometry import Point
from collections import OrderedDict
import re 

dir_list = os.listdir('.')
count = 0
cols = {'CaseNumber':np.str,
        'IncidentDate':datetime,
        'IncidentTime':datetime,
        'DOW':np.str,
        'Address':np.str,
        'Ward':np.str,
        'Offense':np.str,
        'Jurisdiction':np.str,
        'Category':np.str}

cols_youth = OrderedDict([('arrestdate',datetime),
                            ('arresttime',datetime),
                            ('year',datetime),
                            ('Address',np.str),
                            ('Jurisdiction',np.str),                            
                            ('Ward',np.str),
                            ('Sex',np.str),
                            ('Race',np.str),
                            ('Age',np.str),
                            ('arresttype',np.str),
                            ('chargetype',np.str),
                            ('agestatus',np.str),])

for i in range(len(dir_list)):
    with open(dir_list[i], 'r') as f:
        print dir_list[i]
        l = len(f.readlines()) 
        count += l              
    if i == 0:
        df_youth = pd.read_csv(dir_list[i],dtype=cols_youth)        
    else:
        print dir_list[i]
        df_youth = df_youth.append(pd.read_csv(dir_list[i],dtype=cols_youth), 
                                    ignore_index=True, verify_integrity=True)
    
url = ("https://gis4.memphis.edu/arcgis/rest/services/Geolocators"
        "/Midsouth_Composite/GeocodeServer/geocodeAddresses")
df_youth = pd.read_csv('mpd_youth_arrest_all.csv')

df['lon'] = None
df['lat'] = None
df['geom'] = None
df['score'] = None
df['loc'] = None
missing = 0
for i in df.index:
        addrs = {'records':[]}
        addr = df.ix[i].Address.replace('&', ' and ') 
        city = 'Memphis' 
        state = 'TN' 
        zip = ''#str(df.ix[i].ZIP)
        
        row = {'Street':addr,
               'City':city,
               'State':state,
               'Zip':zip} 
        
        addrs['records'].append({'attributes':row})
        try:
            req = requests.get(url+'?addresses={}&f=pjson'.format(json.dumps(addrs).strip()))
            resp = req.json()
            print resp
            x = resp['locations'][0]['location']['x']
            y = resp['locations'][0]['location']['y']
            df.loc[i,'lon'] = x
            df.loc[i,'lat'] = y
            df.loc[i, 'geom'] = Point(x,y)
            df.loc[i, 'score'] = resp['locations'][0]['score']
            df.loc[i, 'loc'] = resp['locations'][0]['attributes']['Loc_name']
                #dist = gpd.GeoSeries([Point(x,y)]).distance(school).values[0]/5280
                #df.loc[i, 'MILEAGE'] = dist
        except:
            print df.ix[i]
            missing += 1

df_dict = {}

gdf = gpd.GeoDataFrame(df,geometry='geom')
gdf.set_geometry('geom', inplace=False, crs='2274')

gdf_test = gdf.index[:50]
gdf.to_file('mpd_incident_2014-022016.shp', driver='ESRI Shapefile')
missing = df[df['locator'].isnull()]

part1crime_raw = set(['13A', '13A   AGGRAVATED ASSAULT/DV', 'Aggravated Burglary', 
             'Aggravated Burglary Business',  '240', 'Motor Vehicle', 
             '240   MOTOR VEHICLE THEFT', 'MOTOR VEHICLE THEFT', 
             'Auto Theft $1000-$10000', 'Auto Theft', 'MVT/Passenger Vehicle', 
             'MVT/Motorcyle', '220', 'Burglary Residential', 
             'Burglary Non Residential', '220 Burglary/Breaking and Entering', 
             'Burglary Business', 'Burglary',  '09A', 'Murder', 
             '09A   MURDER & NON-NEGLIGENT MANSLAUGHTER', 
             'Attempt Criminal, Felony To Wit: First Degree Murder', 
             'Justifiable Homicide', '23A', '23B', '23C', '23D', '23E', '23F', '23H',
             '23H   ALL OTHER LARCENY', '23C   SHOPLIFTING', 
             '  23C Shoplifting/Misdemeanor', '23H  Theft of Property', '23A   POCKET PICKING',
             '11A', '11A   RAPE', 'Forcible rape', 'Rape'])

part1crime = set(['homicide', 'robbery', 'assault', 'rape', 'manslaughter'])
gdf_youth = gpd.read_file('mpd_youth_14-16.shp')

for i in gdf_youth.index:
    charge = gdf_youth.loc[i, 'chargetype']
    #print charge
    if not charge in part1crime_raw:
        gdf_youth.loc[i, 'part1'] = not not set(re.sub(r'\W+', ' ', charge).
                                (lower().split(' ')).intersection(part1crime))
    else:
        gdf_youth.loc[i, 'part1'] = True

    
    print '\t', gdf_youth.loc[i, 'part1']
    
t = gdf_youth[gdf_youth['part1'] == True]
for i in gdf_youth.index:
    charge = set(re.sub(r'\W+', ' ', gdf_youth.loc[i, 'chargetype']).lower().split(' '))   
    if set(['statutory', 'simple']).intersection(charge):
        gdf_youth.loc[i, 'part1'] = False

for i in gdf_youth_p.index:
    gdfindex = gdf_youth_p.loc[i,'index']
    if gdfindex in df_youth.index:
        print gdf_youth_p.loc[i, 'chargetype'], gdf_youth_p.loc[i, 'Address'], 
                                            gdf_youth_p.loc[i, 'arrestdate']
        print df_youth.loc[gdfindex, 'chargetype'], df_youth.loc[i, 'Address'],
                                                 df_youth.loc[i, 'arrestdate']
        gdf_youth_p.loc[i, 'arrestdate'] = df_youth.loc[i, 'arrestdate']
        gdf_youth_p.loc[i, 'year'] = df_youth.loc[i, 'year']
        print '\n\n'
 
gdf_youth.to_file('mpd_youth_14-16_processed.shp')
gdf_youth_p = gpd.read_file('mpd_youth_14-16_processed.shp')
   