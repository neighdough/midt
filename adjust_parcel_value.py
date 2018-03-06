'''
@author: nfergusn@memphis.edu
@organization: Center for Partnerships in GIS, University of Memphis
@summary: compares parcelid and geometry changes between two assessment years 
    and assigns year 2 values 
            to year 1 geometry.
        input: 
            year1 -> shapefile containing parcelid
            year2 -> shapefile containing parcelid and fields for total 
                appraisal and total assessment
        output:
            csv file containing year2 values aggregrated or deaggregated and 
                reassigned to year1 geometry
'''

from shapely.geometry import shape
import fiona
import time
from cpgis import Write_CSV as tbl


def main(dict_base_yr,dict_comp_yr):
    #dictionary to hold assessment updates
    assup = {}
    counter = 1
    #iterate over each key in base year parcel file and compare second year 
    # parcelids and geometry to assess change
    for key_base_yr in dict_base_yr:
        print counter, ' of ', len(dict_base_yr.keys())
        counter += 1
        #only compare parcelids that are not in second year parcel file, 
        # assumes no change if parcel id exists
        if key_base_yr not in dict_comp_yr.keys():
            for key_comp_yr in dict_comp_yr.keys():
                centroid_base_yr = dict_base_yr[key_base_yr].centroid
                centroid_comp_yr = dict_comp_yr[key_comp_yr][0].centroid
                #Identical parcel, new parcel id
                if dict_base_yr[key_base_yr].almost_equals(dict_comp_yr[key_comp_yr][0], 3):
                    assup[key_base_yr] = [dict_comp_yr[key_comp_yr][1], 
                                            dict_comp_yr[key_comp_yr][2]]
                    dict_comp_yr.pop(key_comp_yr)               
                #New parcel split from old
                elif centroid_comp_yr.within(dict_base_yr[key_base_yr]):
                    if key_base_yr not in assup.keys():
                        assup[key_base_yr] = [dict_comp_yr[key_comp_yr][1], 
                                                dict_comp_yr[key_comp_yr][2]]
                    else:
                        assup[key_base_yr][0] += dict_comp_yr[key_comp_yr][1]
                        assup[key_base_yr][1] += dict_comp_yr[key_comp_yr][2]
                #New parcel consolidated from old
                elif centroid_base_yr.within(dict_comp_yr[key_comp_yr][0]):
                    pct = dict_base_yr[key_base_yr].area / dict_comp_yr[key_comp_yr][0].area
                    assup[key_base_yr] = [(dict_comp_yr[key_comp_yr][1] * pct), 
                                        (dict_comp_yr[key_comp_yr][2] * pct)]
        #identical parcel id, carry over full value and assign to year 1
        else:
            assup[key_base_yr] = [dict_comp_yr[key_base_yr][1], 
                                    dict_comp_yr[key_base_yr][2]]
            dict_comp_yr.pop(key_base_yr)
    #custom method that I use for creating tables, 
    # using standard method for writing dictionary will suffice
    tbl.to_csv(assup, ("C:\Data\Scratch\Parcel_Comparison\"
                        "ORIG_ASMT_COMP_12_to_13_COMPARISON.csv"),1,
               True,['PARCELID', 'RTOTAPR', 'RTOTASMT'])
    

if __name__ == '__main__':
    t0 = time.time()
    print "Start"
    #create dictionary with base year values    
    with fiona.open(("C:\Data\Scratch\Parcel_Comparison\"
                        "ORIG_2012_Set.shp") as p:
        dict_yr_1 = {x['properties']['PARCELID']:shape(x['geometry'])for x in p}
    #create dictionary with comparison year values
    with fiona.open(("C:\Data\Scratch\Parcel_Comparison\"
                        "ORIG_2013_Set.shp")) as p:
        dict_yr_2 = {x['properties']['PARCELID']: 
                  (shape(x['geometry']), x['properties']['RTOTAPR'],
                   x['properties']['RTOTASMT']) for x in p}
    main(dict_yr_1, dict_yr_2)
    print "Finished"
    print (time.time()) - t0, "seconds processing time"



