'''
Created on Dec 11, 2013

@author: nfergusn
'''
import arcpy

mxd = arcpy.mapping.MapDocument("e:\data\midt\maps\Pop_Change_1960-2010.mxd")

outpath = 'e:\data\midt\exports\chamber\\'

lyrs = arcpy.mapping.ListLayers(mxd)
for lyr in lyrs:
    if lyr.isGroupLayer:
        print lyr.name
        lyr.visible = True        
        grp_lyrs = arcpy.mapping.ListLayers(lyr)      
        city_boundary = grp_lyrs[1]
        pop_data = grp_lyrs[2]
        for elem in arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT"):
            if elem.name == 'date':
                elem.text = lyr.name
                elem.elementPositionX = 0.6502
                elem.elementPositionY = 6.5719
            elif elem.name == 'poptotals':
                arcpy.SelectLayerByLocation_management(pop_data.longName, 
                    "HAVE_THEIR_CENTER_IN", city_boundary, 
                    selection_type="NEW_SELECTION")
                tbl = arcpy.Frequency_analysis(pop_data.longName, "in_memory", 
                                               frequency_fields="County", 
                                               summary_fields="POP")
                arcpy.SelectLayerByAttribute_management(pop_data, 
                                            selection_type="CLEAR_SELECTION")  
                for row in arcpy.da.SearchCursor(tbl,"POP"):
                    tot_pop = row[0]
                for row in arcpy.da.SearchCursor(city_boundary, "Shape_Area"):
                    tot_area = row[0]
                    sqmi = tot_area/27878400            
                pop_density = int(tot_pop/(sqmi))
                out_text =  ("Population (25 people per dot): {:,}\n"
                            "Square Miles: {:,}\nDensity(per Sq Mi): {:,}")
                elem.text = out_text.format(int(tot_pop),int(sqmi),pop_density)
                elem.elementPositionX = 0.2016
                elem.elementPositionY = 4.0835                                                                                                      
        arcpy.mapping.ExportToJPEG(mxd, outpath + lyr.name, resolution=300)
        lyr.visible = False

        