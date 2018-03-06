'''
Created on Dec 9, 2014

@author: nfergusn
'''
import arcpy
import os
import statistics as stats

arcpy.env.workspace = 'e:\data\midt\idt.gdb'
arcpy.env.overwriteOutput = True

walkshed = 'Analysis\school_comm_center_walkshed'
blightprob = 'Analysis\dssg_blightprob'

upcur = arcpy.da.UpdateCursor(walkshed, ['Shape@','FacilityID','mdnprob', 'ovr50', 'total', 'pctovr'])
for rec in upcur:
    wlk_lyr = arcpy.MakeFeatureLayer_management(walkshed, 'wlkshdlyr', """FacilityID = {0}""".format(rec[1]))
    blight_lyr = arcpy.MakeFeatureLayer_management(blightprob, 'probs')
    arcpy.SelectLayerByLocation_management(blight_lyr, "INTERSECT", wlk_lyr, selection_type='NEW_SELECTION')
    vals = [row[0] for row in arcpy.da.SearchCursor(blight_lyr, 'blightprob')]
    count = arcpy.GetCount_management(blight_lyr)
    total = int(count.getOutput(0))
    ovr = sum(x > .5 for x in vals)
    if vals:
        rec[2] = stats.median(vals)
        rec[3] = ovr
        rec[4] = total
        rec[5] = float(ovr)/float(total)
    else:
        rec[2], rec[3],rec[4],rec[5] = 0,0,0,0        
    upcur.updateRow(rec)
        

                                
    
    
if __name__ == '__main__':
    pass