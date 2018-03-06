import pandas as pd
import sys
from caeser import utils
import numpy as np
import os
from config import cnx_params


engine = utils.connect(**cnx_params.blight)

df = pd.read_sql("""select geoid10, name10, totpop, pct_wh, pct_pov_tot, \
                    library_dist, commcenter_dist, cmgrdn_dist, park_dist \
                    from summary_cen_tract_2010\
                    where substring(geoid10, 1,5) = '47157'""", engine)

"""distance measures, calculated before removing high poverty zones to reflect
county averages"""
dist = [col for col in df.columns if col.split('_')[-1] == 'dist']
#scale distance measures to 0 - 1 value
stds = []
standardize = lambda x: (x - x.min()) / (x.max() - x.min())
for col in dist:
    dist_std = col+'_std'
    df[dist_std] = standardize(df[col])#(df[col] - df[col].min()) / (df[col].max() - df[col].min())
    print col, '\n\t', df[dist_std].describe(), '\n'
    stds.append(dist_std)

#add all standardize distances together and scale between 0 and 1
df['dist_tot_std'] = standardize(df[stds].sum(axis=1))


#remove tracts with more than 30% poverty and less than county avg 53% white pop
df = df[(df.pct_pov_tot < 30.)&(df.pct_wh > 53.)]
df.to_csv('housing_selection.csv', index=False)


epa_file = ("EJSCREEN_Full_V3_USPR_TSDFupdate.csv")
epa = pd.read_csv(epa_file, dtype={'ID':np.str})
epa.columns = [col.lower() for col in epa.columns]
epa = epa[epa.id.str[:5] == '47157'] 

