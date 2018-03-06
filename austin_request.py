import pandas as pd
from caeser import utils
import geopandas as gpd
from config import cnx_params

engine = utils.connect(**cnx_params.blight)

req = pd.read_sql("""select geoid, c.wkb_geometry, sum(numreqs) totreqs 
                    from geography.cen_bg_2016 c
                    join (select parcelid, numreqs, wkb_geometry 
                        from sca_parcels join combined_table on parcelid = parid
                        where reported_date >= '2012-01-01' and 
                            reported_date < '2015-01-01') p
                    on st_within(st_centroid(p.wkb_geometry), 
                        c.wkb_geometry)
                    group by geoid, c.wkb_geometry""", engine)

par = pd.read_sql("""select parid, reported_date, startyr, geoid
                            from combined_table
                            join sca_parcels p on parcelid = parid
                            join geography.cen_bg_2016 c on 
                            st_within(st_centroid(p.wkb_geometry), 
                            c.wkb_geometry)""", engine)

bg =gpd.read_postgis("""select * from geography.cen_bg_2016""", engine, 
                        'wkb_geometry', 2274)

os.chdir('/home/nate/sharedworkspace/Data/Assessor')
years = ['2012','2013', '2014', '2015']

sales = pd.read_csv('2016/SALES.txt')
sales.columns = [col.lower() for col in sales.columns]
pardat = pd.read_csv('2016/PARDAT.txt')
pardat.columns = [col.lower() for col in pardat.columns]
pardat = pardat.merge(par[['parid', 'geoid']], on='parid')
merge = sales.merge(pardat, on='parid')
merge['year'] = merge.saledt.str.split('/').str[-1].str.split(' ').str[0]
par_keep = merge[(merge.year.isin(years)) & (merge.instrtyp == 'WD')]

transno = par_keep.transno.unique()
for i in par_keep.index:
    if par_keep.loc[i].numpars > 1. and par_keep.loc[i].price > 0.:
        price_dist = par_keep.loc[i].price / par_keep.loc[i].numpars
        par_keep[par_keep.transno == par_keep.loc[i].transno]['price_dist'] = price_dist

price_adj = lambda row: utils.inflate(row['year'], '2015', row['price'])
par_keep['price_adj'] = par_keep.apply(price_adj, axis=1)

med_price = par_keep.groupby('geoid').agg({'price_adj':'median'})

start_notnull = par.groupby('geoid').agg({'startyr': lambda x: x.notnull().sum(),
                                            'parid':'count'})

combined = med_price.merge(start_notnull, left_index=True, right_index=True)
combined['geoid'] = combined.index
combined = combined.merge(req[['geoid','totreqs']], on='geoid')
combined.rename(columns={'startyr':'num_deliq', 'parid':'num_pars'}, inplace=True)
combined['pct_deliq'] = combined.num_deliq/combined.num_pars
combined.to_csv('/home/nate/dropbox-caeser/Data/MIDT/Temp/bg_report.csv', 
                index=False)

