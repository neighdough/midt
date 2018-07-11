from caeser import utils
import pandas as pd
from sklearn.cluster import KMeans, MiniBatchKMeans
import matplotlib.pyplot as plt
import numpy as np
import os
from config import cnx_params

engine = utils.connect(**cnx_params.blight)
os.chdir('/home/nate/dropbox-caeser/Data/MIDT/bcs_report/data')
qtract = ("select geoid10, count(parcelid), sum(rating), "
        "sum(rating)/count(parcelid) avg "
        "from (select p.parcelid, rating, wkb_geometry from "
            "sca_parcels p, bcs_property b "
            "where b.parcelid = p.parcelid) p, "
        "geography.tiger_tract_2010 t "
        "where st_within(st_centroid(p.wkb_geometry), t.wkb_geometry) "
        "group by geoid10")
tract = pd.read_sql(qtract, engine)

qsubdist = ("select area, count(parcelid), sum(rating), "
        "sum(rating)/count(parcelid) avg "
        "from (select p.parcelid, rating, wkb_geometry from "
            "sca_parcels p, bcs_property b "
            "where b.parcelid = p.parcelid) p, "
        "geography.com_ec_subdistricts b "
        "where st_within(st_centroid(p.wkb_geometry), b.wkb_geometry) "
        "group by area")
subdist = pd.read_sql(qsubdist, engine)


q = ("select p.parcelid, rating, st_x(st_centroid(wkb_geometry)) x, "
        "st_y(st_centroid(wkb_geometry)) y "
        "from sca_parcels p join "
        "bcs_property bcs on p.parcelid = bcs.parcelid "
        "where substring(p.parcelid, 1,1) = '0'")
df = pd.read_sql(q, engine)

scale = lambda x: (x - x.min()) / (x.max() - x.min())
rescale = lambda x, y: x * (y.max() - y.min()) + y.min()
X = df[[col for col in df.columns if col != 'parcelid']]
for col in X.columns:
    X[col] = scale(X[col])

def optimalK(data, nrefs=3, maxClusters=15, step=1):
    """
    Calculates KMeans optimal K using Gap Statistic from Tibshirani, 
	Walther, Hastie
	modified from https://anaconda.org/milesgranger/gap-statistic
    Params:
        data: ndarry of shape (n_samples, n_features)
        nrefs: number of sample reference datasets to create
        maxClusters: Maximum number of clusters to test for
    Returns: (gaps, optimalK)
    """
    RANDOM_STATE = 12345
    gaps = np.zeros((len(range(1, maxClusters, step)),))
    resultsdf = dict()
    for gap_index, k in enumerate(range(1, maxClusters, step)):
        # Holder for reference dispersion results
        refDisps = np.zeros(nrefs)

        # For n references, generate random sample and perform kmeans
	# getting resulting dispersion of each loop
        for i in range(nrefs):
            
            # Create new random reference set
            randomReference = np.random.random_sample(size=data.shape)
            
            # Fit to it
            mbk = MiniBatchKMeans(k, random_state=RANDOM_STATE)
            mbk.fit(randomReference)
            
            refDisp = mbk.inertia_
            refDisps[i] = refDisp

        # Fit cluster to original data and create dispersion
        mbk = MiniBatchKMeans(k, random_state=RANDOM_STATE)
        mbk.fit(data)
        
        origDisp = mbk.inertia_

        # Calculate gap statistic
        gap = np.log(np.mean(refDisps)) - np.log(origDisp)
	print k, '\t', gap, '\n'
        # Assign this loop's gap statistic to gaps
        gaps[gap_index] = gap
        resultsdf[k] = gap
        #resultsdf = resultsdf.append({'clusterCount':k, 'gap':gap}, 
	#		ignore_index=True)

    # Plus 1 because index of 0 means 1 cluster is optimal, 
    # index 2 = 3 clusters are optimal
    return (gaps.argmax() + 1, resultsdf)  

#Test: reclass 5 to 4 and rerun
df.loc[df.rating == 5, "rating"] = 4

km = KMeans(n_clusters=1000, n_jobs=-1)
km.fit(X)        
cc = pd.DataFrame.from_records(km.cluster_centers_, 
        columns=['rating', 'x', 'y'])
cc.x = rescale(cc.x, df.x)
cc.y = rescale(cc.y, df.y)
df['labels'] = pd.Series.from_array(km.labels_,name='labels')
pt = 'SRID=2274:Point({0} {1})'
geom = lambda x: pt.format(x['x'], x['y'])
df['wkb_geometry'] = df[['x', 'y']].apply(geom, axis=1)
df.to_sql("bcs_rating_kmeans", engine, if_exists="replace")

q_temp = ("create temporary table cluster_poly as "
          "with s as "
          "(select labels, count(parcelid) npar, "
          "st_convexhull(st_collect(wkb_geometry)) geom "
          "from bcs_rating_kmeans where rating=4 "
          "group by labels) "
          "select * from s ")
engine.execute(q_temp)
bound = pd.read_sql(("select labels, npar, st_area(geom)/43560 acres, "
                     "npar/(st_area(geom)/43560) prop_den " 
                         "from cluster_poly"), engine)

q_vac = ("select labels, npar, st_area(p.geom)/43560 acres, "
	 "npar/(st_area(p.geom)/43560) prop_den, "
	 "count(index),count(index)/(st_area(p.geom)/43560) vac_den "
	 "from bcs_rating_kmeans_cluster_poly p, "
	 "(select st_transform(ST_SetSRID(ST_Point(lon, lat),4326), 2274) geom, index "
	 "from mlgw_disconnects where load_date = '2018-02-05') m "
	 "where st_intersects(m.geom, p.geom) "
	 "group by labels, npar, acres, prop_den"
        )
vac = pd.read_sql(q_vac, engine)
vac['combined'] = scale(vac.prop_den) + scale(vac.vac_den)

cc.to_csv('cluster_center_k{}_06222018.csv'.format(str(k)))
df.to_csv('parcel_file_k{}_06222018.csv'.format(str(k)))


