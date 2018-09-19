"""
"""

import pandas as pd
import numpy as np
import os
from caeser import utils
from config import cnx_params
import matplotlib.pyplot as plt
from sklearn import linear_model
from sklearn.model_selection import cross_val_predict
from sklearn.metrics import mean_squared_error, r2_score

engine_blight = utils.connect(**cnx_params.blight)
engine_wwl = utils.connect(**cnx_params.wwl_2017)

os.chdir("/home/nate/dropbox-caeser/Data/MIDT/downtown_businesses")
q_bus_info = (
    "select bus_name, address, b.name district, "
        "regexp_replace(msg, '- ', '') land_use, livunit "
	"from  "
	"(select bus_name, match_addr address, msg, livunit, l.wkb_geometry "
	 "from sca_parcels p, sca_pardat, sca_aedit, geography.bp_business_licenses l  "
	    "where parcelid = parid  "
            "and (tble = 'PARDAT' and fld = 'LUC')  "
            "and val = luc "
	    "and st_within(l.wkb_geometry, p.wkb_geometry)) l,  "
	"(select * from geography.boundaries b  "
	    "where name in ('CBID', 'The Core', 'Parkways', 'Main Street Mall')) b "
	"where st_within(l.wkb_geometry, b.wkb_geometry)  "
        )
bus_info = pd.read_sql(q_bus_info, engine_blight)
bus_info.to_csv("deliverables/business_info.csv")

q_mlgw = ("with mlgw_sel as ("
          "select case when zip = '38014' then '38104' "
            "when zip = '38113' then '38106' "
            "when zip = '38129' then '38128' "
            "when zip = '38029' then '38135' "
            "else zip end as zip, count "
            "from environment.mlgw_rates where scat = 'K')"
          "select zip, sum(count) num_meters "
          "from mlgw_sel "
          "join geography.cen_zip_2010 "
          "on geoid10 = zip "
          "group by zip, wkb_geometry order by zip"
          )

mlgw_meters = pd.read_sql(q_mlgw, engine_wwl)

q_bound_model = (
    "select b.{col_name}, pct_dev, age_bldg, num_sf, strtsdw_pct, "
            "strt_miles, st_area(b.wkb_geometry)/27878000 sqmi,"
            "num_addr "
    "from "
    "geography.{geography} b "
    "join "
    "(select t.{col_name}, 100 * sum(case when luc <> '000'  "
                "then 1 else 0 end)::numeric/ count(luc) as pct_dev "
        "from (sca_parcels  "
        "join sca_pardat on parid = parcelid) as b  "
        "join geography.{geography} as t on  "
        "st_within(st_centroid(b.wkb_geometry), t.wkb_geometry)  "
        "group by t.{col_name} ) dev "
    "on dev.{col_name} = b.{col_name} "
    "join  "
    "(select t.{col_name}, 2014 - avg(yrblt) as age_bldg "
        "from (sca_parcels  "
        "join (select parid, yrblt from sca_comdat "
        "union select parid, yrblt from sca_dweldat) as p on parid = parcelid) as b  "
        "join geography.{geography} as t on  "
        "st_within(st_centroid(b.wkb_geometry), t.wkb_geometry)  "
        "group by t.{col_name}) bldg "
    "on bldg.{col_name} = b.{col_name} "
    "full join "
    "(select t.{col_name}, count(parcelid) as num_sf "
        "from (select parcelid, wkb_geometry from sca_parcels, sca_pardat  "
        "where parid = parcelid and luc = '062') b "
        "join geography.{geography} as t on  "
        "st_within(st_centroid(b.wkb_geometry), t.wkb_geometry)  "
        "group by t.{col_name}) sf "
    "on sf.{col_name} = b.{col_name} "
    "join "
    "(select s.{col_name},  "
        "case  "
        "when (s.cnt_sdw + s.cnt_no_sdw) > 0  "
            "then (s.cnt_sdw::float / (s.cnt_sdw + s.cnt_no_sdw)) * 100  "
        "else 0  "
        "end as strtsdw_pct "												 
        "from (select b.{col_name}, " 
        "sum(case when s.sidewalk='YES' then 1 else 0 end) as cnt_sdw,  "
        "sum(case when s.sidewalk='NO' then 1 else 0 end) as cnt_no_sdw "
                "from geography.{geography} as b  "
        "left join geography.streets_with_sdw as s " 
        "on ST_DWithin(s.wkb_geometry, b.wkb_geometry, 2) group by b.{col_name}) as s "
    ") sdw "
    "on sdw.{col_name} = b.{col_name} "
    "join  "
    "(select b.{col_name}, sum(st_length(s.wkb_geometry) / 5280) strt_miles "
        "from geography.{geography} as b  "
        "left join geography.streets_with_sdw as s  "
        "on ST_DWithin(s.wkb_geometry, b.wkb_geometry, 2) group by b.{col_name} "
    ")strt_mi  "
    "on strt_mi.{col_name} = b.{col_name} "
    "join  "
    "(select b.{col_name}, count(a.wkb_geometry) num_addr  "
         "from geography.sc911_address a, geography.{geography} b "
         "where st_within(a.wkb_geometry, b.wkb_geometry) group by b.{col_name} "
    ") addr "
    "on addr.{col_name} = b.{col_name} "
)

bound = pd.read_sql(q_bound_model.format(col_name="name", geography="boundaries"), 
        engine_blight)
mlgw = pd.read_sql(q_bound_model.format(col_name="geoid10", geography="tiger_zcta_2010"),
        engine_blight)
mlgw.columns = [col if col != "geoid10" else "zip" for col in mlgw.columns]
xy = mlgw_meters.merge(mlgw, how="left", on="zip")
ignore = ["zip", "sqmi", "strt_miles"]#, "strtsdw_pct", "num_sf",]# "num_addr"]
keep = [col for col in mlgw.columns if col not in ignore]
#xy["strt_sqmi"] = xy.strt_miles/xy.sqmi
#xy["addr_sqmi"] = xy.num_addr/xy.sqmi
#keep.extend(["strt_sqmi", "addr_sqmi"])
xy.fillna(0, inplace=True)
bound.fillna(0, inplace=True)

lm = linear_model.LinearRegression()
x = xy[keep]
y = xy.num_meters
lm.fit(x, y)
pred_lm = cross_val_predict(lm,x, y, cv=10)
r2 = r2_score(y, pred_lm)
mse = mean_squared_error(y,pred_lm)

y_pred = lm.predict(bound[keep])
bound["est_meters"] = np.rint(y_pred)
districts = ["CBID", "The Core", "Parkways", "Main Street Mall"]
bound[bound.name.isin(districts)][["name", "num_addr", "est_meters"]].to_csv(
        "deliverables/utility_estimates.csv")


