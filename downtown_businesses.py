"""

Methodology for Target-Density Weighting (TDW) Interpolation taken from:

Target-Density Weighting Interpolation and Uncertainty Evaluation for Temporal
Analysis of Census Data. Schroeder, Johnathan P., Geographical Analysis 39, 2007.

https://onlinelibrary.wiley.com/doi/pdf/10.1111/j.1538-4632.2007.00706.x

"""

import pandas as pd
import os
from caeser import utils
from config import cnx_params
import matplotlib.pyplot as plt
from sklearn import linear_model

engine_blight = utils.connect(**cnx_params.blight)
engine_wwl = utils.connect(**cnx_params.wwl_2017)

def tdw(ast, at, zst, zt, ys):
    """
        Sum(((Ast/At)*zt)/Sum((Ast/At)*zt)*Ys)
        Ast:                sqmi, square miles for boundary intersection
        At:                 sqmi_bound, total square miles for zip 
        zt:                 sum_addr, total number of addresses within boundary
        zst:                num_addr, number of addresses within intersection
        Ys:                 num_meters, total number of meters within zip code
        Sum((Ast/At)*zt)):  sqmi, sqmi_bound, sum_addr
    """
    return (((ast / at) * zst) / ((ast / at ) * zt)) * ys

q_addr_all = ("with zip_select as ( "
            "select case when zip = '38113' then '38106' "
            "when zip = '38131' then '38116' "
            "when zip = '38132' then '38116' "
            "else zip end as zip "
          "from geography.sc911_address) "
         "select zip, count(zip) num from zip_select "
         "group by zip order by zip"
         )
addr_all = pd.read_sql(q_addr_all, engine_blight)
addr_all.zip = addr_all.zip.astype(str)

# q_addr = ("with bz_intersection as ("
            # "select b.name, geoid10 zip, st_area(b.wkb_geometry)/27878400 sqmi_bound,"
                    # "st_intersection(b.wkb_geometry, z.wkb_geometry) geom "
            # "from geography.boundaries b, geography.tiger_zcta_2010 z "
                    # "where st_intersects(b.wkb_geometry, z.wkb_geometry) "
            # "and b.name in('CBID', 'Parkways', 'The Core', 'Main Street Mall') "
            # ")"
            # "select bz_intersection.name, bz_intersection.zip, sqmi_bound, "
                # "st_area(geom)/27878400 sqmi, "
                # "sum_addr, num_addr from bz_intersection "
            # "left join (select b.name, count(a.wkb_geometry) num_addr, b.zip "
                      # "from geography.sc911_address a, bz_intersection b "
                   # "where st_intersects(a.wkb_geometry, b.geom) "
                   # "group by b.name, b.zip"
                  # ") sc_addresses "
            # "on sc_addresses.name = bz_intersection.name "
            # "and sc_addresses.zip = bz_intersection.zip "
            # "left join(select b.name, count(a.wkb_geometry) sum_addr "
                      # "from geography.boundaries b, geography.sc911_address a "
                      # "where st_intersects(a.wkb_geometry, b.wkb_geometry) "
                       # "and b.name in ('CBID', 'Parkways', 'The Core', 'Main Street Mall') "
                        # "group by b.name) addr_all "
            # "on addr_all.name = bz_intersection.name"
	 # )
q_addr = ("with bz_intersection as ("
            "select b.name, geoid10 zip, st_area(b.wkb_geometry)/27878400 sqmi_bound,"
                    "st_intersection(b.wkb_geometry, z.wkb_geometry) geom "
            "from geography.boundaries b, geography.tiger_zcta_2010 z "
                    "where st_intersects(b.wkb_geometry, z.wkb_geometry) "
            "and b.name in('CBID', 'Parkways', 'The Core', 'Main Street Mall') "
            ")"
            "select bz_intersection.name, bz_intersection.zip, "
                "num_addr/st_area(geom)/27878400 addr_density "
                "from bz_intersection "
            "left join (select b.name, count(a.wkb_geometry) num_addr, b.zip "
                      "from geography.sc911_address a, bz_intersection b "
                   "where st_intersects(a.wkb_geometry, b.geom) "
                   "group by b.name, b.zip"
                  ") sc_addresses "
            "on sc_addresses.name = bz_intersection.name "
            "and sc_addresses.zip = bz_intersection.zip "
	 )

addr = pd.read_sql(q_addr, engine_blight)
addr.zip = addr.zip.astype(str)
addr.fillna(0, inplace=True)

q_bound_char = ("with bz_intersection as ("
            "select b.name, geoid10 zip,"
            "st_intersection(b.wkb_geometry, z.wkb_geometry) geom "
            "from geography.boundaries b, geography.tiger_zcta_2010 z "
            "where st_intersects(b.wkb_geometry, z.wkb_geometry) "
            "and b.name in('CBID', 'Parkways', 'The Core', 'Main Street Mall') "
            ")"
            "select b.name, b.zip, sum(livunit) tot_livunit, count(distinct luc) distinct_luc, "
            " st_area(b.geom)/27878400 sqmi "
            "from (select wkb_geometry, livunit, luc "
            "from sca_parcels p, sca_pardat "
                      "where parcelid = parid) p, bz_intersection b "
            "where st_intersects(st_centroid(p.wkb_geometry), b.geom)"
            "group by b.name, b.geom, b.zip"
            )
bound_char = pd.read_sql(q_bound_char, engine_blight)

q_mlgw = ("with mlgw_sel as ("
          "select case when zip = '38014' then '38104' "
            "when zip = '38113' then '38106' "
            "when zip = '38129' then '38128' "
            "when zip = '38029' then '38135' "
            "else zip end as zip, count "
            "from environment.mlgw_rates where scat = 'K')"
          "select zip, sum(count) num_meters, st_area(wkb_geometry)/27878400 sqmi_zip "
          "from mlgw_sel "
          "join geography.cen_zip_2010 "
          "on geoid10 = zip "
          "group by zip, wkb_geometry order by zip"
          )

mlgw = pd.read_sql(q_mlgw, engine_wwl)

q_mlgw_char = (
comb = addr.merge(mlgw, how="left", on="zip", suffixes=["_mlgw", "_addr"])

q_zip_model = ("select geoid10 zip, sqmiland, pct_comm, pct_dev, age_comm"
                "age_bldg, pct_mf, strtsdw_pct, age_sf, hu "
                "from summary_cen_zip_2010")
zip_model = pd.read_sql(q_zip_model, engine_wwl)
xy = mlgw.merge(zip_model, how="left", on="zip")
lm = linear_model()
xy.fillna(0, inplace=True)
lm.fit(xy[[col for col in xy.columns if col not in ["zip", "num_meters"]]], xy.num_meters)

q_bound_model = (

