import pandas as pd
import os
import sys
sys.path.append('/home/nate/source')
from caeser import utils
from config import cnx_params

os.chdir("/home/nate/dropbox-caeser/Data/MIDT/clean_stat/mlk50_survey")
engine = utils.connect(**cnx_params.blight)

par = pd.read_sql("select parcelnumb from clean_stat.survey_prop", engine)
survey_csv = "SW_CleanStat_Feb2018.csv"
survey_xls = pd.read_csv(survey_csv)
survey_xls.columns = [col.lower().replace(' ', '_') for col in survey_xls.columns]
survey_xls.loc[survey_xls.data_points.isnull(), "data_points"] = ""
survey_xls.loc[survey_xls.infrastructure.isnull(), "infrastructure"] = ""

ct = lambda x: len(x.split(','))

survey_xls['data_points_count'] = survey_xls.data_points.apply(ct)
survey_xls['infrastructure_count'] = survey_xls.infrastructure.apply(ct)

prop = (survey_xls[~survey_xls.parcelnumb
         .isin(par.parcelnumb
         .tolist())]
         [['parcelnumb', 'address']])
survey, inf = list(), list()

#get current highest survey id and increment index by that value 
sql_id = "select max(survey_id) from clean_stat.survey_issues"
survey_id = engine.execute(sql_id).fetchone()[0] + 1
survey_xls.index += survey_id

for i in survey_xls.index:
    row = survey_xls.loc[i]
    pid = row.parcelnumb
    survey_id = i
    #lambda function to handle instances where a surveyed item had multiple
    #locations (e.g. WB S E N)
    split_location = lambda x: [[x[0], i] for i in x[1:]]
    #insert new rows into survey table
    if row.data_points_count == 1:
        if row.data_points != "":
            new_rows = split_location(row.data_points.strip().split(" "))
            survey.extend([[pid, row.created_at, x[0], x[1], 
                        survey_id, row.surveyor] for x in new_rows])
    else:
        for li in row.data_points.split(','):
            new_rows = split_location(li.strip().split(" "))
            survey.extend([[pid, row.created_at, x[0], x[1], 
                        survey_id, row.surveyor] for x in new_rows])
    #insert new rows into infrastructure table
    if row.infrastructure_count == 1:
        if row.infrastructure != "":
            infs = split_location(row.infrastructure.strip().split(" "))
            inf.extend([[pid, x[0], x[1], survey_id] for x in infs])
    else:
        for ri in row.infrastructure.split(','):
            infs = split_location(ri.strip().split(" "))
            inf.extend([[pid, x[0], x[1], survey_id] for x in infs])


df_survey = pd.DataFrame(columns=['parcelnumb', 'date', 'li_type', 'li_loc',
                                'survey_id', 'surveyor'])
df_inf = pd.DataFrame(columns=['parcelnumb', 'inf_type', 'inf_loc', 
                               'survey_id'])
for s in survey:
    df_survey.loc[len(df_survey)] = s
for i in inf:
    df_inf.loc[len(df_inf)] = i

prop.to_sql("survey_prop", engine, schema="clean_stat", 
        if_exists="append", index=False)
df_survey.to_sql("survey_issues", engine, schema="clean_stat", 
                    if_exists="append", index=False)
df_inf.to_sql("infrastructure", engine, schema="clean_stat", 
                    if_exists="append", index=False)

def update_views():
    q_li = "select distinct(li_type) lit from clean_stat.survey_issues"
    li_types = engine.execute(q_li).fetchall()
    li_types = [li[0] for li in li_types]
    q_li_view = ("drop materialized view if exists clean_stat.li_type_{0};"
                 "create materialized view clean_stat.li_type_{0} as "
                 "select row_number() over () as uid, li_type, "
                    "count(li_type), parcelnumb, "
	    	    "st_centroid(wkb_geometry) geom_pt "
                 "from clean_stat.survey_issues, sca_parcels "
                 "where li_type = '{1}' and parcelnumb = parcelid "
                 "group by parcelnumb, li_type, wkb_geometry")
    for li_type in li_types:
        engine.execute(q_li_view.format(li_type.lower(), li_type))

def heatmap(table):
    """TODO
    Still having issues running the alg from terminal
    """

    from qgis.core import QgsVectorLayer, QgsDataSourceURI, QgsApplication
    from PyQt4.QtGui import QApplication
    qgs = QgsApplication([], True, None)
    qgs.setPrefixPath("/usr/bin", True)
    sys.path.append('/usr/share/qgis/python/plugins')
    from processing.core.Processing import Processing
    qgs.initQgis()
    
    #app = QApplication([])
    Processing.initialize()
    Processing.updateAlgsList()
    params = cnx_params.blight
    db = params['db']
    host = params['host']
    user = params['user']
    pwd = params['password']
    dbcnx = ("dbname='{db}' host={host} port=5432 "
             "user='{user}' password='{password}'")
    id_field="uid"
    table = ("(select row_number() over () as uid, li_type, "
                    "count(li_type), parcelnumb, "
	    	    "st_centroid(wkb_geometry) geom_pt "
                 "from clean_stat.survey_issues, sca_parcels "
                 "where li_type = '{1}' and parcelnumb = parcelid "
                 "group by parcelnumb, li_type, wkb_geometry)")
    uri = "{0} key={1} table={2} (geom_pt) sql="
    lyr = QgsVectorLayer(uri.format(dbcnx.format(**params),
                                    id_field,
                                    table.format("ll", "LL"),
                                    ),
                        "li_type_ll_test", "postgres")
    Processing.runAlgorithm("saga:kerneldensityestimation", lyr,
                            "count", 300, 0, 
                            ("754005.201649,760791.665104,"
                             "316181.236576,323293.188451"),
                             25,0,"/home/nate/temp/ll_del.tif")
    uri = QgsDataSourceURI()
    uri.setConnection(host, '5432', db, user, pwd, sslmode=uri.SSLrequire)
    uri.setDataSource('clean_stat', table, 'geom_pt', "", "uid")
    lyr = QgsVectorLayer(uri.uri(False), table, "postgres")
    processing.runalg("saga:kerneldensityestimation", lyr, 
                        "count", 300., 0, "", 25, 0, "/home/nate/temp/ll_del.tif")
