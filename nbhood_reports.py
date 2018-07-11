"""
Generate reports for code violators or neighborhood pre-defined neighborhood.
Takes the desired month for the report as input and produces charts and maps 
with information about the top code violators for that month. It also updates 
a running list of the top violators for the current year.

Usage:
    nbhood_reporty.py violator <month>... [--year=<year>]  
    nbhood_reporty.py neighborhood [neighborhood_name]

Options:
    -h, --help      : show help document
    violator        : Generate report for top code violators by ownership
    <month>         : Month or months to be used as the range for the 
                      analysis. Months can either be integer or text values 
                      (i.e. 1 or Jan)
    --year=<year>   : Optional parameter to change the year that the report
                      should be geneated for
    neighborhood    : Generate neighborhood
        

Example:
    $ python reports.py may
TODO:
    *methods:
        - run_report
        - check_directory: check if directory for report exists, create if not
        - 
    *edits:
        -Adjust labels on Total Requests 2008 to 2017 to make chart a bit 
            easier to understand
        -Check labels for line chart for Code Violations over time to make 
            sure that the complete label is shown (e.g. Accumulation of 
            Stagnant)
        -Add title for Population change chart
        -Check to make sure full history is shown (code violations only 
            displayed to 2012)

    *set up project
        - create directory using neighborhood name
        - switch to project directory
    

"""
from caeser import utils
import calendar
from config import cnx_params
import datetime
from docopt import docopt
from math import log
import matplotlib.pyplot as plt
import pandas as pd
from PyQt4.QtCore import QFileInfo, QSize
from PyQt4.QtXml import QDomDocument
from PyQt4.QtGui import QImage, QPainter
from pywaffle import Waffle
import numpy as np
import os
from qgis.core import (QgsProject, QgsComposition, QgsApplication, 
        QgsProviderRegistry, QgsRectangle, QgsPalLayerSettings)
from qgis.gui import QgsMapCanvas, QgsLayerTreeMapCanvasBridge
from scipy.interpolate import spline
import seaborn as sns
from sqlalchemy import text
import sys
sys.path.append('/home/nate/source')
from titlecase import titlecase


params = cnx_params.blight#_local
engine_blight = utils.connect(**params)
#engine_census = utils.connect(**cnx_params.census)
pd.set_option('display.width', 180)
os.chdir(('/home/nate/dropbox-caeser/Data/MIDT/Data_Warehouse/'
          'reports/neighborhood/generated_reports/KlondikeSmokeyCityCDC/'))
nbhood = 'Klondike Smokey City CDC'
cur_year = datetime.datetime.today().year

def run_violator_report(period, yr=None):
    """
    Primary function that handles the query and generates the main output
    for the report.
    
    Args:
        period (int): Numeric value of the month for the report (i.e. 5 = May,
        6 = June, etc.)
        yr (int, optional): Year for the report
    Returns:
        None
    TODO:
        Adjust selection query to adjust start day and end day based upon
        input months.
    """
    os.chdir(("/home/nate/dropbox-caeser/Data"
              "/MIDT/Data_Warehouse/reports/"
              "monthly_code_violators"))
    year = yr if yr else cur_year
    if len(period) == 1:
        start = period[0]
        finish = period[0]
    elif len(period) == 2:
        start, finish = period
    else:
        print ("Incorrect input format. Input can either be a single month, "
                "or a start month and finish month for a range.")
        return 
    start_date = format_date(start, year)[:-2] + "01"
    end_date = format_date(finish, year)
    print "Start: ", start_date, "\nEnd: ", end_date

    ignore = ["city of memphis", "shelby county tax sale", 
              "health educational and housing"]

    q = ("select trim(both ' ' from own1) as own, "
            "par_adr, p.parcelid, "
	    "reported_date, request_type, summary, lon, lat " 
        "from " 
	    "(select parcelid, "
                "concat(adrno, ' ', adrstr, ' ', adrsuf, ', ', zip1) par_adr, "
    		"summary, reported_date, "
                "split_part(request_type, '-', 2) request_type, "
     		"st_x(st_centroid(wkb_geometry)) lon, "
                "st_y(st_centroid(wkb_geometry)) lat "
    	    "from sca_pardat, sca_parcels, com_incident "
    	"where parcelid = parid "
    	    "and parcelid = parcel_id "
    	    "and reported_date "
    		"between '{start_date}'::timestamp "
                    "and '{end_date}'::timestamp) p,"
	"sca_owndat "
        "where parcelid = parid "
        "and lower(own1) not similar to '%({ignore})%' "
        "order by own;")

    #there's a known bug in pandas where using '%' in query causes read_sql
    #to fail unless wrapped in sqlalchemy.text

    def acronyms(word, **kwargs):
        if word in ["CSMA", "LLC", "(RS)", "RS", "FBO", "II"]:
            return word.upper()

    vals = {"start_date": start_date, 
            "end_date": end_date,
            "ignore": "|".join(ignore)}
    df = pd.read_sql(text(q.format(**vals)), engine_blight)
    #remove references to city and county owned properties
    df.own = df.own.apply(titlecase, args=[acronyms]) 
    own_group = (df.groupby('own', as_index=False)
                    .count()
                    .sort_values(by='parcelid', ascending=False)
                    .head(10))
    own_group.reset_index(drop=True, inplace=True)
    own_group['rank'] = own_group.index + 1
    own_group.rename(columns={"parcelid":"count"}, inplace=True)
    own_group = own_group[["own", "count", "rank"]]
    make_map(df[df.own.isin(own_group.own.tolist())],
            own_group)
    own_violations = (df[df.own.isin(own_group.own.tolist())]
                        .groupby(['own', 'request_type'])['request_type']
                        .count()
                        .unstack(level=-1, fill_value=0))

    #get index values in sort order and use to plot from most to least
    sort_order = own_violations.sum(axis=1).sort_values(ascending=False).index
    ax = own_violations.loc[sort_order].plot(kind='bar', 
                                            stacked=True, 
                                            figsize=(10,8),
                                            colormap='BrBG')
    ax.set_xlabel('Owner Name')
    ax.set_ylabel('Number of Violations')
#    plt.figure(figsize=(10, 8),dpi=300, facecolor='white')
    labels = ax.get_xticklabels()
    
    plt.setp(labels, rotation=90, fontsize=8)
    plt.legend(title="Violation Type", fontsize=8)
    plt.tight_layout()
    fig_name = 'owner_bar_stacked_{}.jpg'.format(end_date)
    plt.savefig(fig_name, dpi=300)

def make_map(dframe, group):
    """
    dframe=own_props.copy()
    """
    addr_count = dframe.groupby('par_adr', as_index=False).size()
    dframe.drop_duplicates('par_adr', inplace=True)
    dframe = dframe.join(addr_count.to_frame(), on='par_adr')
    dframe.rename(columns={0:'num_vi'}, inplace=True)
#    dframe = dframe.reset_index()
#    dframe = dframe.join(owner_rank, on='own', rsuffix='_rank')
    dframe = dframe.merge(group, on='own')
    dframe.index += 1
    dframe.to_csv('owner_coords.csv', index_label='id', encoding='utf-8')
    
    canvas = QgsMapCanvas()
    project = QgsProject.instance()
    project.read(QFileInfo("map.qgs"))
    bridge = QgsLayerTreeMapCanvasBridge(
            project.layerTreeRoot(), canvas)
    bridge.setCanvasLayers()

def format_date(month, year):
    """
    Formats date strings to be used in SQL query to pull table from DB.
    
    Args:
        month (str):
    Returns:
        Tuple containing formatted date strings in the form 'YYYY-MM-dd' to be
            passed into SQL query to specify date range for the report.
    """
    
    month_abbr = {v.lower(): k for k, v in enumerate(calendar.month_abbr)}
    if not month.isdigit():
        month = month_abbr[month[:3].lower()]
#    else:
#        month = int(month)
    last_day = calendar.monthrange(year, int(month))[1]
    return "{0}-{1}-{2}".format(year, month, last_day)

def get_tract_values(nbhood):
    """
    
    """
    q = ("select geoid10 from geography.tiger_tract_2010 t, "
            "geography.bldg_cdc_boundaries b "
         "where st_intersects(t.wkb_geometry, b.wkb_geometry) "
            "and b.name = '{}'")
    tracts = engine_blight.execute(q.format(nbhood)).fetchall()
    return [i[0] for i in tracts]

def run_census(nbhood=None):

    tables = {'b02001':'Race', 
              'b19013': 'Median Household Income',
              's1701': 'Poverty Status',
              'b08101': 'Means of Transportation to Work',
              'b07001': 'Residence 1 Year Ago',
              's15001': 'Educational Attainment',
              's16001': 'Language Spoken at Home'}
    years = [1970, 1980, 1990, 2000, 2010]
    cols = ", ".join(["pop%s" % str(i)[2:] for i in years])
    tracts = get_tract_values(nbhood)

    q_pop = ("select tractid, {0} from ltdb.ltdb_std_fullcount "
             "where tractid in ('{1}');")
    pop = pd.read_sql(q_pop.format(cols, "', '".join(tracts)), engine_census)
    q_shelby = ("select  tractid, {} from ltdb.ltdb_std_fullcount "
                "where substring(tractid, 1, 5) = '47157';")
    pop_shelby = pd.read_sql(q_shelby.format(cols), engine_census)
    new_cols = lambda x, y: dict(zip(x, y))
    
    pop.rename(new_cols([col for col in pop.columns if 'pop' in col], years), 
                        axis=1, inplace=True)
    pop_shelby.rename(new_cols([col for col in pop_shelby.columns if 'pop' in col], 
                                years), axis=1, inplace=True)

    ax = plt.subplot(111)
    for tract in pop.tractid.unique():
        plt.plot(pop[pop.tractid == tract][years].sum(), label=tract)
    legend_cols = len(pop.tractid.unique())/2
    ax.legend(loc='upper center',bbox_to_anchor=(.5, 1.05), ncol=legend_cols,
            fancybox=True, shadow=True, fontsize=6)
    ax.set_xlabel('Year')
    ax.set_ylabel('Population')
    ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, 
                        loc: "{:,}".format(int(x))))
    plt.tight_layout()

    plt.savefig('./images/pop_change_tracts.jpg', dpi=300)
    plt.close()
    #--- Plots to compare neighborhood with County
    # ax = plt.subplot(111)
    # plt.plot(pop_shelby[years].sum(), label='Shelby County')
    # plt.plot(pop[years].sum(), label=nbhood)
    # ax.legend(loc='best', fontsize=6)
    # ax.set_xlabel('Year')
    # ax.set_ylabel('Population')
    # ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, 
                        # loc: "{:,}".format(int(x))))
    # plt.tight_layout()
    # plt.savefig('./images/pop_change_nbhood_county.jpg', dpi=300)
    # plt.close()
    
    #median income
    q_inc = ("select * from acs5yr_2015.b19001 where geoid in ('{}')")
    inc = pd.read_sql(q_inc.format("','".join(tracts)), engine_census)
    skip = ['geoid', 'name', 'stusab', 'sumlevel', 'fileid','id']
    inc_list = []
    for col in [col for col in inc.columns if col not in skip]:
        inc_list.append(inc[col].sum())
    nbhood_mdn = calculate_median(inc_list)

    #poverty
    q_pov = ("select sum(b17001002) total, "
             "sum(b17001002)/sum(b17001001)*100 pct_bel_pov "
             "from acs5yr_2015.b17001 "
             "where geoid in ('{}');")
    pov = pd.read_sql(q_pov.format("','".join(tracts)), engine_census)

    #transportation
    q_trans = ("with agg as "
		"(select sum(b08101001) total, sum(b08101009) car, "
               	        "sum(b08101017) carpool, sum(b08101025) trans,  "
                        "sum(b08101033) walk, sum(b08101041) other "
                "from acs5yr_2015.b08101 "
                "where geoid in ('{}') "
                    "and fileid = '2015e5' ) "
                "select car/total*100 pct_car, carpool/total*100 pct_carpool, "
                    "trans/total*100 pct_trans, walk/total*100 pct_walk, "
                    "other/total*100 pct_other "
                "from agg")
    trans = pd.read_sql(q_trans.format("','".join(tracts)), engine_census)

    q_move = ("with agg as "
		"(select sum(b07001001) total, sum(b07001017) same_home,"
		    "sum(b07001033) same_county, sum(b07001049) same_state,  "
                    "sum(b07001065) diff_state, sum(b07001081) diff_country "
                 "from acs5yr_2015.b07001 "
                 "where geoid in ('{}') "
                "and fileid = '2015e5') "
            "select same_home/total*100 same_home, "
            "same_county/total*100 same_county, "
            "same_state/total*100 same_state, "
            "diff_state/total*100 diff_state, "
            "diff_country/total*100 diff_country "
            "from agg")
    move = pd.read_sql(q_move.format("','".join(tracts)), engine_census)

    q_ed = ("with agg as "
		"(select sum(b15001001) total, "
            "sum(b15001004) + sum(b15001005) + "
                "sum(b15001012) + sum(b15001013) + "
                "sum(b15001020) + sum(b15001021) + "
                "sum(b15001028) + sum(b15001029) + "
                "sum(b15001036) + sum(b15001037) + "
                "sum(b15001045) + sum(b15001046) + "
                "sum(b15001053) + sum(b15001054) + "
                "sum(b15001061) + sum(b15001062) + "
                "sum(b15001069) + sum(b15001070) + "
                "sum(b15001077) + sum(b15001078) no_dip, "
             "sum(b15001006) + sum(b15001014) + "
                "sum(b15001022) + sum(b15001030) + "
                "sum(b15001038) + sum(b15001047) + "
                "sum(b15001055) + sum(b15001063) + "
                "sum(b15001071) + sum(b15001079) dip, "
             "sum(b15001008) + sum(b15001016) + "
                "sum(b15001024) + sum(b15001032) + "
                "sum(b15001040) + sum(b15001049) + "
                "sum(b15001057) + sum(b15001065) + "
                "sum(b15001073) + sum(b15001081) assoc, "
             "sum(b15001009) + sum(b15001017) + "
                "sum(b15001025) + sum(b15001033) + "
                "sum(b15001041) + sum(b15001050) + "
                "sum(b15001058) + sum(b15001066) + "
                "sum(b15001074) + sum(b15001082) bach, "
             "sum(b15001010) + sum(b15001018) + "
                "sum(b15001026) + sum(b15001034) + "
                "sum(b15001042) + sum(b15001051) + "
                "sum(b15001059) + sum(b15001067) + "
                "sum(b15001075) + sum(b15001083) grad        "
             "from acs5yr_2015.b15001 "
                 "where geoid in ('{}')"
                 "and fileid = '2015e5') "
            "select no_dip/total*100 no_dip, dip/total*100 dip,"
                "assoc/total*100 assoc, bach/total*100 bach, "
                "grad/total*100 grad "
            "from agg")

    q_lang = ("with agg as "
		"(select sum(b16001001) total, "
	            "sum(b16001002) eng, sum(b16001003) span "
         	"from acs5yr_2015.b16001 "
                    "where geoid in ('{}') "
                    "and fileid = '2015e5') "
             "select eng/total*100 eng, span/total*100 span "
             "from agg")

    age_labels = []
    for i in range(5, 90, 5):
        if i == 5:
            age_labels.append('<'+str(i))
        elif i == 85:
            age_labels.append('-'.join([str(i-5), str(i-1)]))
            age_labels.append(str(i)+'+')
        else:
            age_labels.append('-'.join([str(i-5), str(i-1)]))
    #male query
    male_index = range(3, 26)
    female_index = range(27, 50)
    male_cols = build_age_query(male_index, (6,18,20), 8)
    female_cols = build_age_query(female_index, (29, 42, 44), 31)
    q_age = "select {0} from acs5yr_2015.b01001 where geoid in ('{1}')"
    df_male = pd.read_sql(q_age.format(','.join(i for i in male_cols),
                            "','".join(i for i in tracts)), engine_census)
    df_female = pd.read_sql(q_age.format(','.join(i for i in female_cols),
                            "','".join(i for i in tracts)), engine_census)
    df = df_male.append(df_female)
    df = df.transpose()
    df.columns = ['m', 'f']
    df['pctm'] = df.m /(df.m + df.f)
    df['pctf'] = df.f /(df.m + df.f)
    fig, axes = plt.subplots(ncols=2, sharey=True)
    position=range(len(age_labels))
    axes[0].barh(position, df.m, 
            align='center', color='#79B473')
    axes[0].set(title='Male Population')
    axes[1].barh(position, df.f,
            align='center', color='#440D0F')
    axes[1].set(title='Female Population')
    axes[0].invert_xaxis()
    axes[0].set(yticks=position, yticklabels=age_labels)
    axes[0].yaxis.tick_right()
    plt.tight_layout()
    fig.subplots_adjust(wspace=0.22)
    plt.savefig('./images/pop_pyramid.jpg', dpi=300)
    plt.close()

    yticks = [0., .25, .5, .75, 1.]
    ytick_labels = ['0%', '25%', '50%', '25%', '0%']
    df[['pctm', 'pctf']].plot.bar(stacked=True, 
            width=.94, color=['#79B473','#440D0F'])
    plt.legend(['Male', 'Female'],loc='upper center',bbox_to_anchor=(.5, 1.05), ncol=2,
            fancybox=True, shadow=True)
    plt.xlabel('Age')
    plt.ylabel('Percent of Group')
    plt.yticks(yticks, ytick_labels)
    pctile_x = [17]
    plt.plot(x, [.25, .25], '--k')
    plt.plot(x, [.5, .5], 'k')
    plt.plot(x, [.75, .75], '--k')
    plt.tight_layout()
    plt.savefig('./images/mf_ratio.jpg', dpi=300)
    plt.close()
    
def build_age_query(var_index, twos, threes):
    """
    Helper function to build query for age by sex table.
    Args:
        var_index [int]: list of values that specifies the starting and
            ending location of column values
        twos (int): tuple specifying the starting position of columns that
            need to add two columns in order to attain 5-year cohort
        threes int: value specifying the starting position of columns that 
            need to add three columns in order to attain 5-year cohort
    Returns:
        List of strings containing the necessary elements to excute query.
    """
    cols = []
    st = 'sum(b01001{0})'
    for i in var_index:
        j = var_index.index(i)
        if i in twos:
            val = [i,i+1]
            col = '+'.join([st.format(str(v).zfill(3)) for v in val])
            cols.append(col+'c'+str(j))
            del var_index[j:j+1]
        elif i == threes:
            val = var_index[j:j+3] 
            col = '+'.join([st.format(str(v).zfill(3)) for v in val])
            cols.append(col+'c'+str(j)) 
            del var_index[j:j+2]
        else:
            cols.append(st.format(str(i).zfill(3))+'c'+str(j))
    return cols


def calculate_median(incomedata):
	"""
	modified from https://gist.github.com/albertsun/1245817
	"""
	bucket_tops = [10000, 15000, 20000, 25000, 30000, 35000, 
                       40000, 45000, 50000, 60000, 75000, 100000, 
                       125000, 150000, 200000]
	total = incomedata[0]
	for i in range(2,18):
		if (sum(incomedata[1:i]) > total/2.0):
			lower_bucket = i-2
			upper_bucket = i-1
			if (i == 17):
				break
			else:
				lower_sum = sum(incomedata[1:lower_bucket+1])
				upper_sum = sum(incomedata[1:upper_bucket+1])
				lower_perc = float(lower_sum)/total
				upper_perc = float(upper_sum)/total
				lower_income = bucket_tops[lower_bucket-1]
				upper_income = bucket_tops[upper_bucket-1]
				break
	if (i==17):
		return 200000

	#now use pareto interpolation to find the median within this range
	if (lower_perc == 0.0):
		sample_median = lower_income + ((upper_income - lower_income)/2.0)
	else:
		theta_hat = ((log(1.0 - lower_perc) - log(1.0 - upper_perc)) 
                                    / 
                (log(upper_income) - log(lower_income)))
		
                k_hat = (pow((upper_perc - lower_perc) 
                    / 
                ( (1/pow(lower_income, theta_hat)) - 
                    (1/pow(upper_income, theta_hat)) ), 
                (1/theta_hat) ))

		sample_median = k_hat * pow(2, (1/theta_hat))
	return sample_median

def ownership():
    """
    """

    #selects distinct owner names for ownership in neighborhood
    q_make_distinct = ("drop table if exists own_count;"
                        "create temporary table own_count as "
                            "select own_adr, count(own_adr) from "
                            "(select parcelid, parid, "
                                "concat(adrno, ' ', adrstr) par_adr "
            	            "from sca_parcels p, sca_pardat, "
                            "geography.boundaries b "
                            "where parid = parcelid "
                            "and st_intersects(st_centroid(p.wkb_geometry), "
                                                "b.wkb_geometry) "
                            "and name = '{}') par "
                            "join (select parid, own1, "
                                "concat(adrno, ' ', adrstr, ' ', cityname) own_adr, "
                                    "cityname, statecode, zip1 "
                                "from sca_owndat) own "
                            "on own.parid = parcelid "
                            "group by own_adr order by own_adr")
    engine_blight.execute(q_make_distinct.format(nbhood))

    #------------------------------------------------------------------------
    #------------------Waffle Chart for Owner Occupancy----------------------
    #------------------------------------------------------------------------
   
    #gets break down of owner occupancy totals
    q_ownocc = ("with own as "
                "(select lower(concat(adrno,adrstr)) ownadr, parid "
                        "from sca_owndat) "
                "select ownocc, nonownocc from "
                "(select count(parcelid) ownocc from nbhood_props, own "
                "where parcelid = parid and paradrstr = ownadr) oc "
                "join "
                "(select count(parcelid) nonownocc from nbhood_props, own "
                "where parcelid = parid and paradrstr <> ownadr) noc "
                "on 1 = 1")
    own_totals = engine_blight.execute(q_ownocc).fetchone()
    vals = {'Non-Owner Occupied':
            int(round(own_totals[1]/float(sum(own_totals)),2)*100),
            'Owner Occupied':
            int(round(own_totals[0]/float(sum(own_totals)),2)*100)}
    fig = plt.figure(FigureClass=Waffle,
            rows=5,
            values=vals,
            colors=('#ffffff', '#79B473'),
#            grid={'color':'black', 'linestyle':'solid', 'linewidth':2},
            #colors=('#003BD1', '#79B473'),
            title={'label': 'Ownership Occupancy Totals', 'loc':'center'},
            labels=["{0} ({1}%)".format(k, v) for k, v in vals.items()],
            legend={'loc': 'center', 'bbox_to_anchor': (0, -0.4), 
                'ncol': len(own_totals), 'framealpha': 0})
#    plt.grid(color='k', linestyle='solid', linewidth=2)
    ax = plt.gca()
    ax.set_facecolor('black')
    plt.tight_layout()
    plt.savefig('./images/ownocc_waffle.jpg', dpi=300)
    plt.close()



    #select counts for unique owners in neighborhood
    q_own = ("select distinct on(count, own_adr) initcap(own) as own, "
            "initcap(concat(own_adr, ' ', statecode, ' ', zip1)) as adr, "
            "count as props "
            "from "
            "(select own_adr, count from own_count "
            " order by count desc limit 5) k "
            "inner join (select "
                  "case when lower(own1) like '%shelby county tax sale%' "
                        "then 'Shelby County Tax Sale' "
                  "when lower(concat(adrno, adrstr)) like '%125main%' "
                        "then 'City of Memphis' "
                      "when lower(concat(adrno, adrstr, cityname, statecode)) "
                        "like '%po box 2751memphistn%' "
                        "then 'Shelby County Tax Sale' "	  
                      "when lower(concat(adrno, adrstr, cityname)) "
                        "like '%160mainmemphis%' "
                        "then 'Shelby County' "
                      "when lower(concat(adrno, adrstr, cityname)) "
                        "like '%170mainmemphis%' "
                        "then 'State of Tennessee' "
                      "else own1 "
                  "end as own, "
                  "concat(adrno, ' ', adrstr, ' ', cityname) owna, "
                  "cityname, statecode, zip1 from sca_owndat) o "
            "on own_adr = owna "
            "order by count desc, own_adr ")
    df_own = pd.read_sql(text(q_own), engine_blight)
    fig, ax = plt.subplots()
    ax.barh(df_own.index.tolist(), df_own.props.tolist(), color="#253C78")
    ax.set_yticks(df_own.index.tolist())
    ax.set_yticklabels(df_own.own.tolist())
    ax.set_xlabel('Number of Properties')
    plt.tight_layout()
    plt.savefig('./images/top_owners.jpg', dpi=300)
    plt.close()


def make_property_table(schema="public", table="sca_parcels"):
    #creates temporary table of parcels that are within neighborhood
    
    if table == "sca_parcels":
        pardat = "sca_pardat"
    else:
        pardat = "sca_pardat_2001"
    params = {"schema":schema,
              "table":table,
              "nbhood":nbhood,
              "pardat":pardat}

    q_nbhood_props =("create temporary table nbhood_props as "
                    "select parcelid, lower(concat(adrno, adrstr)) paradrstr,"
                    "initcap(concat(adrno, ' ', adrstr)) "
                    "from {schema}.{table} p, {pardat}, " 
                        "geography.boundaries b "
                    "where st_intersects(st_centroid(p.wkb_geometry), "
                            "b.wkb_geometry) "
                    "and name = '{nbhood}' "
                    "and parcelid = parid")
    engine_blight.execute(q_nbhood_props.format(**params))

def property_table_exists():
    try:
        q_table = "select * from nbhood_props"
        engine_blight.execute(q_table)
        return True
    except:
        return False

def property_conditions():
    """
    """
    if not property_table_exists():
        make_property_table()
    df_props = pd.read_sql("select * from nbhood_props", engine_blight)
    #selects all of the code enforcement violations over time
    q_code = ("select * from "
	"nbhood_props,"
            "(select incident_id, category,request_type, reported_date, "
            "summary, group_name, parcel_id "
	"from com_incident) incident "
	"where parcelid = parcel_id")
    df_code = pd.read_sql(q_code, engine_blight)
    
    df_code.request_type = df_code.request_type.str.split('-').str[1]
    df_code.reported_date = pd.to_datetime(df_code.reported_date)

    #------------------------------------------------------------------------
    #------------------line plot for Code Request by type--------------------
    #------------------------------------------------------------------------
    lbls = sorted(df_code.reported_date.dt.year.unique())
    req_count = df_code.groupby('request_type').parcelid.count()
    keep = req_count >= req_count.median()
    reqs = req_count[keep].index
    num_colors = len(reqs)
    cm = plt.get_cmap('gist_rainbow')
    fig, ax = plt.subplots()
    #generate random colors to avoid duplicating colors 
    ax.set_color_cycle([cm(1.*i/num_colors) for i in range(num_colors)])
    for req in reqs:
        y = (df_code[df_code.request_type == req]
                            .request_type
                            .groupby(df_code.reported_date.dt.year).count())
        x = sorted(df_code[df_code.request_type == req].reported_date.dt.year.unique())
        x_smoothe = np.linspace(x[0], x[-1], 200)
        y_smoothe = spline(x, y, x_smoothe)
        plt.plot(y_smoothe, label=req)
    plt.legend(loc='upper center',bbox_to_anchor=(.5, 1.05), ncol=3,
            fancybox=True, shadow=True, fontsize='xx-small')
    ax.set_xticklabels(lbls)
    ax.set_ylabel("Number of Requests")
    ax.set_xlabel("Year")
    plt.tight_layout()
    plt.savefig('./images/code_viols_all.jpg', dpi=300)
    plt.close()

    #------------------------------------------------------------------------
    #-----------------Heat Map for All Requests over time--------------------
    #------------------------------------------------------------------------
    df_code['month'] = df_code.reported_date.dt.month
    df_code['year'] = df_code.reported_date.dt.year
    df_code_grp = (df_code.groupby(['month', 'year'])
                          .parcelid
                          .count()
                          .reset_index())
    df_code_grp_pivot = df_code_grp.pivot('month', 'year', 'parcelid')
    sns.heatmap(df_code_grp_pivot,cmap='Greens', 
                center=df_code_grp.parcelid.median())
    plt.xlabel("Year")
    plt.xticks(rotation=45)
    plt.ylabel("Month")
    plt.title("Total Requests {0} to {1}".format(df_code_grp.year.min(),
                                                 df_code_grp.year.max()))
    plt.tight_layout()
    plt.savefig('./images/code_req_heatmap.jpg', dpi=300, bbox_inches="tight")
    plt.close()

    #------------------------------------------------------------------------
    #-----------------Bar Chart for most common Requests---------------------
    #------------------------------------------------------------------------
    
    req_type_grp = (df_code[df_code.request_type.isin(reqs)]
                           .groupby('request_type')
                           .parcelid
                           .count()
                           .reset_index()
                           .sort_values('parcelid'))
    pos = range(len(req_type_grp))
    plt.barh(pos, 
            req_type_grp.parcelid, 
            tick_label=req_type_grp.request_type.tolist(),
            color='#440D0F')
    plt.xticks(rotation=90)
    plt.xlabel('Number of Code Requests')
    plt.title('Common Code Requests by Type')
    plt.tight_layout()
    plt.savefig('./images/req_by_type.jpg', dpi=300)
    plt.close()
    
    total_parcels = engine_blight.execute(("select count(parcelid) "
                                            "from nbhood_props")).fetchone()[0]

    q_tables = [
        {"table": "mlgw_disconnects",
          "parcelid": "parid", 
          "options": ("and load_date = (select max(load_date) "
          "from mlgw_disconnects) and mtrtyp = 'R'")
        },
        {"table": "com_incident",
         "parcelid": "parcel_id",
         "options": (" and reported_date >= '{}-01-01'".format(str(cur_year)))
        },
        {"table": "sca_pardat",
         "parcelid": "parid",
         "options": (" and luc = '000'")
        }
    ]

    q_vals = ("select count(t.{parcelid}) from {table} t, nbhood_props p "
              "where t.{parcelid} = p.parcelid {options}")

    results = dict()
    for t in q_tables:
        result = engine_blight.execute(q_vals.format(**t)).fetchone()[0]
        results[t["table"]] = result/float(total_parcels)

def financial():
    """
    TODO:
        - Median residential sale value for past year
        - Percent change in appraisal since 2000
        - Percent of properties with delinquent taxes
        - Total number of mortgage originations for past year
        - Median rent
        - Percent of parcels that are tax sale eligible
    """
    if not property_table_exists():
        make_property_table()
    df_props = pd.read_sql("select * from nbhood_props", engine_blight)
    q_appr = ("select parcelid, apr17, apr01 "
              "from nbhood_props, "
              "(select a17.parid, a01.rtotapr apr01, a17.rtotapr apr17 "
                "from sca_asmt a17, geography.sca_asmt_2001 a01 "
                "where a17.parid = a01.parid) a "
              "where parcelid = parid")
    df_appr = pd.read_sql(q_appr, engine_blight)
    df_appr["apr01_adj"] = utils.inflate("2001", "2017", df_appr.apr01)
    pct_chg = lambda y1, y2: (y2-y1)/y1*100
    df_appr["pct_chg"] = pct_chg(df_appr.apr01_adj, df_appr.apr17)

    
def main(args):
    if args["neighborhood"]:
        pass
    elif args["violator"]:
        #print args
        run_violator_report(args["<month>"], 
                            args["--year"]
                           )

if __name__=="__main__":
    args = docopt(__doc__)
    main(args)

