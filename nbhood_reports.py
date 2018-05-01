"""Module to pull monthly reports for code violators. Takes the desired month 
for the report as input and produces charts and maps with information about
the top code violators for that month. It also updates a running list of the 
top violators for the current year.

Example:
    $ python reports.py may
TODO:
    *methods:
        run_report
    *set up project
        - create directory using neighborhood name
        -switch to project directory

"""
import sys
import os
sys.path.append('/home/nate/source')
from caeser import utils
from config import cnx_params
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import calendar
import seaborn as sns
from sqlalchemy import text
from titlecase import titlecase
from math import log

params = cnx_params.blight
engine_blight = utils.connect(**params)
engine_census = utils.connect(**cnx_params.census)
pd.set_option('display.width', 180)
os.chdir(('/home/nate/dropbox-caeser/Data/MIDT/Data_Warehouse/'
          'reports/neighborhood/generated_reports/KlondikeSmokeyCityCDC/')
nbhood = 'Klondike Smokey City CDC'

def run_report(period):
    """Primary function that handles the query and generates the main output
    for the report.
    
    Args:
        month (str): Name of the month for the report. Can either be the full
            name or a 3-letter abbreviation.
    Returns:
        None
    TODO:
        Adjust selection query to adjust start day and end day based upon
        input months.
    """
    os.chdir(("/home/nate/dropbox-caeser/Data"
              "/MIDT/Data_Warehouse/reports/"
              "monthly_code_violators"))
    if len(period) == 1:
        start = period[0]
        finish = period[0]
    elif len(period) == 2:
        start, finish = period
    else:
        print ("Incorrect input format. Input can either be a single month, "
                "or a start month and finish month for a range.")
        return 
    
    # q = ("select case "
            # "when lower(own1) like 'shelby county tax sale%' "
                # "then 'Shelby County Tax Sale' "
            # "when lower(concat(adrno,adrdir,adrstr,adrsuf)) = '125nmainst' "
                # "then 'City of Memphis' "
                  # "else trim(both ' ' from own1) end as own, "
            # "par_adr, p.parcelid, "
	    # "reported_date, request_type, summary, lon, lat " 
        # "from " 
	    # "(select parcelid, "
                # "concat(adrno, ' ', adrstr, ' ', adrsuf, ', ', zip1) par_adr, "
                    # "summary, reported_date, "
                # "split_part(request_type, '-', 2) request_type, "
                     # "st_x(st_centroid(wkb_geometry)) lon, "
                # "st_y(st_centroid(wkb_geometry)) lat "
                # "from sca_pardat, sca_parcels, com_incident "
            # "where parcelid = parid "
                # "and parcelid = parcel_id "
                # "and reported_date "
                    # "between '2018-01-01'::timestamp "
                    # "and '2018-01-31'::timestamp) p,"
	# "sca_owndat "
        # "where parcelid = parid "
        # "order by own;")
    
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
    		"between '2018-{start}-01'::timestamp "
                    "and '2018-{finish}-31'::timestamp) p,"
	"sca_owndat "
        "where parcelid = parid "
        "and lower(own1) not similar to '%({ignore})%' "
        "order by own;")

    #there's a known bug in pandas where using '%' in query causes read_sql
    #to fail unless wrapped in sqlalchemy.text

    def acronyms(word, **kwargs):
        if word in ["CSMA", "LLC", "(RS)", "RS", "FBO", "II"]:
            return word.upper()
    
#    ignore = ("city of memphis|shelby county tax sale|"
#                "memphis educational and housing facility")
    vals = {"start": start, "finish": finish,
            "ignore": "|".join(ignore)}
    df = pd.read_sql(text(q.format(**vals)), engine_blight)
    #remove references to city and county owned properties
#    df = df[~df.own.str.lower().str.match(ignore)]
    df.own = df.own.apply(titlecase, args=[acronyms]) 
    # own_group = (df.groupby('own')['own']
                    # .count()
                    # .sort_values(ascending=False)
                    # .head(10))
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
    fig_name = 'owner_bar_stacked_{}_2018.jpg'.format(month)
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

def get_dates(month):
    """
    Formats date strings to be used in SQL query to pull table from DB.
    
    Args:
        month (str):
    Returns:
        Tuple containing formatted date strings in the form 'YYYY-MM-dd' to be
            passed into SQL query to specify date range for the report.
    """
    if len(month) > 3:
        month = month[:3]
    else:
        month = month.lower()
    month_abbr = {v.lower(): k for k, v in enumerate(calendar.month_abbr)}
    last_day = calendar.monthrange()[1]

def get_tract_values(nbhood):
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
                 "where geoid in ('{}')
                 and fileid = '2015e5') "
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

    """
    Population Pyramid
        Male -> 3 to 25
            groups:
                6-7
                8-10
                18-19
                20-21
        Female -> 27 to 49
            groups:
                30-31
                32-34
                42-43
                44-45    
    """
    age_labels = []
    for i in range(5, 90, 5):
        if i == 5:
            age_labels.append('<'+str(i))
        elif i == 85:
            age_labels.append(str(i)+'+')
        else:
            age_labels.append('-'.join([str(i-5), str(i-1)]))
    #male query
    male_cols = []
    for i in range(3,21,1):
        if 

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
		theta_hat = (log(1.0 - lower_perc) - log(1.0 - upper_perc)) 
                                    / 
                (log(upper_income) - log(lower_income))
		
                k_hat = (pow((upper_perc - lower_perc) 
                    / 
                ( (1/pow(lower_income, theta_hat)) - 
                    (1/pow(upper_income, theta_hat)) ), 
                (1/theta_hat) ))

		sample_median = k_hat * pow(2, (1/theta_hat))
	return sample_median
