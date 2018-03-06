"""Module to pull monthly reports for code violators. Takes the desired month 
for the report as input and produces charts and maps with information about
the top code violators for that month. It also updates a running list of the 
top violators for the current year.

Example:
    $ python reports.py may
TODO:
    *methods:
        run_report

"""
import sys
sys.path.append('/home/nate/source')
from caeser import utils
from config import cnx_params
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import calendar
import seaborn as sns
from sqlalchemy import text

params = cnx_params.blight
engine = utils.connect(**params)
pd.set_option('display.width', 180)

def run_report(month):
    """Primary function that handles the query and generates the main output
    for the report.
    
    Args:
        month (str): Name of the month for the report. Can either be the full
            name or a 3-letter abbreviation.
    Returns:
        None
    """
    os.chdir(("/home/nate/dropbox-caeser/Data"
              "/MIDT/Data_Warehouse/reports/"
              "monthly_code_violators"))
    q = ("select case "
            "when lower(own1) like 'shelby county tax sale%' "
                "then 'Shelby County Tax Sale' "
            "when lower(concat(adrno,adrdir,adrstr,adrsuf)) = '125nmainst' "
                "then 'City of Memphis' "
      	    "else trim(both ' ' from own1) end as own, "
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
    		"between '2018-01-01'::timestamp "
                    "and '2018-01-31'::timestamp) p,"
	"sca_owndat "
        "where parcelid = parid "
        "order by own;")

    #there's a known bug in pandas where using '%' in query causes read_sql
    #to fail unless wrapped in sqlalchemy.text
    df = pd.read_sql(text(q), engine)
    own_group = (df.groupby('own')['own']
                    .count()
                    .sort_values(ascending=False)
                    .head(10))
    make_map(df[df.own.isin(own_group.index.tolist())])
    own_violations = (df[df.own.isin(own_group.index.tolist())]
                        .groupby(['own', 'request_type'])['request_type']
                        .count()
                        .unstack(level=-1, fill_value=0))

    #get index values in sort order and use to plot from most to least
    sort_order = own_violations.sum(axis=1).sort_values(ascending=False).index
    ax = own_violations.ix[sort_order].plot(kind='bar', 
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
    plt.savefig('owner_bar_stacked_jan_2018.jpg', dpi=300)

def make_map(dframe):
    """
    dframe=own_props.copy()
    """
    addr_count = dframe.groupby('par_adr', as_index=False).size()
    dframe.drop_duplicates('par_adr', inplace=True)
    dframe = dframe.join(addr_count.to_frame(), on='par_adr')
    dframe.rename(columns={0:'num_vi'}, inplace=True)
#    dframe = dframe.reset_index()
    owners = pd.DataFrame(dframe.own.unique(), columns=['own'])
    owners['label'] = owners.index + 1
    dframe = dframe.merge(owners, on='own')
    dframe.index += 1
    dframe.to_csv('owner_coords.csv', index_label='id')

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
    last_day = calendar.monthrange(
    

