"""
Generate reports for code violators or neighborhood pre-defined neighborhood.
Takes the desired month for the report as input and produces charts and maps 
with information about the top code violators for that month. It also updates 
a running list of the top violators for the current year.

Usage:
    nbhood_reports.py violator <month>... [--year=<year>]  
    nbhood_reports.py neighborhood <neighborhood_name>

Options:
    -h, --help          : show help document
    violator            : Generate report for top code violators by ownership
    <month>             : Month or months to be used as the range for the 
                          analysis. Months can either be integer or text values 
                          (i.e. 1 or Jan)
    --year=<year>       : Optional parameter to change the year that the report
                          should be geneated for
    neighborhood        : Generate neighborhood
    <neighborhood_name> : Name of neighborhood that the report should be generated for.
                          Neighborhoods with more than one word should be enclosed in 
                          quotation marks.
        

Example:
    $ python nbhood_reports.py violator may
    $ python nbhood_reports.py neighborhood "Klondike Smokey City CDC"
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
from collections import defaultdict
from config import cnx_params
import datetime
from docopt import docopt
from lxml import etree
from math import log
import matplotlib.pyplot as plt
import pandas as pd
from pywaffle import Waffle
import numpy as np
import os
from qgis.core import (QgsProject, QgsComposition, QgsApplication, 
                       QgsProviderRegistry, QgsRectangle, QgsPalLayerSettings,
                       QgsComposerAttributeTableV2, QgsComposerMap, QgsComposerLegend,
                       QgsComposerPicture, QgsComposerLabel, QgsComposerFrame,
                       QgsMapLayerRegistry)
from qgis.gui import QgsMapCanvas, QgsLayerTreeMapCanvasBridge
from PyQt4.QtCore import QFileInfo, QSize
from PyQt4.QtXml import QDomDocument
from PyQt4.QtGui import QImage, QPainter
from scipy.interpolate import pchip
import seaborn as sns
import shutil
from sqlalchemy import text
import string
import sys
sys.path.append('/home/nate/source')
from titlecase import titlecase
import zipfile
import warnings

warnings.filterwarnings("ignore")


engine_blight = utils.connect(**cnx_params.blight)
engine_census = utils.connect(**cnx_params.census)
pd.set_option('display.width', 180)
os.chdir("/home/nate/dropbox-caeser/Data/MIDT/Data_Warehouse/reports")
#nbhood = 'Klondike Smokey City CDC'
cur_year = datetime.datetime.today().year
ACS_SCHEMA = "acs5yr_2016"
FILEID = ACS_SCHEMA.split("_")[-1]+"e5"
#list of layers to be active in all maps for neighborhood report
BASEMAP_LAYERS = ["boundaries", "boundary_mask", "street_labels", 
                  "streets_carto", "sca_parcels"
                 ] 
COMPOSER_ITEMS = ["legend", "scale_bar", "main_map"]

class Report:
    """
    Class used to handle the report creation and modification
    """

    def __init__(self, nbhood_name, z_in, z_out, *args, **kwargs):
        """
        Parameters:
            nbhood_name (str): Name of the neighborhood that report is being generated
                for.
            z_in (str): Name of the template being used.
            z_out (str): Name of the odt document that will be created upon report
                completion
        """
        self.nbhood = nbhood_name
        self.z_in = zipfile.ZipFile(z_in)
        self.zip_out = zipfile.ZipFile(z_out, "w")
        self.xml_content = self.zip_in.read("content.xml")
        self.xml_manifest = self.zip_in.read("META-INF/manifest.xml")
        self.root_content = etree.fromstring(self.xml_content)
        seslf.root_mainifest = etree.fromstring(self.xml_manifest)
        
        #All required namespace prefixes needed to locate xml tags in odt
	self.ns = {"draw": "urn:oasis:names:tc:opendocument:xmlns:drawing:1.0",
		   "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
		   "style": "urn:oasis:names:tc:opendocument:xmlns:style:1.0",
		   "table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
		   "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
		   "manifest": "urn:oasis:names:tc:opendocument:xmlns:manifest:1.0",
		   "xlink": "http://www.w3.org/1999/xlink",
		   "re:": "http://exslt.org/regular-expressions"
		  }
        
    def create_titles(self):
        """
        """
        pass
    
    def save_report(self):
        """
        Finalize report and save to disk
        """
        pass

    def insert_image(self, image_name):
        """
        use image name to update 
        """
        pass

    def update_table(self, table_name, *args):
        """
        update values in table
        """
        pass

    def update_text(self):
        """
        Locate 
        """
        pass

        


class QgisMap:
    """
    Class used to automatically export images from pre-formatted QGIS maps. Individual
    map elements can be added to each project to allow for a little more flexibility
    in the maps that are automated.
    """
    
    def __init__(self, project_name, template_name, *args, **kwargs):
        """
        
        Parameters:
            project_name (str): Name of qgs file containing maps to be generated
            template_name (str): Each QGIS map composer must be saved as a template 
                (qpt) file in order to be loaded for automated map generation.
        """
        gui_flag = True
        self.app = QgsApplication(sys.argv, True)
        self.app.setPrefixPath("/usr", True)
        self.app.initQgis()

        self.project_name = project_name
        self.template_name = template_name
        self.canvas = QgsMapCanvas()

        self.scale = self.canvas.scale()
        self.project = QgsProject.instance()
        self.project.read(QFileInfo(project_name))
        self.root = QgsProject.instance().layerTreeRoot()
        self.bridge = QgsLayerTreeMapCanvasBridge(
                QgsProject.instance().layerTreeRoot(), self.canvas)
        self.bridge.setCanvasLayers()
        self.registry = QgsMapLayerRegistry.instance()
        self.template_file = file(self.template_name)
        self.template_content = self.template_file.read()
        self.template_file.close()
        self.document = QDomDocument()
        self.document.setContent(self.template_content)
        self.map_settings = self.canvas.mapSettings()
        self.composition = QgsComposition(self.map_settings)
        self.composition.loadFromTemplate(self.document)
        self.rectangle = self.canvas.extent()
        self.map_layers = [lyr for lyr in self.registry.mapLayers() if self.root.findLayer(lyr)]
        self.element_methods = {QgsComposerAttributeTableV2:
                                    {"refreshAttributes": args},
                                QgsComposerMap: 
                                    {"setMapCanvas": args, 
                                     "zoomToExtent": args,
                                     "setNewScale": args},
                                QgsComposerLegend: 
                                    {"updateLegend": args},
                                QgsComposerPicture: 
                                    {"setPicturePath": kwargs, 
                                     "refreshPicture": args},
                                QgsComposerLabel: 
                                    {"setText": args}
                                }
        if "basemap" in kwargs:
            self.basemap = kwargs["basemap"]
        if "layers" in kwargs:
            self.layers = kwargs["layers"]


    def get_element_methods(self):
        return self.element_methods

    def add_element(self, item_id, *args, **kwargs):
        """
        Add elements to the map composer to be displayed on the exported image.

        Args:
            item_id (str): Item id for the composer element as listed in the print
                composer in QGIS.

        Keyword Args:
            image_path (str): Full path including extension to image to be used with the
                setPicturePath method

        Returns:
            None
        """
        composer_item = self.composition.getComposerItemById(item_id)
        if type(composer_item) == QgsComposerFrame:
            composer_item = composer_item.multiFrame()
        for method, params in self.element_methods[type(composer_item)].iteritems():
            if kwargs:
                if method in kwargs.keys():
                    getattr(composer_item, method)(kwargs[method])
                else:
                    getattr(composer_item, method)()
            if args:
                getattr(composer_item, method)(*args)
        self.composition.refreshItems()
        self.canvas.refresh()

    def set_layers(self, layers):
        """

        """
        self.layers = layers

    def update_scale(self, scale):
        self.scale = scale

    def update_extent(self, rectangle):
        """
        Set new extent for canvas.

        Args:
            rectangle (tuple, float): coordinates for new extent rectangle in format 
                (xmin, ymin, xmax, ymax)

        Returns:
            None
                
        """
        self.rectangle = QgsRectangle(*rectangle)
    
    def zoom_to_layer(self, layer_name):
        """
        """
        lyr = self.registry.mapLayersByName(layer_name)[0]
        lyr_ids = [feat.id() for feat in lyr.getFeatures()]
        self.canvas.zoomToFeatureIds(lyr, lyr_ids)
        #multiplying scale by 1.2 seems to give sufficient space around the boundary
        new_scale = canvas.scale() * 1.2
        self.canvas.zoomScale(new_scale)
        self.update_scale(new_scale)

    def has_layers(self):
        """

        """

        try:
            self.basemap + self.layers
            return True
        except:
            return False

    def set_visible_layers(self, map_element, keep_set=False):
        """
        Set which layers are visible in canvas before saving

        Args:
            map_element (str): name of the composer map element
            keep_set (bool): determine whether to lock current layers when working with 
                multiple map items in a single composer such as an inset map.
        Returns:
            None
        """
        if not self.has_layers():
            msg = ("Layers cannot be set because none have been provided. Run `set_layers` "
                    "method and try again."
                    )

            print msg
            return
        else:
            visible = []
            for m_lyr in self.map_layers:
                lyr = self.root.findLayer(m_lyr)
                if lyr.layerName() in self.basemap + self.layers:
                    visible.append(m_lyr)
                    lyr.setVisible(2) #Qt.CheckState checked
                else:
                    lyr.setVisible(0) #Qt.CheckState unchecked
            self.map_settings.setLayers(visible)
            comp_map = self.composition.getComposerItemById(map_element)
            comp_map.setMapCanvas(self.canvas)
            comp_map.setNewExtent(self.canvas.extent())
            comp_map.setLayerSet(visible)
            comp_map.setKeepLayerSet(keep_set)


    def add_label(self, layer_index, field_name):
        """
        Enable label for layer in map canvas.

        Args:
            layer_index (int): index position of layer in project layer tree (TOC)
            field_name (str): name of field used for label

        Returns:
            None
        """
        lyr = self.canvas.layer(layer_index)
        lbl = QgsPalLayerSettings()
        lbl.readFromLayer(lyr)
        lbl.enabled = True
        lbl.fieldName = field_name
        self.canvas.refresh()


    def save_map(self, map_name, extension="jpg", dpi=300):
        """
        Export map as image.

        Args:
            map_name (str): name of saved image
            extension (str, optional): Type of image to be saved (e.g. jpg, png, etc.)
            dpi (int, optional): Resolution of saved image.

        Returns:
            None
        """

        dpmm = dpi/25.4
        width = int(dpmm * self.composition.paperWidth())
        height = int(dpmm * self.composition.paperHeight())
        image = QImage(QSize(width, height), QImage.Format_ARGB32)
        image.setDotsPerMeterX(dpmm * 1000)
        image.setDotsPerMeterY(dpmm * 1000)
        image.fill(0)

        imagePainter = QPainter(image)
        self.composition.renderPage(imagePainter, 0)
        imagePainter.end()
        image.save(".".join([map_name,extension]), extension)
        self.project.clear()
        self.app.exitQgis()
    
    def close(self):
        self.project.clear()
        self.app.exitQgis()

def run_violator_report(period, yr=None):
    """
    Primary function that handles the query and generates the main output
    for the report.
    
    Args:
        period (int): Numeric value of the month for the report (i.e. 5 = May,
                      6 = June, etc.). Can either be a single month to run a one-month
                      report, or 2 values specifying the start and end months for a range.
        yr (int, optional): Year for the report
    Returns:
        None
    """
    os.chdir("./monthly_code_violators")
    year = int(yr) if yr else cur_year
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
    date_label = "{0} {1} to {2} {3}, {4}".format(
                                            calendar.month_name[int(start)],
                                            "1",
                                            calendar.month_name[int(finish)],
                                            end_date.split("-")[2],
                                            end_date.split("-")[0]
                                            )

    #List of owner names in sca_owndat that should be excluded from the analysis. Names
    #are typically added based on the recommendation of various members from the Blight
    #Elimination Steering Team (BEST)
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
        if word in ["CSMA", "LLC", "(RS)", "RS", "FBO", "II", "LP"]:
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
    owner_violator_map(df[df.own.isin(own_group.own.tolist())],
            own_group, date_label)
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
    labels = ax.get_xticklabels()
    
    plt.setp(labels, rotation=90, fontsize=8)
    plt.legend(title="Violation Type", fontsize=8)
    plt.tight_layout()
    fig_name = 'owner_bar_stacked_{}.jpg'.format(end_date)
    plt.savefig(fig_name, dpi=300)

def owner_violator_map(dframe, group, date_label):
    """
    Formats and exports a map for property code violation owner report using an existing
    template in the monthly_code_violators report director in dropbox-caeser

    Args:
        dframe (Pandas DataFrame): full dataframe generated from a sql selection of all
            properties with property code violations for specified time range
        group (Pandas DataFrame): DataFrame containing unique names and number of violations
            for top 10 code violations.
        date_label (str): Formatted string containing date range for report. String is to
            be used as the map label for generating report

    Returns:
        None
    """
    addr_count = dframe.groupby('par_adr', as_index=False).size()
    dframe.drop_duplicates('par_adr', inplace=True)
    dframe = dframe.join(addr_count.to_frame(), on='par_adr')
    dframe.rename(columns={0:'num_vi'}, inplace=True)
    dframe = dframe.merge(group, on='own')
    dframe.index += 1
    dframe.to_sql("owner_coords", engine_blight, 
                    schema="reports", if_exists="replace", index_label="id")
    
    nhood_map = QgisMap("map.qgs", "map_template.qpt")
    logo_path = ("/home/nate/dropbox-caeser/CAESER"
                 "/logos_letterhead_brand_standards/logos/UofM_horiz_cmyk_CAESER.png")
    nhood_map.add_element("logo", setPicturePath=logo_path)
    nhood_map.update_extent((677676.557, 249522.171, 864816.297, 381843.199))
    nhood_map.update_scale(192055)
    nhood_map.add_element("map")
    nhood_map.add_element("legend")
    nhood_map.add_element("table")
    nhood_map.add_label(0, "rank")

    nhood_map.add_element("date_range", date_label)

    map_title = "owner_violator_" + date_label.replace(" ", "_").replace(",", "")
    nhood_map.save_map(map_title)
    

def format_date(month, year):
    """
    Formats date strings to be used in SQL query to pull table from DB.
    
    Args:
        month (str):
        year (int): 
    Returns:
        String containing formatted date strings in the form 'YYYY-MM-dd' to be
            passed into SQL query to specify date range for the report.
    """
    
    month_abbr = {v.lower(): k for k, v in enumerate(calendar.month_abbr)}
    if not month.isdigit():
        month = month_abbr[month[:3].lower()]
    last_day = calendar.monthrange(year, int(month))[1]
    return "{0}-{1}-{2}".format(year, month, last_day)

def get_tract_ids(nbhood):
    """
    Helper function that gets all Census tract geoids for tracts that intersect 
    neighborhood boundary.

    Parameters:
        nbhood (str): Neighborhood name. The neighborhood should already exist in the
            Property Hub table `geography.boundaries`
    
    Returns:
        List: List of string values containing all geoids for Census tracts that 
            intersect neighborhood boundary.
    """
    q = ("select geoid10 from geography.tiger_tract_2010 t, "
            "geography.bldg_cdc_boundaries b "
         "where st_intersects(t.wkb_geometry, b.wkb_geometry) "
            "and b.name = '{}'")
    tracts = engine_blight.execute(q.format(nbhood)).fetchall()
    return [i[0] for i in tracts]

def neighborhood_profile(nbhood=None):

    tables = {'b02001':'Race', 
              'b19013': 'Median Household Income',
              's1701': 'Poverty Status',
              'b08101': 'Means of Transportation to Work',
              'b07001': 'Residence 1 Year Ago',
              's15001': 'Educational Attainment',
              's16001': 'Language Spoken at Home'}
    years = [1970, 1980, 1990, 2000, 2010]
    cols = ", ".join(["pop%s" % str(i)[2:] for i in years])
    tracts = get_tract_ids(nbhood)
    table_params = {"schema": ACS_SCHEMA,
                   "fileid": FILEID,
                   "tracts": "','".join(tracts)
                   }

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

   #------------------------------------------------------------------------------------ 
   #--------------------------- Pop change by Tract -------------------------- 
   #------------------------------------------------------------------------------------ 
    ax = plt.subplot(111)
    for tract in pop.tractid.unique():
        y = pop[pop.tractid == tract][years].sum()
        x_s = np.linspace(years[0], years[-1], 200)
        pch = pchip(years, y)
        plt.plot(pch(x_s), label=tract)
        #plt.plot(pop[pop.tractid == tract][years].sum(), label=tract)

    legend_cols = len(pop.tractid.unique())/2
    ax.legend(loc='upper center',bbox_to_anchor=(.5, 1.05), ncol=legend_cols,
            fancybox=True, shadow=True, fontsize=6)
    ax.set_xlabel('Year')
    ax.set_ylabel('Population')
    ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, 
                        loc: "{:,}".format(int(x))))
    yr_labels = range(years[0] -5, years[-1] + 5, 5)
    xi = [i for i in range(0, len(yr_labels))]
    ax.set_xticklabels(yr_labels)
    plt.title("Population Change by Census Tract", {"y":1.03})
    plt.tight_layout()

    plt.savefig('./Pictures/pop_change_tracts.jpg', dpi=300)
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
    # plt.savefig('./Pictures/pop_change_nbhood_county.jpg', dpi=300)
    # plt.close()

   #------------------------------------------------------------------------------------ 
   #------------------- Data for Socioecomic table  -------------------------- 
   #------------------------------------------------------------------------------------ 


    #median income
    q_inc = ("select * from {schema}.b19001 "
             "where geoid in ('{tracts}') and fileid = '{fileid}'")
    inc = pd.read_sql(q_inc.format(**table_params), engine_census)
    skip = ['geoid', 'name', 'stusab', 'sumlevel', 'fileid','id']
    inc_list = []
    for col in [col for col in inc.columns if col not in skip]:
        inc_list.append(inc[col].sum())
    nbhood_mdn = int(round(calculate_median(inc_list)))

    #poverty
    q_pov = ("select sum(b17001002) total, "
             "sum(b17001002)/sum(b17001001)*100 pct_bel_pov "
             "from {schema}.b17001 "
             "where geoid in ('{tracts}') and fileid = '{fileid}';")
    pov = engine_census.execute(q_pov.format(**table_params)).fetchone()

    #transportation
    q_trans = ("with agg as "
		"(select sum(b08101001) total, sum(b08101009) car, "
               	        "sum(b08101017) carpool, sum(b08101025) trans,  "
                        "sum(b08101033) walk, sum(b08101041) other "
                "from {schema}.b08101 "
                "where geoid in ('{tracts}') "
                    "and fileid = '{fileid}' ) "
                "select car/total*100 pct_car, carpool/total*100 pct_carpool, "
                    "trans/total*100 pct_trans, walk/total*100 pct_walk, "
                    "other/total*100 pct_other "
                "from agg")
    trans = pd.read_sql(q_trans.format(**table_params), engine_census)

    q_move = ("with agg as "
		"(select sum(b07001001) total, sum(b07001017) same_home,"
		    "sum(b07001033) same_county, sum(b07001049) same_state,  "
                    "sum(b07001065) diff_state, sum(b07001081) diff_country "
                 "from {schema}.b07001 "
                 "where geoid in ('{tracts}') "
                "and fileid = '{fileid}') "
            "select same_home/total*100 same_home, "
            "same_county/total*100 same_county, "
            "same_state/total*100 same_state, "
            "diff_state/total*100 diff_state, "
            "diff_country/total*100 diff_country "
            "from agg")
    move = pd.read_sql(q_move.format(**table_params), engine_census)

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
             "from {schema}.b15001 "
                 "where geoid in ('{tracts}')"
                 "and fileid = '{fileid}') "
            "select no_dip/total*100 no_dip, dip/total*100 dip,"
                "assoc/total*100 assoc, bach/total*100 bach, "
                "grad/total*100 grad "
            "from agg")
    ed_vals = engine_census.execute(q_ed.format(**table_params)).fetchone()
    ed_pct = [int(round(i)) for i in ed_vals]

    q_lang = ("with agg as "
		"(select sum(b16001001) total, "
	            "sum(b16001002) eng, sum(b16001003) span "
         	"from {schema}.b16001 "
                    "where geoid in ('{tracts}') "
                    "and fileid = '{fileid}') "
             "select eng/total*100 eng, span/total*100 span "
             "from agg")
    lang_vals = engine_census.execute(q_lang.format(**table_params)).fetchone()
    lang_pct = [int(round(i)) for i in lang_vals]

   #------------------------------------------------------------------------------------ 
   #--------------------------- Population Pyramid ----------------------- ---
   #------------------------------------------------------------------------------------ 

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
    q_age = "select {columns} from {schema}.b01001 where geoid in ('{tracts}')"
    table_params["columns"] = ",".join(male_cols)
    df_male = pd.read_sql(q_age.format(**table_params), engine_census)
    table_params["columns"] = ",".join(female_cols)
    df_female = pd.read_sql(q_age.format(**table_params), engine_census)
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
    plt.savefig('./Pictures/pop_pyramid.jpg', dpi=300)
    plt.close()

   #------------------------------------------------------------------------------------ 
   #--------------------------- Male vs Female Chart -----------------------------------
   #------------------------------------------------------------------------------------ 


    yticks = [0., .25, .5, .75, 1.]
    ytick_labels = ['0%', '25%', '50%', '25%', '0%']
    df[['pctm', 'pctf']].plot.bar(stacked=True, 
            width=.94, color=['#79B473','#440D0F'])
    plt.legend(['Male', 'Female'],loc='upper center',bbox_to_anchor=(.5, 1.05), ncol=2,
            fancybox=True, shadow=True)
    plt.xlabel('Age')
    plt.ylabel('Percent of Group')
    plt.yticks(yticks, ytick_labels)
    plt.xticks([i for i in range(len(age_labels))], age_labels)
    pctile_x = lambda x: [x for i in range(len(df.index))]
    plt.plot(pctile_x(.25), '--k')
    plt.plot(pctile_x(.5), 'k')
    plt.plot(pctile_x(.75), '--k')
    plt.title("Ratio of Male to Female", {"y":1.03})
    plt.tight_layout()
    plt.savefig('./Pictures/mf_ratio.jpg', dpi=300)
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

def ownership_profile():
    """
    Generates maps and charts for Ownership Profile page of neighborhood report

    Maps:
        - landbank
            + current_landbank
        - owner occupancy

        
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
            colors=('#003BD1', '#79B473'),
            title={'label': 'Ownership Occupancy Totals', 'loc':'center'},
            labels=["{0} ({1}%)".format(k, v) for k, v in vals.items()],
            legend={'loc': 'center', 'bbox_to_anchor': (0.5, -0.4), 
                'ncol': len(own_totals), 'framealpha': 0})
    ax = plt.gca()
    ax.set_facecolor('black')
    plt.tight_layout()
    plt.savefig('./Pictures/ownerocc_waffle.jpg', dpi=300)
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
    plt.savefig('./Pictures/top_owners.jpg', dpi=300)
    plt.close()

    own_count = pd.read_sql("select * from own_count", engine_blight)
    
    total_parcels = sum(own_totals)
    unique_own = own_count.shape[0]
    sum_top_own = df_own.props.sum()
    pct_top_own = int(round(sum_top_own/float(total_parcels)*100,0))
    total_own_occ = own_totals[0]
    pct_own_occ = int(round(total_own_occ/float(total_parcels)*100, 0))
    long_string =  "{0} parcels ({1}%) owned by 5 owners and {2} ({3}%) owner occupied"
    print "Total Parcels: {}".format(total_parcels)
    print "Unique Owners: {}".format(unique_own)
    print long_string.format(sum_top_own, pct_top_own, total_own_occ, pct_own_occ)


def make_property_table(nbhood, schema="public", table="sca_parcels"):
    """
    creates PostgreSQL temporary table of parcels that are within neighborhood

    Args:
        nbhood (str): String representing name of the neighborhhood as it exists
            in the `geography.boundary` table in the blight_data database
        schema (str, optional): PostgreSQL schema where the parcel data are stored default
            value is the public schema
        table (str, optional): name of the parcel file to be used to generate the temp
            table. Default value is `sca_parcels`
    """

    if table == "sca_parcels":
        pardat = "sca_pardat"
    else:
        pardat = "sca_pardat_2001"
    params = {"schema":schema,
              "table":table,
              "nbhood":nbhood,
              "pardat":pardat}

    q_nbhood_props =("drop table if exists reports.nbhood_props;"
                    "create table reports.nbhood_props as "
                    "select parcelid, lower(concat(adrno, adrstr)) paradrstr,"
                    "initcap(concat(adrno, ' ', adrstr)), p.wkb_geometry  "
                    "from {schema}.{table} p, {pardat}, " 
                        "geography.boundaries b "
                    "where st_intersects(st_centroid(p.wkb_geometry), "
                            "b.wkb_geometry) "
                    "and name = '{nbhood}' "
                    "and parcelid = parid;"
                    "create index gix_nbhood_props "
                    "on reports.nbhood_props using gist(wkb_geometry);")
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
    Maps:
        - code_enforcement
            + code_enforcement_incidents


    """
    if not property_table_exists():
        make_property_table(NEIGHBORHOOD)
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
    keep = req_count >= req_count.mean()
    reqs = req_count[keep].index
    num_colors = len(reqs)
    cm = plt.get_cmap('gist_rainbow')
    fig, ax = plt.subplots()
    #generate random colors to avoid duplicating colors 
    ax.set_color_cycle([cm(1.*i/num_colors) for i in range(num_colors)])
    for req in [r for r in reqs if r != "Downtown Cleanup"]:
        y = (df_code[df_code.request_type == req]
                            .request_type
                            .groupby(df_code.reported_date.dt.year).count())
        x = sorted(df_code[df_code.request_type == req].reported_date.dt.year.unique())
        x_smoothe = np.linspace(x[0], x[-1], 200)
#        y_smoothe = BSpline(x, y, x_smoothe)
        pch = pchip(x,y)
        plt.plot(pch(x_smoothe), label=req)

    plt.legend(loc='upper center',bbox_to_anchor=(.5, 1.05), ncol=3,
            fancybox=True, shadow=True, fontsize='xx-small')
    ax.set_xticklabels(lbls)
    ax.set_ylabel("Number of Requests")
    ax.set_xlabel("Year")
    title = "Most Frequent Code Enforcement Requests {} to present".format(min(lbls))
    plt.title(title, {"y":1.02})
    plt.tight_layout()
    plt.savefig('./Pictures/code_viols_all.jpg', dpi=300)
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
                center=df_code_grp.parcelid.mean(),
                cbar_kws={"label": "Number of Requests"})
    plt.xlabel("Year")
    plt.xticks(rotation=45)
    plt.ylabel("Month")
    plt.title("Total Requests {0} to {1}".format(df_code_grp.year.min(),
                                                 df_code_grp.year.max()))
    plt.tight_layout()
    plt.savefig('./Pictures/code_req_heatmap.jpg', dpi=300, bbox_inches="tight")
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
    plt.savefig('./Pictures/req_by_type.jpg', dpi=300)
    plt.close()
    
    #calculate values for text in middle of page
    total_parcels = engine_blight.execute(("select count(parcelid) "
                                            "from reports.nbhood_props")).fetchone()[0]

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

    q_vals = ("select count(t.{parcelid}) from {table} t, reports.nbhood_props p "
              "where t.{parcelid} = p.parcelid {options}")

    results = dict()
    for t in q_tables:
        result = engine_blight.execute(q_vals.format(**t)).fetchone()[0]
        results[t["table"]] = pct(result/float(total_parcels))

def pct(val):
    return str(int(round(val * 100, 0))) + "%"

def financial_profile():
    """
    TODO:
        - Total number of mortgage originations for past year
    """
    tax_yr = engine_blight.execute("select taxyr from sca_pardat limit 1").fetchone()[0]
    if not property_table_exists():
        make_property_table(NEIGHBORHOOD)
    df_props = pd.read_sql("select * from nbhood_props", engine_blight)

    #--------------------------Percent change in appraised value-------------------------
    q_appr = ("select parcelid, apr_cur, apr01 "
              "from nbhood_props, "
              "(select asmt.parid, a01.rtotapr apr01, asmt.rtotapr apr_cur "
                "from sca_asmt asmt, geography.sca_asmt_2001 a01 "
                "where asmt.parid = a01.parid) a "
              "where parcelid = parid")
    df_appr = pd.read_sql(q_appr, engine_blight)
    df_appr["apr01_adj"] = utils.inflate(2001, tax_yr, df_appr.apr01)
    pct_chg = lambda y1, y2: (y2-y1)/y1*100
    df_appr["pct_chg"] = pct_chg(df_appr.apr01_adj, df_appr.apr_cur)
    df_appr.to_sql("appraisal_change", engine_blight, schema="reports", if_exists="replace")
    #Total percent change in apprasied value all properties
    tot_chg = round((df_appr.apr_cur.sum()-df_appr.apr01_adj.sum())/
                        df_appr.apr01_adj.sum()*100, 2)

    #-----------------------------Average gross rent------------------------------------
    fileid = ACS_SCHEMA.split("_")[-1] 
    q_rent = ("select rent.geoid, b25003003 renter, b25065001 agg_rent "
              "from {0}.b25003 tenure, {0}.b25065 rent "
              "where tenure.geoid = rent.geoid "
              "and rent.geoid in ('{1}') "
              "and rent.fileid = '{2}e5' "
              "and tenure.fileid = '{2}e5'")
    df_rent = pd.read_sql(q_rent.format(*[ACS_SCHEMA, 
                                          "','".join([g for g in t]), 
                                          fileid]), engine_census)
    avg_rent = round(df_rent.agg_rent.sum()/df_rent.renter.sum(), 2)

    #------------------------Median Residential sale value------------------------------
    #The PostgreSQL median function needs to be created if it doesn't already exist
    #the code can be found at https://wiki.postgresql.org/wiki/Aggregate_Median 
    q_sales = ("select median(price) "
               "from "
               "(select s.parid, case when numpars > 1 then price/numpars "
		    "else price "
                    "end as price "
      		  "from sca_sales s, sca_pardat p "
		  "where s.parid = p.parid "
                    "and class = 'R' and price > 0 and instrtyp = 'WD' "
		    "and date_part('year', saledt::date) >= {0}) p {1}")
    #date range limited to sales over past year
    dt_val = str(tax_yr - 1)
    #limit selection to parcels in neighborhood parcels
    sales_nbhood = (engine_blight.execute(q_sales
                                 .format(dt_val, ", nbhood_props where parcelid = parid"))
                                 .fetchone()[0])
    #limit selection to city parcels
    sales_city = (engine_blight.execute(q_sales
                                 .format(dt_val, "where substring(parid, 1,1) = '0'"))
                                 .fetchone()[0])
    #median for all parcels in county
    sales_county = (engine_blight.execute(q_sales
                                 .format(dt_val, ""))
                                 .fetchone()[0])

    #-----------------------------------Tax Sale-----------------------------------------
    q_tax = ("select parcelid, sum(sumdue) due, sum(sumrecv) recv, status "
             "from nbhood_props n, sc_trustee t "
             "where n.parcelid = parid "
             "and load_date = (select max(load_date) from sc_trustee) "
             "group by parcelid, status "
             "order by parcelid")
    df_tax = pd.read_sql(q_tax, engine_blight)
    ct_elig = df_tax[df_tax.status == "Eligible"].shape[0]
    ct_total = df_props.shape[0]
    pct_elig = int(round(ct_elig/float(ct_total)*100))
    ct_active = df_tax[df_tax.status == "Active"].shape[0]
    pct_active = int(round(ct_active/float(ct_total)*100))
    df_tax.to_sql("tax_sale", engine_blight, schema="reports", if_exists="replace")

def intro_page(nbhood_name):
    """
    location_overview:
        CANVAS
        - boundaries
        - boundary mask
        - bldg_2014
        - streets_labels
            + interstate
            + major
            + collector
            + local
        - streets_carto
        INSET MAP
        - boundaries
        - Inset
            + streets_carto_copy
            + tiger_place_2016
        COMPOSER
        - scale_bar
        - inset_map
        - main_map
    
    """

def run_neighborhood_report(nbhood_name):
    """
    Main method for generating neighborhood report. This method works with the following
    methods to generate the content for each of the five pages that comprise a complete
    neighborhood report:
        + intro_page
        + property_conditions
        + ownership_profile
        + neighborhood_profile
        + financial_profile
    Args:
        nbhood_name (str): String corresponding to the name of the neighborhood boundary
            as it exists in `geography.boundary` in the blight_data database
    
    Returns:
        None
    """
    os.chdir("./neighborhood")
    dir_name = nbhood_name.replace(" ", "_")
    report_name = dir_name + "_report.odt"
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
    os.mkdir(dir_name+"/Pictures")
    shutil.copytree("REPORT_TEMPLATE/maps", dir_name+"/maps")
    shutil.copy("REPORT_TEMPLATE/report_template.odt", os.path.join(dir_name, report_name))
    os.chdir(dir_name)
    for f in os.listdir("./maps"):
        with open("./maps/"+f, "r") as f_qgs:
            qgis_doc = f_qgs.read()
        str_formatter = string.Formatter()
        qgis_doc_new = str_formatter.vformat(qgis_doc, None, 
                                             defaultdict(str, neighborhood=nbhood_name)
                                            )
        with open("./maps/"+f,"w") as f_qgs:
            f_qgs.write(qgis_doc_new)
    
    #creates table (reports.nbhood_props) in postgres with all parcels in the study area
    make_property_table(nbhood_name) 

    
def main(args):
    if args["neighborhood"]:
        NEIGHBORHOOD = args["<neighborhood_name>"]
        run_neighborhood_report(NEIGHBORHOOD)
    elif args["violator"]:
        run_violator_report(args["<month>"], 
                            args["--year"]
                           )

if __name__=="__main__":
    args = docopt(__doc__)
    main(args)

