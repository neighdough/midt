"""

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
                          analysis. Months should be numeric representation 
                          (i.e. 1 for Jan, 2 for Feb, etc.)
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
import sys
sys.path.append('/home/nate/source')
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
                       QgsMapLayerRegistry, QgsComposerScaleBar)
from qgis.gui import QgsMapCanvas, QgsLayerTreeMapCanvasBridge
from PyQt4.QtCore import QFileInfo, QSize
from PyQt4.QtXml import QDomDocument
from PyQt4.QtGui import QImage, QPainter
from scipy.interpolate import pchip
import seaborn as sns
import shutil
from sqlalchemy import text
import string
import subprocess
import time
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
ACS_SCHEMA = "acs5yr_2015" #"acs5yr_2016"
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

    def __init__(self, nbhood_name, *args, **kwargs):
        """
        Parameters:
            nbhood_name (str): Name of the neighborhood that report is being generated
                for.
            z_in (str): Name of the template being used.
            z_out (str): Name of the odt document that will be created upon report
                completion
        """
        self.nbhood = nbhood_name
        dir_name = self.nbhood.replace(" ", "_")
        self.zip_in = zipfile.ZipFile(dir_name+"_report.odt")
        self.zip_out = zipfile.ZipFile(dir_name+".odt", "w")
        self.xml_content = self.zip_in.read("content.xml")
        self.xml_manifest = self.zip_in.read("META-INF/manifest.xml")
        self.root_content = etree.fromstring(self.xml_content)
        self.root_manifest = etree.fromstring(self.xml_manifest)
        
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

    def update_image_path(self, img_name):
        """
        update picture names referenced in 
        """
        img_path = ("//draw:frame[@draw:name='{}']/draw:image".format(img_name))
        img = self.root_content.xpath(img_path, namespaces=self.ns)[0]
        orig_img = img.attrib["{{{xlink}}}href".format(**self.ns)]
        new_img = "Pictures/" + img_name + ".jpg"
        #update image name in file manifest before updating link in content.xml
        path = "//manifest:file-entry[@manifest:full-path='{}']".format(orig_img)
        manifest_attr = self.root_manifest.xpath(path.format(path),namespaces=self.ns)[0]
        manifest_attr.attrib["{{{manifest}}}full-path".format(**self.ns)] = new_img
        img.attrib["{{{xlink}}}href".format(**self.ns)] = new_img

       
    def create_titles(self):
        """
        """
        pass
    
    def save_report(self):
        """
        Finalize report and save to disk
        """
        for f in self.zip_in.filelist:
            if (f.filename not in ["content.xml", "META-INF/manifest.xml"]
                    and "Pictures" not in f.filename):
                self.zip_out.writestr(f.filename, self.zip_in.read(f.filename))
        self.zip_out.writestr("content.xml", etree.tostring(self.root_content))
        self.zip_out.writestr("META-INF/manifest.xml", 
                etree.tostring(self.root_manifest))  
        for pic in os.listdir("./Pictures"):
            self.zip_out.write("./Pictures/"+pic)
        self.zip_out.close()
        # time.sleep(40)
        print "Converting to pdf."
        # subprocess.check_output(["libreoffice", "--convert-to", "pdf:writer_pdf_Export", 
            # "--outdir", "../../COMPLETED_REPORTS", self.zip_out.filename])
        # cmd = ("libreoffice --headless --convert-to pdf:writer_pdf_Export "
                # "--outdir {0} {1}".format(os.path.join(os.environ["HOME"],
                                                  # "caeser-nas1/ftproot/npi/neighborhood_reports"),
                                                  # self.zip_out.filename))
        cmd = ("libreoffice --headless --convert-to pdf:writer_pdf_Export "
                "--outdir ../../COMPLETED_REPORTS {}".format(self.zip_out.filename))
        subprocess.call(cmd, shell=True)

    def insert_image(self, image_name):
        """
        use image name to update 
        """
        pass

    def update_table(self, table_name, header_row, row_values):
        """
        update values in table

        Args:
            table_name (str): name of table in table:name tag to be used in query to pull
                rows for to be updated
            header_row (list:int): list of integers containing header rows to be skipped
                while updating row values
            row_values (list:list:str): nested list of values with new data to be 
                inserted into final report

        Returns:
            None
        """
        table_xpath = "//table:table[@table:name='{}']/table:table-row".format(table_name)
        xml_table = self.root_content.xpath(table_xpath, namespaces=self.ns)
        num_rows = len(xml_table)
        for row_num in [i for i in range(num_rows) if i not in header_row]:
            row = xml_table[row_num].xpath(".//text:p", namespaces=self.ns)
            col_num = 0
            new_row = row_values.pop(0)
            for cell in row:
                val = new_row[col_num]
                cell.text = str(val)
                col_num += 1

    def update_title(self, neighborhood_name, tag_name="main"):
        """
        Update title on each page on report.
        
        Args:
            neighborhood_name (str): name of the neighborhood that the report is being 
                generated for.
        Optional Args:
            tag_name (str): references the element tag in content.xml to determine if the 
                date should be included in the string. `main` refers to the title on the
                main page of the report. Acceptable values include:
                    + own
                    + property
                    + neighborhood
                    + financial
        Returns:
            None
        """
        title_xpath = ("//draw:frame[@draw:name='title_{}']/"
                       "draw:text-box/text:p/text:span")

        title = self.root_content.xpath(title_xpath.format(tag_name), 
                namespaces=self.ns)[1]
        if tag_name == "main":
            title.text = neighborhood_name
        else:
            title.text = "{0} {1}".format(neighborhood_name, 
                                    "{:%B %d, %Y}".format(datetime.datetime.today()))
    
    def update_text(self, tag_name, text_values):
        """
        Update text value in content.xml for the specified tag

        Args:
            tag_name (str): tag name for the text element to be updated in content.xml
            text_values (list:str): list containing strings to be inserted into the
                specified element

        Returns:
            None
        """
        text_path = ("//draw:frame[@draw:name='{}']/draw:text-box/text:p/text:span")
        values = self.root_content.xpath(text_path.format(tag_name), namespaces=self.ns)
        for i in range(len(values)):
            values[i].text = text_values[i] if type(text_values) == list else text_values

class QgisMap:
    """
    Class used to automatically export images from pre-formatted QGIS maps. Individual
    map elements can be added to each project to allow for a little more flexibility
    in the maps that are automated.
    """
    
    def __init__(self, project_name, *args, **kwargs):
        """
        
        Parameters:
            project_name (str): Name of qgs file containing maps to be generated
         """
        gui_flag = True
        self.app = QgsApplication(sys.argv, True)
        self.app.setPrefixPath("/usr", True)
        self.app.initQgis()

        self.project_name = project_name
        self.project = QgsProject.instance()
        self.project.read(QFileInfo(project_name))
        # self.basemap_layers = []
   
    def root(self):
        return self.project.layerTreeRoot()

    def close(self):
        self.project.clear()
        self.app.exitQgis()

    def set_basemap_layers(self, layers):
        self.basemap_layers = layers



class QgisTemplate():
  
    def __init__(self, qgis_map, template_name, *args, **kwargs):
        """
        
        Parameters:
            qgis_map (obj: QgisMap): a QGIS map document object
            template_name (str): Each QGIS map composer must be saved as a template 
                (qpt) file in order to be loaded for automated map generation.
        """
        self.canvas = QgsMapCanvas()
        self.root = qgis_map.root()
        self.bridge = QgsLayerTreeMapCanvasBridge(self.root, self.canvas)
        self.bridge.setCanvasLayers()

        self.registry = QgsMapLayerRegistry.instance()

        self.map_layers = [lyr for lyr in self.registry.mapLayers() if self.root.findLayer(lyr)]
        #only layers in neighborhood reports need to be reordered
        if template_name != "map_template.qpt":
            self.reorder_layers()
        self.scale = self.canvas.scale()
        self.template_file = file(template_name)
        self.template_content = self.template_file.read()
        self.template_file.close()
        self.document = QDomDocument()
        self.document.setContent(self.template_content)
        self.map_settings = self.canvas.mapSettings()
        self.composition = QgsComposition(self.map_settings)
        self.composition.loadFromTemplate(self.document)
        self.rectangle = self.canvas.extent()
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
                                    {"setText": args},
                                QgsComposerScaleBar:
                                    {"applyDefaultSize": args,
                                     "setVisibility": args}
                                }
        if "basemap" in kwargs:
            self.basemap = kwargs["basemap"]
        if "layers" in kwargs:
            self.layers = kwargs["layers"]

    def reorder_layers(self):#, thematic_layers, basemap_layers):
        """
        Reorder layer names in list so that they draw in the correct order once set
        in visible layer list. Layers will be inserted above the last two layers in 
        basemap layers. Thematic layers should be listed in reverse order in which they
        should be displayed in the map.

        Args:
            thematic_layers (list:str): list of map layers to be inserted.
            basemap_layers (list:str): list of current basemap layers.
        Returns:
            basemap (list:str)
        """
        for i in range(len(self.map_layers)):
            if "sca_parcels" in self.map_layers[i]:
                idx = i
        parcels = self.map_layers.pop(idx)
        self.map_layers.insert(len(self.map_layers), parcels)

        # basemap = [lyr for lyr in basemap_layers]
        # for lyr in thematic_layers:
            # basemap.insert(-2, lyr)
        # return basemap
        
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
        new_scale = self.canvas.scale() * 1.2
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

    def set_visible_layers(self, map_element, visible_layers, 
            zoom_layer=None,keep_set=False):
        """
        Set which layers are visible in canvas before saving

        Args:
            map_element (str): name of the composer map element
            visible_layers (list:str): list containing names of layers as written in 
                TOC that should be visible on exported image.
        Optional Args:
            zoom_layer (str): name of layer to be used as map extent
            keep_set (bool): determine whether to lock current layers when working with 
                multiple map items in a single composer such as an inset map.
        Returns:
            None
        """
        #if not composer_layers:
        #    composer_layers = self.map_layers
        # if not self.has_layers():
           # msg = ("Layers cannot be set because none have been provided. Run `set_layers` "
                   # "method and try again."
                   # )

           # print msg
           # return
        # else:
        visible = []
        for m_lyr in self.map_layers:
            lyr = self.root.findLayer(m_lyr)
            if lyr.layerName() in visible_layers:#self.basemap + self.layers:
                visible.append(m_lyr)
                lyr.setVisible(2) #Qt.CheckState checked
            else:
                lyr.setVisible(0) #Qt.CheckState unchecked
        if zoom_layer:
            self.zoom_to_layer(zoom_layer)
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
        #self.project.clear()
        #self.app.exit()#self.app.exitQgis()
    
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
    nhood_doc = QgisMap("map.qgs")
    nhood_map = QgisTemplate(nhood_doc, "map_template.qpt")
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
    nhood_map.save_map(os.path.join(os.environ["HOME"],
        "caeser-nas1/ftproot/npi/code_violator_report",
        map_title))
    

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
            #"geography.bldg_cdc_boundaries b "
            #"geography.boundaries b "
            "geography.boundaries b "
         "where st_intersects(t.wkb_geometry, b.wkb_geometry) "
            "and b.name = '{}'")
    tracts = engine_blight.execute(q.format(nbhood)).fetchall()
    return [i[0] for i in tracts]

def formatted(string, key):
    string_formats = {"dollars": "${:,}",
                      "percent": "{:.0f}%",
                      "numeric": "{:,}"
                      }
    return string_formats[key].format(string)

def neighborhood_profile(nbhood, report, map_document):
    report.update_title(nbhood, "neighborhood")
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
    report.update_image_path("pop_change_tracts")
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
    #create nested list containing 4 rows in demographic table
    dem_table_values = [[] for row in range(4)]
    #median income
    q_inc = ("select * from {schema}.b19001 "
             "where geoid in ('{tracts}') and fileid = '{fileid}'")
    inc = pd.read_sql(q_inc.format(**table_params), engine_census)
    skip = ['geoid', 'name', 'stusab', 'sumlevel', 'fileid','id']
    inc_list = []
    for col in [col for col in inc.columns if col not in skip]:
        inc_list.append(inc[col].sum())
    nbhood_mdn = int(round(calculate_median(inc_list)))
    dem_table_values[0].append(formatted(nbhood_mdn,"dollars"))
    #poverty
    q_pov = ("select sum(b17001002) total, "
             "sum(b17001002)/sum(b17001001)*100 pct_bel_pov "
             "from {schema}.b17001 "
             "where geoid in ('{tracts}') and fileid = '{fileid}';")
    pov = engine_census.execute(q_pov.format(**table_params)).fetchone()
    dem_table_values[0].append(formatted(pov[1],"percent"))
    #language
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
    dem_table_values[0].append("") #add empty value for missing column in row
    dem_table_values[0].extend([formatted(i,"percent") for i in lang_pct])

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
    trans = engine_census.execute(q_trans.format(**table_params)).fetchall()[0]
    dem_table_values[1] = [formatted(round(i), "percent") for i in trans]
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
    move = engine_census.execute(q_move.format(**table_params)).fetchall()[0]
    dem_table_values[2] = [formatted(round(i), "percent") for i in move]

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
    dem_table_values[3] = [formatted(i, "percent") for i in ed_pct]
    headers = [0,1,3,4,6,7,9,10]
    #add empty "rows" to stand in for headers so list is same shape as table
    # full_dem_table = []
    # for i in range(12):
        # if i in headers:
            # full_dem_table.append([])
        # else:
            # full_dem_table.append(dem_table_values.pop(0))

    report.update_table("tbl_demographic", headers, dem_table_values)#full_dem_table)

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
    report.update_image_path("pop_pyramid")
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
    report.update_image_path("mf_ratio")

    #update map image
    template_landuse = "land_use"
    map_landbank = QgisTemplate(map_document, 
                           "./maps/{}.qpt".format(template_landuse))
#    visible_layers = map_landbank.reorder_layers(["tracts", "land_use"], 
#                                    map_document.basemap_layers)
    visible_layers = ["boundaries", "boundary_mask", 
                      #"streets_labels", "steets_carto", 
                      "nhd_waterbody", "sca_parcels",
                      "tracts", "land_use"
                      ]

    #basemap_layers = map_document.basemap_layers
    map_landbank.set_visible_layers("main_map", visible_layers, "boundaries")
    map_landbank.save_map("./Pictures/{}".format(template_landuse), "jpg")
    report.update_image_path(template_landuse)
   

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

def ownership_profile(nhood_name, report, map_document):
    """
    Generates maps and charts for Ownership Profile page of neighborhood report

    Maps:
        - landbank
            + current_landbank
        - owner occupancy

        
    """
    #------------------------------------------------------------------------
    #----------------------- Ownership Profile Maps -------------------------
    #------------------------------------------------------------------------
    template_landbank = "landbank"
    map_landbank = QgisTemplate(map_document, 
                           "./maps/{}.qpt".format(template_landbank))
    
    landbank_layers = ["boundaries", "boundary_mask", 
                       #"streets_labels", "steets_carto", 
                       "nhd_waterbody", "sca_parcels",
                       "current_landbank"]
#    basemap_layers = map_document.basemap_layers
#    landbank_layers = map_landbank.reorder_layers(["current_landbank"], basemap_layers)
    map_landbank.set_visible_layers("main_map", landbank_layers, "boundaries")
    # map_landbank.add_element("scale_bar")
    map_landbank.save_map("./Pictures/{}".format(template_landbank), "jpg")
    report.update_image_path(template_landbank)
    template_own = "owner_occupancy"
    map_own = QgisTemplate(map_document,
                      "./maps/{}.qpt".format(template_own))
    own_layers = ["boundaries", "boundary_mask", 
                  #"streets_labels", "streets_carto",
                   "nhd_waterbody", "sca_parcels",
                   "own_occ_parcels"]
#    own_layers = map_own.reorder_layers(["own_occ_parcels"], basemap_layers)
    map_own.set_visible_layers("main_map", own_layers, "boundaries")
    # map_own.add_element("scale_bar")
    map_own.save_map("./Pictures/{}".format(template_own), "jpg")
    report.update_image_path(template_own)

    #selects distinct owner names for ownership in neighborhood
    q_make_distinct = ("drop table if exists reports.own_count;"
                        "create table reports.own_count as "
                            "select own_adr, count(own_adr) from "
                            "(select parcelid, parid, "
                                "concat(adrno, ' ', adrstr) par_adr "
            	            "from sca_parcels p, sca_pardat, "
                            #"geography.boundaries b "
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
    engine_blight.execute(q_make_distinct.format(nhood_name))
    
    #------------------------------------------------------------------------
    #------------------Waffle Chart for Owner Occupancy----------------------
    #------------------------------------------------------------------------
   
    #gets break down of owner occupancy totals
    q_ownocc = ("with own as "
                "(select lower(concat(adrno,adrstr)) ownadr, parid "
                        "from sca_owndat) "
                "select ownocc, nonownocc from "
                "(select count(parcelid) ownocc from reports.nbhood_props, own "
                "where parcelid = parid and paradrstr = ownadr) oc "
                "join "
                "(select count(parcelid) nonownocc from reports.nbhood_props, own "
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
    waffle_name = "ownerocc_waffle"
    plt.savefig('./Pictures/{}.jpg'.format(waffle_name), dpi=300)
    plt.close()
    report.update_image_path(waffle_name)


    #select counts for unique owners in neighborhood
    q_own = ("select distinct on(count, own_adr) initcap(own) as own, "
            "initcap(concat(own_adr, ' ', statecode, ' ', zip1)) as adr, "
            "count as props "
            "from "
            "(select own_adr, count from reports.own_count "
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
    top_owner_name = "top_owners"
    plt.savefig('./Pictures/{}.jpg'.format(top_owner_name), dpi=300)
    plt.close()
    report.update_image_path(top_owner_name)
    
    #update ownership table in report
    df_own.index += 1 
    new_values = [[row for row in rows] for rows in df_own.to_records()]
    report.update_table("tbl_own", [0], new_values)

    own_count = pd.read_sql("select * from reports.own_count", engine_blight)
    
    total_parcels = sum(own_totals)
    unique_own = own_count.shape[0]
    sum_top_own = df_own.props.sum()
    pct_top_own = int(round(sum_top_own/float(total_parcels)*100,0))
    total_own_occ = own_totals[0]
    pct_own_occ = int(round(total_own_occ/float(total_parcels)*100, 0))
    paragraph = ["{} contains a total of ".format(nhood_name),
                 str(total_parcels),
                 " parcels with ",
                 str(unique_own),
                 " unique owners. ",
                 str(sum_top_own),
                 " parcels ({}%) are owned by 5 different owners and ".format(pct_top_own),
                 str(total_own_occ),
                 " ({}%) are owner occupied.".format(pct_own_occ),
                 ""
                 ]
    report.update_text("par_own", paragraph)
    text_par_own = ("{neighborhood} contains a total of {parcel_count} parcels\n"
               "with {unique_count} unique owners. {parcel_count_top_owners} parcels "
               "({pct_top_owners}%)\nare owned by 5 different owners and {owner_occ_count}"
               " ({pct_owner_occ}%)\nare onwer occupied")
    paragraph_tag = "par_own"
    report.update_title(nhood_name, "own")

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
                        #"geography.boundaries b "
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
        q_table = "select * from reports.nbhood_props"
        engine_blight.execute(q_table)
        return True
    except:
        return False

def property_conditions(nbhood_name, report, map_document):
    """
    Maps:
        - code_enforcement
            + code_enforcement_incidents


    """
    # if not property_table_exists():
        # make_property_table(NEIGHBORHOOD)
    report.update_title(nbhood_name, "property")
    df_props = pd.read_sql("select * from reports.nbhood_props", engine_blight)
    #selects all of the code enforcement violations over time
    q_code = ("select * from "
	"reports.nbhood_props,"
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
    report.update_image_path("code_viols_all")
    
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
    report.update_image_path("code_req_heatmap")

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
    report.update_image_path("req_by_type")
    
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
    
    report.update_text("txt_code_enf", [results["com_incident"]])
    report.update_text("txt_elec", [results["mlgw_disconnects"]])
    report.update_text("txt_vacant", [results["sca_pardat"]])
    
    template_code = "code_enforcement"
    #basemap_layers = map_document.basemap_layers
    map_code = QgisTemplate(map_document,
                      "./maps/{}.qpt".format(template_code))
    code_layers = ["boundaries", "boundary_mask", 
                   #"streets_labels", "steets_carto", 
                   "nhd_waterbody", "sca_parcels",
                   "code_enforcement_incidents"
                   ]
    #code_layers = map_code.reorder_layers(["code_enforcement_incidents"], basemap_layers)
    map_code.set_visible_layers("main_map", code_layers, "boundaries")
    map_code.save_map("./Pictures/{}".format(template_code), "jpg")
    report.update_image_path(template_code)


def pct(val):
    return str(int(round(val * 100, 0))) + "%"

def financial_profile(nhood_name, report, map_document):
    """
    TODO:
        - Total number of mortgage originations for past year
    """
    report.update_title(nhood_name, "financial")
    tax_yr = engine_blight.execute("select taxyr from sca_pardat limit 1").fetchone()[0]
    df_props = pd.read_sql("select * from reports.nbhood_props", engine_blight)

    #--------------------------Percent change in appraised value-------------------------
    q_appr = ("select parcelid, apr_cur, apr01 "

              "from reports.nbhood_props, "
              "(select asmt.parid, a01.rtotapr apr01, asmt.rtotapr apr_cur "
                "from sca_asmt asmt, geography.sca_asmt_2001 a01 "
                "where asmt.parid = a01.parid) a "
              "where parcelid = parid")
    df_appr = pd.read_sql(q_appr, engine_blight)
    df_appr["apr01_adj"] = utils.inflate(2001, tax_yr, df_appr.apr01)
    pct_chg = lambda y1, y2: (y2-y1)/y1*100
    df_appr["pct_chg"] = pct_chg(df_appr.apr01_adj, df_appr.apr_cur)
    df_appr.to_sql("appr_change", engine_blight, schema="reports", if_exists="replace")
    #Total percent change in apprasied value all properties
    tot_chg = round((df_appr.apr_cur.sum()-df_appr.apr01_adj.sum())/
                        df_appr.apr01_adj.sum()*100, 2)
    report.update_text("txt_appr_chng", formatted(tot_chg, "percent"))

    #-----------------------------Average gross rent------------------------------------
    tracts = get_tract_ids(nhood_name)
    fileid = ACS_SCHEMA.split("_")[-1] 
    q_rent = ("select rent.geoid, b25003003 renter, b25065001 agg_rent "
              "from {0}.b25003 tenure, {0}.b25065 rent "
              "where tenure.geoid = rent.geoid "
              "and rent.geoid in ('{1}') "
              "and rent.fileid = '{2}e5' "
              "and tenure.fileid = '{2}e5'")
    df_rent = pd.read_sql(q_rent.format(*[ACS_SCHEMA, 
                                          "','".join([g for g in tracts]), 
                                          fileid]), engine_census)
    avg_rent = round(df_rent.agg_rent.sum()/df_rent.renter.sum(), 2)
    report.update_text("txt_avg_rent", formatted(int(avg_rent), "dollars"))
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
                                 .format(dt_val, ", reports.nbhood_props where parcelid = parid"))
                                 .fetchone()[0])
    report.update_text("txt_mdn_res_sale_nbhd", formatted(int(sales_nbhood), "dollars")) 
    #limit selection to city parcels
    sales_city = (engine_blight.execute(q_sales
                                 .format(dt_val, "where substring(parid, 1,1) = '0'"))
                                 .fetchone()[0])
    report.update_text("txt_mdn_res_sale_mem", formatted(int(sales_city), "dollars"))
    #median for all parcels in county
    sales_county = (engine_blight.execute(q_sales
                                 .format(dt_val, ""))
                                 .fetchone()[0])
    report.update_text("txt_mdn_res_sale_county", formatted(int(sales_county), "dollars"))

    #-----------------------------------Tax Sale-----------------------------------------
    q_tax = ("select parcelid, sum(sumdue) due, sum(sumrecv) recv, status "
             "from reports.nbhood_props n, sc_trustee t "
             "where n.parcelid = parid "
             "and load_date = (select max(load_date) from sc_trustee) "
             "group by parcelid, status "
             "order by parcelid")
    df_tax = pd.read_sql(q_tax, engine_blight)
    ct_elig = df_tax[df_tax.status == "Eligible"].shape[0]
    ct_total = df_props.shape[0]
    pct_elig = int(round(ct_elig/float(ct_total)*100))
    report.update_text("txt_tax_sale_elig", formatted(pct_elig, "percent"))
    ct_active = df_tax[df_tax.status == "Active"].shape[0]
    pct_active = int(round(ct_active/float(ct_total)*100))
    report.update_text("txt_tax_sale_act", formatted(pct_active, "percent"))
    df_tax.to_sql("tax_sale", engine_blight, schema="reports", if_exists="replace")

    #update appraisal change map
    template_appraisal = "appraisal_change"
    map_appraisal = QgisTemplate(map_document, 
                           "./maps/{}.qpt".format(template_appraisal))
#    basemap_layers = map_document.basemap_layers
    #appraisal_layers = map_appraisal.reorder_layers(["appr_change"], basemap_layers)
    appraisal_layers = ["boundaries", "boundary_mask", 
                        #"streets_labels", "steets_carto", 
                        "nhd_waterbody", "sca_parcels",
                        "appr_change"
                        ]
    map_appraisal.set_visible_layers("main_map", appraisal_layers, "boundaries")
    map_appraisal.save_map("./Pictures/{}".format(template_appraisal), "jpg")
    report.update_image_path(template_appraisal)
    
    #update tax sale map
    template_tax = "tax_sale"
    map_tax = QgisTemplate(map_document, 
                           "./maps/{}.qpt".format(template_tax))
#    tax_layers = map_tax.reorder_layers(["tax_sale"], basemap_layers)
    tax_layers = ["boundaries", "boundary_mask", 
                  #"streets_labels", "steets_carto", 
                  "nhd_waterbody", "sca_parcels",
                  "tax_sale"
                  ]
    map_tax.set_visible_layers("main_map", tax_layers, "boundaries")
    map_tax.save_map("./Pictures/{}".format(template_tax), "jpg")
    report.update_image_path(template_tax)
 

def intro_page(nbhood_name, report, map_document):
    """
    Updates necessary elements contained on page 1 of report.

    Args:
        nbhood_name (str): name of neighborhood entered in terminal at run time
        report (:obj: Report): Report object created with `run_neibhorhood_report`
        map_document (:obj: QgisMap): QGIS map document object
    
    """
    template_name = "location_overview"
    qmap = QgisTemplate(map_document, 
                   "./maps/{}.qpt".format(template_name))
    inset_layers = ["overview_location", "streets_carto_inset", "tiger_place_2016"]
    map_layers = ["boundaries", "boundary_mask", "streets_labels", "bldg_2014", 
                  "streets_carto", "sca_parcels"
                  ]
    #inset_map
    qmap.set_visible_layers("inset_map", inset_layers, 
            "tiger_place_2016", True)
    #main map 
    qmap.set_visible_layers("main_map", map_layers,
            "boundaries", False)
    # qmap.add_element("scale_bar", kwargs={"u":1, "setVisibility": False})
    qmap.save_map("./Pictures/"+template_name)
    report.update_title(nbhood_name, "main")
    report.update_image_path(template_name)
    for org in ["caeser", "im", "npi"]:
        report.update_image_path(org+"_logo")


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
    os.chdir("./neighborhood/GENERATED_TEMPLATES")
    dir_name = nbhood_name.replace(" ", "_")
    report_name = dir_name + "_report.odt"
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
        os.mkdir(dir_name+"/Pictures")
        shutil.copytree("../REPORT_TEMPLATE/maps", dir_name+"/maps")
        shutil.copy("../REPORT_TEMPLATE/report_template.odt", os.path.join(dir_name, report_name))
        logo_path = "../REPORT_TEMPLATE/logos"
        for jpg in os.listdir(logo_path):
            shutil.copy("/".join([logo_path, jpg]),dir_name+"/Pictures")

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
    print "Building database tables.\n"
    make_property_table(nbhood_name) 
    print "Setting up report template.\n"
    report = Report(nbhood_name)
    report_map = QgisMap("./maps/report_maps.qgs")
    #"thematic_layer" is a placeholder for the layer to be activated and should be updated
    #for each map generated except for the location_overview since its layers are hardcoded
    basemap_layers = ["boundaries", "boundary_mask", "streets_labels", 
                      "steets_carto", "nhd_waterbody", "sca_parcels"
                      ]
#    report_map.set_basemap_layers(basemap_layers)
    print "Generating content for Page 1.\n"
    intro_page(nbhood_name, report, report_map)
    print "Generating content for Page 2.\n"
    ownership_profile(nbhood_name, report, report_map)
    print "Generating content for Page 3.\n"
    property_conditions(nbhood_name, report, report_map)
    print "Generating content for Page 4.\n"
    neighborhood_profile(nbhood_name, report, report_map)
    print "Generating content for Page 5.\n"
    financial_profile(nbhood_name, report, report_map)
    report.save_report()
    report_map.close()





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


