
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
   
    def root(self):
        return self.project.layerTreeRoot()

    def close(self):
        self.project.clear()
        self.app.exitQgis()



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
    

