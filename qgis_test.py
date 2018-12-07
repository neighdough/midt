import os
from collections import namedtuple
from datetime import date
from PyQt4 import QtGui, uic
from PyQt4.QtCore import pyqtSignal, QSettings
from qgis.core import *
from qgis.gui import QgsMessageBar

canvas = iface.mapCanvas()
import sys 
from qgis.core import (QgsProject, QgsComposition, QgsApplication, 
                      QgsMapLayerRegistry)
from qgis.gui import QgsMapCanvas, QgsLayerTreeMapCanvasBridge 
from PyQt4.QtCore import QFileInfo, QSize 
from PyQt4.QtXml import QDomDocument 
from PyQt4.QtGui import QImage, QPainter 
import os 

os.chdir("/home/nate/dropbox-caeser/Data/MIDT/Data_Warehouse/reports/neighborhood/GENERATED_TEMPLATES/South_City/maps") 
gui_flag = True 
app = QgsApplication(sys.argv, True) 
QgsApplication.setPrefixPath("/usr", True) 
QgsApplication.initQgis() 

project_path = 'report_maps.qgs' 
template_path = 'owner_occupancy.qpt' 

canvas = QgsMapCanvas() 
canvas.resize(QSize(1450, 850)) 
#start = time.time()
QgsProject.instance().read(QFileInfo(project_path)) 
#end = time.time()
root = QgsProject.instance().layerTreeRoot() 
bridge = QgsLayerTreeMapCanvasBridge(root, canvas) 
bridge.setCanvasLayers() 
registry = QgsMapLayerRegistry.instance() 

template_file = file(template_path) 
template_content = template_file.read() 
template_file.close() 
document = QDomDocument() 
document.setContent(template_content) 
map_settings = canvas.mapSettings()
composition = QgsComposition(map_settings) 
#start = time.time()
composition.loadFromTemplate(document) 
#end = time.time()

#create list of all layers currently in the map 
map_layers = [lyr for  lyr in registry.mapLayers() if root.findLayer(lyr)] 

