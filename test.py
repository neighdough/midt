import sys 
from qgis.core import (QgsProject, QgsComposition, QgsApplication, 
                      QgsMapLayerRegistry)
from qgis.gui import QgsMapCanvas, QgsLayerTreeMapCanvasBridge 
from PyQt4.QtCore import QFileInfo, QSize 
from PyQt4.QtXml import QDomDocument 
from PyQt4.QtGui import QImage, QPainter 
import os 

os.chdir("/home/nate/dropbox-caeser/Data/MIDT/Data_Warehouse/reports/neighborhood/Orange_Mound/maps") 
gui_flag = True 
app = QgsApplication(sys.argv, True) 
QgsApplication.setPrefixPath("/usr", True) 
QgsApplication.initQgis() 

project_path = 'report_maps.qgs' 
template_path = 'location_overview.qpt' 

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
#these layers are common for all maps
basemap = ["streets_labels", "streets_carto", "sca_parcels",
            "boundaries", "boundary_mask"
          ]  

#this section sets up an inset map that shows the project location
#in context of the region
inset_layers = ["boundaries", "tiger_place_2016", "streets_carto_inset"] 
# #this loop is to activate layers shown in inset map 
inset_visible = []
for map_lyr in map_layers: 
    lyr = root.findLayer(map_lyr) 
    if lyr.layerName() in inset_layers:
        inset_visible.append(map_lyr)
        lyr.setVisible(2) 
    else: 
        lyr.setVisible(0) 

all_cities = registry.mapLayersByName("tiger_place_2016")[0] 
ids_all_cities = [f.id() for f in all_cities.getFeatures()]
canvas.zoomToFeatureIds(all_cities, ids_all_cities)
canvas.zoomScale(canvas.scale() * 1.2)
#map_settings = canvas.mapSettings()
map_settings.setLayers(inset_visible)
inset_map = composition.getComposerItemById("inset_map") 
inset_map.setMapCanvas(canvas)
inset_map.setNewExtent(canvas.extent())
#lock the inset layers style and extent to switch to larger map 
inset_map.setLayerSet(inset_visible)
inset_map.setKeepLayerSet(True) 
#inset_map.setKeepLayerStyles(True) 

#this section sets up the thematic layers for this section which zooms
#in on the specific project boundary     
visible_layers = []
for map_lyr in map_layers: 
    lyr = root.findLayer(map_lyr) 
    if lyr.layerName() in basemap + ["bldg_2014"]: 
        visible_layers.append(map_lyr)
        lyr.setVisible(2) 
    else: 
        lyr.setVisible(0)

boundary = registry.mapLayersByName("boundaries")[0] 
ids = [f.id() for f in boundary.getFeatures()]
canvas.zoomToFeatureIds(boundary, ids)
canvas.zoomScale(canvas.scale() * 1.2)
#map_settings = canvas.mapSettings()
map_settings.setLayers(visible_layers)
# composition = QgsComposition(map_settings) 
# composition.loadFromTemplate(document) 

map_item = composition.getComposerItemById('main_map') 
#canvas.zoomByFactor(.5) 
map_item.setMapCanvas(canvas) 
map_item.setNewExtent(canvas.extent())
composition.refreshItems() 
#reset inset map items for future changes
#map_item.setKeepLayerSet(False) 
#map_item.setKeepLayerStyles(False) 

#set up the image for export     
dpi = 300 
dpmm = dpi / 25.4 
width = int(dpmm * composition.paperWidth()) 
height = int(dpmm * composition.paperHeight()) 

image = QImage(QSize(width, height), QImage.Format_ARGB32) 
image.setDotsPerMeterX(dpmm * 1000) 
image.setDotsPerMeterY(dpmm * 1000) 
image.fill(0) 

imagePainter = QPainter(image) 
composition.renderPage(imagePainter, 0) 
imagePainter.end() 
image.save("project_output.jpg", "jpg") 

QgsProject.instance().clear() 
QgsApplication.exitQgis()     
