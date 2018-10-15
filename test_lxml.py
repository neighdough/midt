from lxml import etree
from odf.opendocument import load
from odf import (text, draw, table)
import zipfile
import os
import shutil


os.chdir("/home/nate/dropbox-caeser/Data/MIDT/Data_Warehouse/reports/neighborhood/")
nbhd = "Orange Mound"
dir_name = nbhd.replace(" ", "_")
report_name = dir_name + "_report.odt"

if not os.path.exists(dir_name):
    os.mkdir(dir_name)
os.mkdir(dir_name+"/Pictures")
shutil.copytree("REPORT_TEMPLATE/maps", dir_name+"/maps")
shutil.copy("REPORT_TEMPLATE/report_template.odt", os.path.join(dir_name, report_name))

os.chdir(dir_name)
z_in = zipfile.ZipFile(dir_name+"_report.odt")
z_out = zipfile.ZipFile(dir_name+"_report_new.odt", "w")

xml_content = z_in.read("content.xml")
xml_manifest = z_in.read("META-INF/manifest.xml") 

root_content = etree.fromstring(xml_content)
root_manifest = etree.fromstring(xml_manifest)

ns = {"draw": "urn:oasis:names:tc:opendocument:xmlns:drawing:1.0",
      "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
      "style": "urn:oasis:names:tc:opendocument:xmlns:style:1.0",
      "table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
      "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
      "manifest": "urn:oasis:names:tc:opendocument:xmlns:manifest:1.0",
      "xlink": "http://www.w3.org/1999/xlink",
      "re:": "http://exslt.org/regular-expressions"
      }

#Update picture names
img_path= ("//draw:frame/draw:image[re:match(@xlink:href,'Pictures\/[0-9A-z]+.jpg')]")

#Report side bar title for each page
title_xpath = ("//draw:frame[@draw:name='title_{}']/"
                "draw:text-box/text:p/text:span")

title = root_content.xpath(title_xpath.format("main"), namespaces=ns)

#Paragraph on Ownership Profile
txt = root_content.xpath("//draw:frame[@draw:name='par_own']/draw:text-box/text:p/text:span/text()",
        namespaces=ns)

#Ownership table in Ownership Profile
own_table = root_content.xpath("//table:table[@table:name='tbl_own']/table:table-row", 
                                namespaces=ns)
for row in own_table:
    row.xpath(".//text:p/text()", namespaces=ns)

#Demographics table in Neighborhood Profile
dem_table = root_content.xpath("//table:table[@table:name='tbl_demographic']/table:table-row", 
                                namespaces=ns)
for row in dem_table:
    row.xpath(".//text:p/text()", namespaces=ns)

#Create new document
for f in z_in.filelist:
    if f.filename not in ["content.xml", "META-INF/manifest.xml"]:
        z_out.writestr(f.filename, z_in.read(f.filename))

z_out.writestr("content.xml", etree.tostring(root_content))
z_out.writestr("META-INF/manifest.xml", etree.tostring(root_manifest))
z_out.close()
