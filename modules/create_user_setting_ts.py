# --------------------------------------------------------------------------------------------------------------------------------
# Name:        create_user_setting_ts
# Purpose:     This class creates a file outside of the repository folder to define all user specific paths necessary for processing
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
# --------------------------------------------------------------------------------------------------------------------------------


import os
from os.path import exists
from lxml import etree


class CreateUserSettingTs:

    user_folder = ""
    start_date = ""
    end_date = ""
    aoi_wkt = ""
    aoi_geojson = ""
    s1_folder = ""
    s2_folder = ""
    weather_folder = ""
    rasdaman_user = ""
    rasdaman_passw = ""
    processing_seq = ""
    gdd_base = ""

    def __init__(self):

        # Get the repository folder path
        workingPath = os.getcwd()
        #self.user_folder = workingPath.split("/Agri-Timeseries-Generator")

        # Use main directory for distribution app
        self.user_folder = [workingPath]

        # Create user setting file if not available
        file_exists = exists(self.user_folder[0] + "/user_settings_ts.xml")

        if not file_exists:

            root = etree.Element("root")
            paths = etree.SubElement(root, "paths")
            etree.SubElement(paths, "start_date", name="start date").text = ""
            etree.SubElement(paths, "end_date", name="end date").text = ""
            etree.SubElement(paths, "aoi_wkt", name="aoi wkt").text = ""
            etree.SubElement(paths, "aoi_geojson", name="aoi geojson").text = ""
            etree.SubElement(paths, "s1_folder", name="folder for s1 series").text = ""
            etree.SubElement(paths, "s2_folder", name="folder for s2 series").text = ""
            etree.SubElement(paths, "weather_folder", name="folder for weather data").text = ""
            etree.SubElement(paths, "rasdaman_user", name="rasdaman user name").text = ""
            etree.SubElement(paths, "rasdaman_passw", name="rasdaman user password").text = ""
            etree.SubElement(paths, "pros_seq", name="processing sequence").text = ""
            etree.SubElement(paths, "gdd_base", name="gdd base").text = ""

            etree.ElementTree(root)
            prettyString = etree.tostring(root, pretty_print=True, encoding='unicode')

            with open(self.user_folder[0] + "/user_settings_ts.xml", "w") as f:
                f.write(prettyString)

            print("User setting file has been created at: " + self.user_folder[0] + "/user_settings_ts.xml")
            print("User folder paths and rasdaman info must now be set in user_settings_ts.xml file")
        else:
            print("User setting file available at: " + self.user_folder[0] + "/user_settings_ts.xml")
            tree = etree.parse(self.user_folder[0] + '/user_settings_ts.xml')
            root = tree.getroot()

            paths = root.find("paths")

        if len(root.find("paths")) == 11:
            self.start_date = root[0][0].text
            self.end_date = root[0][1].text
            self.aoi_wkt = root[0][2].text
            self.aoi_geojson = root[0][3].text
            self.s1_folder = root[0][4].text
            self.s2_folder = root[0][5].text
            self.weather_folder = root[0][6].text
            self.rasdaman_user = root[0][7].text
            self.rasdaman_passw = root[0][8].text
            self.processing_seq = root[0][9].text
            self.gdd_base = root[0][10].text
        else:
            print("user_settings_ts.xml file Error")

    def setAttribute(self, attribute, value):
        tree = etree.parse(self.user_folder[0] + '/user_settings_ts.xml')
        root = tree.getroot()
        read = root.xpath("//" + attribute)
        read[0].text = value
        prettyString = etree.tostring(root, pretty_print=True, encoding='unicode')
        with open(self.user_folder[0] + '/user_settings_ts.xml', "w") as f:
            f.write(prettyString)
