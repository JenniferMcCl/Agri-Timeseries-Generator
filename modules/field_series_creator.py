# --------------------------------------------------------------------------------------------------------------------------------
# Name:        field_series_creator
# Purpose:     The main class to generate a range of geo specific time series in given folders.
#              The data for the timeseries is derived from specific coverages of the Raster data manager Rasdaman.
#              These coverages are always in ESPG:25832
#              The timeseries can either be saved in dedicated folders or entered directly into a dedicated
#              sql database structured for holding geo-referenced agricultural information.
#              Geo-referencing is either by point or polygon.
#              Functionality interfacing with the database depends on the CreateAgriRefDatabase accessing a
#              PostGreSql database integrating Postgis and timescale DB and tables according to a specific structure.
#              Multiple logfiles and .csv table files are created to file and log processing and database access.
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
# --------------------------------------------------------------------------------------------------------------------------------

import time
import os
import csv
import json
import rasterio
import io

import numpy as np
from rasterio import float64
from rasterio.merge import merge
from rasterio.io import MemoryFile
from pyproj import CRS
from rasterio.mask import mask

import datetime
from datetime import datetime as td
from datetime import timedelta as dt


from modules.rasdaman_request import RasdamanRequest
from modules.datacube_S2 import DatacubeSatData
from modules.date_transformer import DateTransformer
from modules.file_utils import FileUtils
import modules.geo_position as geo
from modules.veg_specific_tiff_operations import VegSpecificTiffOperations

# CreateAgriDatabase repository must be accessible
from modules.access_sql import AccessSql
from modules.field_id_creator import FieldIdCreation


class FieldSeriesCreator:

    # The macro to indicate processing is done on points.
    FROM_POINT = False

    # The bounding box to create around the given point.
    POINT_BB = 2000

    # This is the relevant crop type in the geojsons applied for processing
    CROP_TYPE = "W-Weizen"

    # This is the name of the sql table to enter the derived data into
    SQL_TABLE_NAME = "field_day_regular_size"

    def __init__(self, start_date, end_date, ras_user, ras_passw, log_output):
        """
            :param start_date: The start date of the series
            :param end_date: The end date of the series
            :param ras_user: The Rasdaman username
            :param ras_passw: The Rasdaman user password
            :param log_output: Holds the open log file
        """

        self.start_date = start_date
        self.end_date = end_date
        self.ras_user = ras_user
        self.ras_passw = ras_passw
        self.output_folder = None
        self.current_field = None
        self.log_csv_name = None
        self.log_output = log_output
        self.file_handler_writer = None

    def create_dwd_files(self, folder_to_fields, output_folder, gdd_base):
        """
            This method creates dwd weather files including the GDD (Growing degree days) for all field geojsons
            and given GDD base in a given folder.
                :param folder_to_fields: The geojson of the field to derive weather data from
                :param output_folder: The folder to add the weather files to.
                :param gdd_base: The base for GDD calculation.
                :return: No return value.
        """

        self.output_folder = output_folder

        field_items = []
        if os.path.isdir(folder_to_fields):
            # Get list of field polygons in folder by name
            field_items = os.listdir(folder_to_fields)
            field_items.sort()

        elif ".geojson" in folder_to_fields:
            field_items = [os.path.basename(folder_to_fields)]
            folder_to_fields = os.path.dirname(folder_to_fields) + "/"

        for k in range(0, len(field_items)):
            field_name = field_items[k].replace(".geojson", "")
            file_name = output_folder + self.start_date + "_" + self.end_date + "_DWD_GDD_" + str(gdd_base) + "_" + field_name + ".csv"

            if os.path.exists(file_name):
                print("Item already created: " + file_name)
            else:
                self.create_dwd_field_series(folder_to_fields + field_items[k], file_name, gdd_base)

    def create_S_data_to_table(self, table_name, folder_to_fields, output_folder, crop_type):
        """
            This function queries through a list of field geojsons containing BBCH stages and Bdates for each stage in
            the geojson properties, derives Sentinel-2 and Sentinel-1 raster data from Rasdaman for each BDates date and
            enters the derived raster data into the specified sql database table. If there is no data for this specific date,
            one day before and one day after is required for.
            To use this functionality the CreateAgriRefDatabase repository modules AccessSql and FieldIdCreator must be accessible.
            :param table_name: The name of the sql table to enter the data in.
            :param folder_to_fields: Folder to geojsons containing polygons and properties including BDate and BBCH values.
            :param output_folder: The folder to enter the log files and csv tables.
            :return:
        """

        db_connector, db_cursor = AccessSql.create_db_connection()

        log_csv_name_s2 = output_folder + "s2_series_to_sql_from_bbch_geojsons"
        log_csv_name_s1 = output_folder + "s1_series_to_sql_from_bbch_geojsons"

        field_items = os.listdir(folder_to_fields)
        field_items.sort()

        for j in range(0, len(field_items)):
            self.current_field = field_items[j].replace(".geojson", "")
            cur_field = os.path.basename(self.current_field)

            days = []
            ndvi_raster_binary = rvi_raster_binary = ndvi_array = rvi_array = None

            hashed_field_id = FieldIdCreation.hash_from_geojson(folder_to_fields + field_items[j], crop_type)

            with open(folder_to_fields + field_items[j], 'r', encoding='utf-8') as geojson_file:
                geojson_data = json.load(geojson_file)

                poly = geo.transfer_geom(folder_to_fields + field_items[j], 25832, 25832)
                polygon = str(poly.wkt).replace(' (', '(')

                properties = geojson_data.get("properties", {})
                days = properties.get("BDate")
                bbch = properties.get("BBCH")

            with open(log_csv_name_s2 + ".csv", 'a') as write_Array1, open(log_csv_name_s1 + ".csv", 'a') as write_Array2:
                print("File for S2 optical ('codede_reflectanceXboaXs2gg_irregular' layer) Spatial Coverage Metrics created here: \n"
                      + log_csv_name_s2 + ".csv")

                time.sleep(3)

                print("Nan value is set to 0 for S2 optical data.")
                print("Raw optical data type with 10 bands is: int 32 with range [0,+]")
                print("NDVI optical data type with 1 band is: float64 normalised to range [0,1]")

                print("File for S1 Backscatter ('codede_gamma0XascXs1gg_irregular' and 'codede_gamma0XascXs1gg_irregular' layers) \n"
                      + "Spatial Coverage Metrics created here: " + log_csv_name_s1 + ".csv")

                print("Nan value is set to 0 for radar raster data.")
                print("Raw raster data type is: int32 with range [-,+]")
                print("RVI raster data type is: float64 normalised to range [0,1]")

                file_handler_writer1 = csv.writer(write_Array1)
                file_handler_writer1.writerow(["Multi Polygon Source", "Date",
                                               "Cnt Valid S2 Pixel", "Valid Percent", "Cnt All Pixel"])
                file_handler_writer1.writerow([cur_field, "", "", "", ""])

                self.file_handler_writer = csv.writer(write_Array2)
                self.file_handler_writer.writerow(["Polygon Name", "Date",
                                                   "Valid S1 Pixel", "Valid Percent", "All Pixel"])
                self.file_handler_writer.writerow([cur_field, "", "", "", ""])

                for k in range(0, len(days)):

                    # This is the current layer name of S2 optical data at JKI Rasdaman Server.
                    # In case of changes check https://sf.julius-kuehn.de/openapi/new_Coverage_names/
                    layer = 'codede_reflectanceXboaXs2gg_irregular'

                    ndvi_array, meta1 = self.get_ndvi_image_for_day(layer, days[k], polygon, file_handler_writer1, cur_field)

                    if ndvi_array is None:
                        new_date = FieldSeriesCreator.adjust_date(days[k], 1)
                        ndvi_array, meta1 = self.get_ndvi_image_for_day(layer, new_date, polygon, file_handler_writer1,
                                                                        cur_field)
                    if ndvi_array is None:
                        new_date = FieldSeriesCreator.adjust_date(days[k], -1)
                        ndvi_array, meta1 = self.get_ndvi_image_for_day(layer, new_date, polygon, file_handler_writer1,
                                                                        cur_field)

                    if ndvi_array is not None:
                        with MemoryFile() as memfile, memfile.open(**meta1) as dst:
                            dst.write(ndvi_array, 1)
                            ndvi_raster_binary = memfile.read()

                        AccessSql.update_partial_row(db_cursor, db_connector, table_name, ras_as_bin=True,
                                                     field_id=hashed_field_id, date=days[k],
                                                    bbch_phase = bbch[k], s2_data=ndvi_raster_binary)

                    layer1 = "codede_gamma0XascXs1gg_irregular"
                    layer2 = "codede_gamma0XdescXs1gg_irregular"

                    rvi_array, meta = self.get_s1_rvi_image_for_day(layer1, layer2, days[k], polygon, cur_field)

                    if rvi_array is None:
                        new_date = FieldSeriesCreator.adjust_date(days[k], 1)
                        rvi_array, meta = self.get_s1_rvi_image_for_day(layer1, layer2, new_date, polygon, cur_field)

                    if rvi_array is None:
                        new_date = FieldSeriesCreator.adjust_date(days[k], -1)
                        rvi_array, meta = self.get_s1_rvi_image_for_day(layer1, layer2, new_date, polygon, cur_field)

                    if rvi_array is not None:
                        with MemoryFile() as memfile, memfile.open(**meta) as dst:
                            dst.write(rvi_array, 1)
                            rvi_raster_binary = memfile.read()

                        AccessSql.update_partial_row(db_cursor, db_connector, table_name, ras_as_bin=True,
                                                     field_id=hashed_field_id, date=days[k],
                                                    bbch_phase=bbch[k], bsc_data=rvi_raster_binary)

    def create_S2_field_series(self, folder_to_fields, output_folder, log_csv_name, from_point=False, point_bb=0):
        """
            This method creates time series as raster geotiffs of either all geojson polygons in a folder,
            or a single geojson polygon. For each geojson a subfolder is created in the output folder
            which then contains the timeseries. The Sentinel 2 Rasdaman coverage is accessed here to derive the data.
                :param folder_to_fields: The folder to the geojson field boundaries or a path to one geojson.
                :param output_folder: the folder to create the series in.
                :param log_csv_name: The name of the log file to track the info of the series creation and output raster
                        geotiffs.
                :param from_point: Flag to set if only a point is given to derive the data from.
                :param point_bb: If only a point is given, this defines the size of the polygon created around the
                point to derive the data from.
                :return:
        """

        self.output_folder = output_folder

        with open(log_csv_name + ".csv""", 'a') as write_Array:

            print("File for S2 optical ('codede_reflectanceXboaXs2gg_irregular' layer) Spatial Coverage Metrics created here: \n"
                  + log_csv_name + ".csv")
            time.sleep(3)

            print("Nan value is set to 0 for S2 optical data.")
            print("Raw optical data type with 10 bands is: int 32 with range [0,+]")
            print("NDVI optical data type with 1 band is: float64 normalised to range [0,1]")

            self.file_handler_writer = csv.writer(write_Array)

            self.file_handler_writer.writerow(["Multi Polygon Source", "Date",
                                          "Cnt Valid S2 Pixel", "Valid Percent", "Cnt All Pixel"])

            if os.path.isdir(folder_to_fields):

                # Get list of field polygons in folder by name
                field_items = os.listdir(folder_to_fields)
                field_items.sort()

            elif ".geojson" in folder_to_fields:
                field_items = [os.path.basename(folder_to_fields)]
                folder_to_fields = os.path.dirname(folder_to_fields) + "/"

            # Create list of field polygon paths
            geojson_list = self.get_geojson_list(folder_to_fields, field_items)

            days = RasdamanRequest.get_dates_in_range(self.start_date, self.end_date)

            raw_data_folder = "raw/"

            os.makedirs(self.output_folder + raw_data_folder, exist_ok=True)

            # Set the permissions to 755 (read/write/execute for owner, read/execute for group and others)
            os.chmod(self.output_folder + raw_data_folder, 0o755)

            ndvi_data_folder = "ndvi_ras/"

            os.makedirs(self.output_folder + ndvi_data_folder, exist_ok=True)

            # Set the permissions to 755 (read/write/execute for owner, read/execute for group and others)
            os.chmod(self.output_folder + ndvi_data_folder, 0o755)

            for j in range(0, len(geojson_list)):

                self.current_field = field_items[j].replace(".geojson", "")
                cur_field = os.path.basename(self.current_field)
                self.file_handler_writer.writerow([cur_field, "", "", "", ""])

                # This is code only used when processes stall or are aborted because of Rasdaman Server migration
                #if os.path.exists(self.output_folder + raw_data_folder + self.current_field):
                #    print("Aborting field processing: " + self.current_field + " folder already created.")
                #    continue

                os.makedirs(self.output_folder + raw_data_folder + self.current_field, exist_ok=True)

                # Set the permissions to 755 (read/write/execute for owner, read/execute for group and others)
                os.chmod(self.output_folder + raw_data_folder + self.current_field, 0o755)

                os.makedirs(self.output_folder + ndvi_data_folder + self.current_field, exist_ok=True)

                # Set the permissions to 755 (read/write/execute for owner, read/execute for group and others)
                os.chmod(self.output_folder + ndvi_data_folder + self.current_field, 0o755)

                for k in range(0, len(days)):

                    poly = geo.transfer_geom(folder_to_fields + field_items[j], 25832, 25832)
                    if from_point:
                        poly = geo.create_bounding_box(poly, point_bb, point_bb)
                    elif isinstance(poly, Point):
                        print("FROM_POINT macro set to false. Can not run with Points.")
                        return

                    if not poly:
                        return

                    polygon = str(poly.wkt).replace(' (', '(')

                    # This is the current layer name of S2 optical data at JKI Rasdaman Server.
                    # In case of changes check https://sf.julius-kuehn.de/openapi/new_Coverage_names/
                    layer = 'codede_reflectanceXboaXs2gg_irregular'

                    timeBefore = datetime.datetime.now()

                    img = DatacubeSatData.get_Sat_imagery(
                        polygon=polygon,
                        layer= layer,
                        date=days[k],
                        user=self.ras_user,
                        pw=self.ras_passw,
                        host='https://datacube.julius-kuehn.de/flf/ows',
                        epsg=25832,
                        band_subset=False,
                        printout=True,
                        get_query=False
                    )

                    timeAfter = datetime.datetime.now()
                    timeForProcessing = timeAfter - timeBefore
                    timeOutput = "The time for rasdaman access is: %s micro sec" % (datetime.timedelta(
                        microseconds=timeForProcessing.microseconds).microseconds)

                    print("Requesting S2 optical date: " + str(days[k]) + " for field: " + field_items[j])
                    print(timeOutput)

                    self.log_output.appendProcTime(field_items[j], (datetime.timedelta(
                        microseconds=timeForProcessing.microseconds).microseconds))

                    if not img:
                        log_string = "S2 date: " + str(days[k]) + " for: \n" + cur_field + " has no valid Rasdaman return value! Skipping!"
                        print(log_string)
                        continue

                    log_string = "Processing S2 date: " + str(days[k]) + " for: \n" + cur_field
                    print(log_string)

                    try:
                        with rasterio.open(io.BytesIO(img.content), 'r', nodata=0) as src:

                            valid, val_pro, size, amount_pix = RasdamanRequest().check_valid_non_zero(src, 0)
                            if val_pro == 0.0:
                                print("S2 date: " + str(days[k]) + " for: \n" + cur_field + " has 0 array content.")
                                continue

                            self.file_handler_writer.writerow(["", days[k], amount_pix, val_pro, size])

                            field_name = field_items[j].replace(".geojson", "")
                            field_output_folder_raw = raw_data_folder + field_name + "/"

                            date = days[k].replace("-", "")
                            name_for_clip = date + "_S2_" + field_name

                            tiff_array = src.read()
                            self.save_geotiff(tiff_array, src.meta, field_output_folder_raw + name_for_clip)

                            # Calculate the NDVI of the S2 data and save in separate dedicated folder.
                            ndvi_array = VegSpecificTiffOperations.calculate_norm_ndvi(tiff_array)
                            field_output_folder_ndvi = ndvi_data_folder + field_name + "/"

                            meta = src.meta.copy()
                            meta.update({"count": 1,
                                         "dtype": float64})

                            self.save_geotiff(ndvi_array, meta, field_output_folder_ndvi + name_for_clip, 1)

                    except rasterio.errors.RasterioIOError as e:
                        log_string = "Error opening raster file:" + str(e)
                        print(log_string)
                        self.log_output.setError()

                        log_string = str(days[k]) + " could not be opened."
                        print(log_string)

    def create_S1_field_series(self, folder_to_fields, output_folder, log_csv_name, from_point=False, point_bb=0):
        """
            This method creates time series of raster geotiffs of either all geojson polygons in a folder,
            or a single geojson polygon. For each geojson a subfolder is created in the output folder which then
            contains the timeseries.
            The Sentinel 1 Rasdaman coverages for ascending and descending satellite orbits are accessed.
                :param folder_to_fields: The folder to the geojson field boundaries or a path to one geojson.
                :param output_folder: the folder to create the series in.
                :param log_csv_name: The name of the log file to track the info of the series creation and output raster
                        geotiffs.
                :param from:point: Flag to set if only a point is given to derive the data from.
                :param point_bb: If only a point is given, this defines the size of the polygon created around the
                point to derive the data from.
                :return:
        """

        self.output_folder = output_folder

        with open(log_csv_name + ".csv""", 'a') as write_Array:

            string = ("File for S1 Backscatter ('codede_gamma0XascXs1gg_irregular' and 'codede_gamma0XascXs1gg_irregular' layers) "
                      "Spatial Coverage Metrics created here: ") + log_csv_name + ".csv"

            print(string)
            print("Nan value is set to 0 for radar raster data.")
            print("Raw raster data type is: int32 with range [-,+]")
            print("RVI raster data type is: float64 normalised to range [0,1]")

            time.sleep(3)

            self.file_handler_writer = csv.writer(write_Array)
            self.file_handler_writer.writerow(["Polygon Name", "Date",
                                          "Valid S1 Pixel", "Valid Percent", "All Pixel"])

            if os.path.isdir(folder_to_fields):
                # Get list of field polygons in folder by name
                field_items = os.listdir(folder_to_fields)
                field_items.sort()

            elif ".geojson" in folder_to_fields:
                field_items = [os.path.basename(folder_to_fields)]
                folder_to_fields = os.path.dirname(folder_to_fields) + "/"

            # Create array of field polygon paths
            geojson_list = FieldSeriesCreator.get_geojson_list(folder_to_fields, field_items)

            days = RasdamanRequest.get_dates_in_range(self.start_date, self.end_date)

            # These paths must be kept identical in the "load_and_save_s1_cov_raster" method.
            raw_data_folder = self.output_folder + "raw"
            rvi_data_folder = self.output_folder + "rvi_ras"

            os.makedirs(raw_data_folder, exist_ok=True)
            # Set the permissions to 755 (read/write/execute for owner, read/execute for group and others)
            os.chmod(raw_data_folder, 0o755)

            os.makedirs(rvi_data_folder, exist_ok=True)
            # Set the permissions to 755 (read/write/execute for owner, read/execute for group and others)
            os.chmod(rvi_data_folder, 0o755)

            for j in range(0, len(geojson_list)):

                self.current_field = field_items[j].replace(".geojson", "")
                cur_field = os.path.basename(self.current_field)
                self.file_handler_writer.writerow([cur_field, "", "", "", ""])

                abs_path_raw = raw_data_folder + "/" + self.current_field

                # This is code only used when processes stall or are aborted because of Rasdaman Server migration
                if os.path.exists(abs_path_raw):
                    print("Aborting field processing: " + self.current_field + " folder already created.")
                    continue

                os.makedirs(abs_path_raw, exist_ok=True)
                # Set the permissions to 755 (read/write/execute for owner, read/execute for group and others)
                os.chmod(abs_path_raw,0o755)

                abs_path_rvi = rvi_data_folder + "/" + self.current_field

                os.makedirs(abs_path_rvi, exist_ok=True)
                # Set the permissions to 755 (read/write/execute for owner, read/execute for group and others)
                os.chmod(abs_path_rvi, 0o755)

                for k in range(0, len(days)):
                    # requesting alternative DataCube on Code-De S3 Bucket.
                    # ras_cde_host = 'https://code-de.rasdaman.com/rasdaman/ows'

                    poly = geo.transfer_geom(folder_to_fields + field_items[j], 25832, 25832)
                    if from_point:
                        poly = geo.create_bounding_box(poly, point_bb, point_bb)
                    elif isinstance(poly, Point):
                        print("FROM_POINT macro set to false. Can not run with Points.")
                        return

                    if not poly:
                        return

                    polygon = str(poly.wkt).replace(' (', '(')

                    layer = "codede_gamma0XascXs1gg_irregular"

                    self.load_and_save_s1_cov_raster(polygon, layer, days[k], "asc_")

                    layer = "codede_gamma0XdescXs1gg_irregular"

                    self.load_and_save_s1_cov_raster(polygon, layer, days[k], "desc_")

# -------------------Rasdaman specific helper methods------------------------------------------------ #

    def create_dwd_field_series(self, path_to_field, output_folder, gdd_base):
        """
            This is a helper method to create a dwd .csv file for a specific field.
            If the field is a polygon, the middle point in taken. See Readme for csv table structure.
                :param path_to_field: The geojson of the field to derive weather data from
                :param output_folder: The folder to add the weather files to.
                :param gdd_base: The base for GDD calculation.
                :return: No return value
        """

        self.output_folder = output_folder

        poly = geo.transfer_geom(path_to_field, 25832, 25832)

        centroid = geo.get_centroid_bounds_area(poly)[0]
        easting = centroid.x
        northing = centroid.y
        print("E value: " + str(easting) + " N value: " + str(northing))

        dates = DateTransformer.generate_date_range(self.start_date, self.end_date)

        timeBefore = datetime.datetime.now()

        rainfall = RasdamanRequest.get_coverage_subset(startdate=self.start_date, enddate=self.end_date,
                                                       rasdaman_layer='dwd_precipitation_daily',
                                                       easting=easting, northing=northing, user=self.ras_user,
                                                       passwd=self.ras_passw)

        temp_mean = RasdamanRequest.get_coverage_subset(startdate=self.start_date, enddate=self.end_date,
                                                        rasdaman_layer='dwd_temperatureXaverage_daily',
                                                        easting=easting, northing=northing, user=self.ras_user,
                                                        passwd=self.ras_passw, day_begin="T12:00:00.000Z")

        temp_min = RasdamanRequest.get_coverage_subset(startdate=self.start_date, enddate=self.end_date,
                                                       rasdaman_layer='dwd_temperatureXminimum_daily',
                                                       easting=easting, northing=northing, user=self.ras_user,
                                                       passwd=self.ras_passw)

        temp_max = RasdamanRequest.get_coverage_subset(startdate=self.start_date, enddate=self.end_date,
                                                       rasdaman_layer='dwd_temperatureXmaximum_daily',
                                                       easting=easting, northing=northing, user=self.ras_user,
                                                       passwd=self.ras_passw)

        precip_valid = np.array(rainfall != 0)
        temp_mean_valid = np.array(temp_mean != 0)

        timeAfter = datetime.datetime.now()
        timeForProcessing = timeAfter - timeBefore
        timeOutput = "The time for rasdaman access is: %s micro sec" % (datetime.timedelta(
            microseconds=timeForProcessing.microseconds).microseconds)

        print("Requesting DWD precipitation and mean temperature for range: " + str(self.start_date) + "/" + str(self.end_date)
              + " for pos: " + str(northing) + "/" + str(easting))

        print("Rasdaman Request valid: " + str(precip_valid) + "/" + str(temp_mean_valid))
        print(timeOutput)

        timeseries_rain = list(map(int, rainfall))
        timeseries_temp_mean = list(map(int, temp_mean))
        timeseries_temp_mean = [x / 10 for x in timeseries_temp_mean]

        timeseries_temp_min = list(map(int, temp_min))
        timeseries_temp_min = [x / 10 for x in timeseries_temp_min]

        timeseries_temp_max = list(map(int, temp_max))
        timeseries_temp_max = [x / 10 for x in timeseries_temp_max]

        timeseries_gdd = FieldSeriesCreator.calculate_gdd(timeseries_temp_min, timeseries_temp_max, float(gdd_base))

        # This is a workaround because the rasdaman coverage 'dwd_temperatureXaverage_daily' has a different starting time
        # than other coverages "T00:00:00.000Z" vs "T12:00:00.000Z"
        # Adapting the time to "T12:00:00.000Z" evokes skipping the first day.
        dates.pop(0)
        timeseries_temp_mean.pop(0)

        timeseries_dwd = FileUtils.create_date_value_pair_dict(dates, timeseries_rain, timeseries_temp_mean,
                                                               timeseries_temp_min, timeseries_temp_max, timeseries_gdd)

        FileUtils.write_dict_to_csv(timeseries_dwd, self.output_folder)

        time.sleep(3)

    def get_s1_rvi_image_for_day(self, layer1, layer2, day, polygon, geojson_name):
        """
            This derives the backscatter ascending and descending data from the given Rasdaman coverages
            layer1 and layer2 for the given day and polygon in ESPG:25832
            :param layer1: The ascending layer on rasdaman
            :param layer2: The descending layer on rasdaman
            :param day: The day to derive the data for
            :param polygon: The polygon to derive the data for
            :param geojson_name: The name of the polygon geojson, used for log information.
            :return: rvi_array, meta: A numpy array containg the derived data, the meta raster data
        """
        timeBefore = datetime.datetime.now()

        tiff_array1 = None
        tiff_array2 = None

        img1 = DatacubeSatData.get_Sat_imagery(
            polygon=polygon,
            layer=layer1,
            date=day,
            user=self.ras_user,
            pw=self.ras_passw,
            host='https://datacube.julius-kuehn.de/flf/ows',
            epsg=25832,
            band_subset=False,
            printout=True,
            get_query=False
        )

        img2 = DatacubeSatData.get_Sat_imagery(
            polygon=polygon,
            layer=layer2,
            date=day,
            user=self.ras_user,
            pw=self.ras_passw,
            host='https://datacube.julius-kuehn.de/flf/ows',
            epsg=25832,
            band_subset=False,
            printout=True,
            get_query=False
        )

        if img1:

            try:
                with rasterio.open(io.BytesIO(img1.content), 'r', nodata=0) as src1:

                    valid, val_pro, size, amount_pix = RasdamanRequest().check_valid_non_zero(src1, 0)
                    if val_pro == 0.0:
                        print("S1 backscatter date: " + str(day) + " asc for field: " + geojson_name + " has 0 array content.")
                        return None, None

                    tiff_array1 = src1.read()

                    self.file_handler_writer.writerow(["", day, amount_pix, val_pro, size])

            except rasterio.errors.RasterioIOError as e:
                log_string = "Error opening raster file:" + str(e)
                print(log_string)
                self.log_output.setError()

                log_string = str(day) + " could not be opened."
                print(log_string)
                return None, None

        if img2:

            try:
                with rasterio.open(io.BytesIO(img2.content), 'r', nodata=0) as src2:

                    valid, val_pro, size, amount_pix = RasdamanRequest().check_valid_non_zero(src2, 0)
                    if val_pro == 0.0:
                        print("S1 backscatter date: " + str(day) + " desc for field: " + geojson_name + " has 0 array content.")
                        return None, None

                    tiff_array2 = src2.read()

                    self.file_handler_writer.writerow(["", day, amount_pix, val_pro, size])

            except rasterio.errors.RasterioIOError as e:
                log_string = "Error opening raster file:" + str(e)
                print(log_string)
                self.log_output.setError()

                log_string = str(day) + " could not be opened."
                print(log_string)
                return None, None

            timeAfter = datetime.datetime.now()
            timeForProcessing = timeAfter - timeBefore
            timeOutput = "The time for rasdaman access is: %s micro sec" % (datetime.timedelta(
                microseconds=timeForProcessing.microseconds).microseconds)

            dir_field = dir + self.current_field

            print("Requesting S1 backscatter date: " + str(day) + " for field: " + dir_field)
            print(timeOutput)

            self.log_output.appendProcTime(dir_field, (datetime.timedelta(
                microseconds=timeForProcessing.microseconds).microseconds))

        if tiff_array1 and not tiff_array2:
            return tiff_array1, src1.meta.copy()

        if tiff_array2 and not tiff_array1:
            return tiff_array2, src2.meta.copy()

        if not (tiff_array1 or tiff_array2):
            return None, None

        merged_data, merged_transform = merge([tiff_array1, tiff_array2])

        # Update metadata for the merged raster
        meta = src1.meta.copy()

        # Calculate the RVI of the S1 data and save in separate dedicated folder.
        rvi_array = VegSpecificTiffOperations.calculate_norm_rvi(merged_data)

        meta.update({
            "count": 1,
            "dtype": float64,
            "height": merged_data.shape[1],
            "width": merged_data.shape[2],
            "transform": merged_transform
        })

        return rvi_array, meta

    def load_and_save_s1_cov_raster(self, polygon, layer, day, path):
        """
            This derives data from the given Rasterman layer for a specific polygon and day and saves the raster
            data as geotiff to the given absolute path
        :param polygon: The polygon to derive the data for.
        :param layer: The Radaman layer name.
        :param day: The day to derive the data for.
        :param path: The absolute path to save the data under.
        :return:
        """

        timeBefore = datetime.datetime.now()

        img = DatacubeSatData.get_Sat_imagery(
            polygon=polygon,
            layer=layer,
            date=day,
            user=self.ras_user,
            pw=self.ras_passw,
            host='https://datacube.julius-kuehn.de/flf/ows',
            epsg=25832,
            band_subset=False,
            printout=True,
            get_query=False
        )

        timeAfter = datetime.datetime.now()
        timeForProcessing = timeAfter - timeBefore
        timeOutput = "The time for rasdaman access is: %s micro sec" % (datetime.timedelta(
                        microseconds=timeForProcessing.microseconds).microseconds)

        dir_field = path + self.current_field

        print("Requesting S1 backscatter date: " + str(day) + " for field: " + dir_field)
        print(timeOutput)

        self.log_output.appendProcTime(dir_field, (datetime.timedelta(
                        microseconds=timeForProcessing.microseconds).microseconds))

        if not img:
            print("Skipping day, return response is None.")
            return

        try:
            with rasterio.open(io.BytesIO(img.content), 'r', nodata=0) as src:

                valid, val_pro, size, amount_pix = RasdamanRequest().check_valid_non_zero(src, 0)
                if val_pro == 0.0:
                    print("S1 backscatter date: " + str(day) + " for field: " + dir_field + " has 0 array content.")
                    return

                # Make sure the folder extensions are the same created in usage function
                field_output_folder_raw = "raw/" + self.current_field
                field_output_folder_rvi = "rvi_ras/" + self.current_field

                date = day.replace("-", "")
                name_for_clip = date + "_S1_" + dir_field

                tiff_array = src.read()

                self.save_geotiff(tiff_array, src.meta, field_output_folder_raw + "/" + name_for_clip)

                self.file_handler_writer.writerow(["", day, amount_pix, val_pro, size])

                # Calculate the RVI of the S1 data and save in separate dedicated folder.
                rvi_array = VegSpecificTiffOperations.calculate_norm_rvi(tiff_array)

                meta = src.meta.copy()

                meta.update({"count": 1,
                             "dtype": float64})

                self.save_geotiff(rvi_array, meta, field_output_folder_rvi + "/" + name_for_clip, 1)

        except rasterio.errors.RasterioIOError as e:
            log_string = "Error opening raster file:" + str(e)
            print(log_string)
            self.log_output.setError()

            log_string = str(day) + " could not be opened."
            print(log_string)

    def get_ndvi_image_for_day(self, layer, day, polygon, file_handler_writer, geojson_name):
        """
        This derives a geotiff image from the given rasdaman layer for a given day and aoi polygon.
        :param layer: The layer to derive the geotiff by.
        :param day: The day of interest.
        :param polygon: The area of interest.
        :param file_handler_writer: The file handler to write information of the derived data to csv table.
        :param geojson_name: The geojson name of the AOI polygon
        :return:
        """

        timeBefore = datetime.datetime.now()

        # ----------------------Try to get NDVI values for polygon from Rastaman coverage---------------------------

        img = DatacubeSatData.get_Sat_imagery(
            polygon=polygon,
            layer=layer,
            date=day,
            user=self.ras_user,
            pw=self.ras_passw,
            host='https://datacube.julius-kuehn.de/flf/ows',
            epsg=25832,
            band_subset=False,
            printout=True,
            get_query=False
        )

        timeAfter = datetime.datetime.now()
        timeForProcessing = timeAfter - timeBefore
        timeOutput = "The time for rasdaman access is: %s micro sec" % (datetime.timedelta(
            microseconds=timeForProcessing.microseconds).microseconds)

        print("Requesting S2 optical date: " + str(day) + " for field: " + geojson_name)
        print(timeOutput)

        self.log_output.appendProcTime(geojson_name, (datetime.timedelta(
            microseconds=timeForProcessing.microseconds).microseconds))

        if not img:
            log_string = "S2 date: " + str(day) + " for: \n" + geojson_name + " has no valid Rasdaman return value! Skipping!"
            print(log_string)
            return None, None

        log_string = "Processing S2 date: " + str(day) + " for: \n" + geojson_name
        print(log_string)

        try:
            with rasterio.open(io.BytesIO(img.content), 'r', nodata=0) as src:

                valid, val_pro, size, amount_pix = RasdamanRequest().check_valid_non_zero(src, 0)
                if val_pro == 0.0:
                    print("S2 date: " + str(day) + " for: \n" + geojson_name + " has 0 array content.")
                    return None, None

                file_handler_writer.writerow(["", day, amount_pix, val_pro, size])

                # Calculate the NDVI of the S2 data
                ndvi_array = VegSpecificTiffOperations.calculate_norm_ndvi(src.read())
                crs = CRS.from_epsg(25832)

                meta = src.meta.copy()
                meta.update({
                    "count": 1,
                    "dtype": float64,
                    "crs":crs
                })

                return ndvi_array, meta

        except rasterio.errors.RasterioIOError as e:
            log_string = "Error opening raster file:" + str(e)
            print(log_string)
            self.log_output.setError()

            log_string = str(day) + " could not be opened."
            print(log_string)
            return None, None

# -------------------Raster data helper methods------------------------------------------------ #

    def clip_to_aoi(self, opened_data_tiff, geojson_poly, name_for_clip):
        """
        This clips a geotiff to a given AOI polygon.
            :param opened_data_tiff: The geotiff src already opened
            :param geojson_poly: The polygon to clip to
            :param name_for_clip: The absolute path to save the clipped geotiff under
            :return:
        """

        # load the raster, mask it by the polygon and crop it
        try:
            field_clip_data_array, out_trans = mask(opened_data_tiff, [geojson_poly], crop=True)

            meta = opened_data_tiff.meta
            meta.update({"driver": "GTiff",
                         "height": field_clip_data_array.shape[1],
                         "width": field_clip_data_array.shape[2],
                         "transform": out_trans})

            self.save_geotiff(field_clip_data_array, opened_data_tiff.meta, name_for_clip)

        except Exception as e:
            print(str(e))
            self.log_output.setError()

    def save_geotiff(self, field_clip_data_array, meta, name_for_clip, dim=None):
        """
            This saves the values in a given array under the given path as a raster geotiff names as given
            applying the meta raster information. Arrays containing more than 90% of invalid values are not saved.
            The invalid value is 0.
            :param field_clip_data_array: The array to save
            :param The meta to apply:
            :param name_for_clip: The path including the name
            :param dim: The dimension of the raster data to be saved
        """

        # load the raster, mask it by the polygon and crop it
        try:
            meta.update(nodata=0)

            relative_zero_field, relative_nan_field = FieldSeriesCreator.get_amount_zero_and_nan(
                field_clip_data_array)

            path_to_clip = self.output_folder + name_for_clip + ".tif"

            if relative_zero_field > 0.9:
                print("Relative amount zero/nan > 0.9")

            print("Relative Nan values for: " + name_for_clip + " are " + str(relative_zero_field))

            if os.path.exists(path_to_clip):
                print("Item already created: " + path_to_clip)
                return False

            with rasterio.open(path_to_clip, 'w', **meta) as dest1:

                if dim == 1:
                    dest1.write(field_clip_data_array, 1)
                else:
                    for i in range(field_clip_data_array.shape[0]):
                        dest1.write(field_clip_data_array[i], i + 1)

                print("Created item: " + path_to_clip)

        except Exception as e:
            print(str(e))
            self.log_output.setError()

# --------------------Static helper methods---------------------------------------- #

    @staticmethod
    def calculate_gdd(temp_min_list, temp_max_list, base):
        """This function calculates the GDD from 2 lists and returns an accumulated list
            :param temp_min_list The list of min temp values
            :param temp_max_list The list of max temp valuse
            :param base The base to use for GDD calculation
            :return a list of GDD in reference to the first date of te temp values
        """

        gdd_list = []

        # Iterate over the minimum and maximum temperature lists
        for temp_min, temp_max in zip(temp_min_list, temp_max_list):
            # Calculate the average temperature
            a = (temp_min + temp_max) / 2

            # Apply the condition to get the value of b
            if a < base:
                b = 0
            else:
                b = a

            # Calculate the gdd
            if b >= base:
                gdd = b - base
            else:
                gdd = b

            # Append the gdd to the gdd_list
            gdd_list.append(gdd)

        # Initialize the list to store the accumulated values
        accumulated_list = []
        total = 0

        # Accumulate the gdd values
        for gdd in gdd_list:
            total += gdd
            accumulated_list.append(round(total, 2))

        # Return the accumulated list
        return accumulated_list

    @staticmethod
    def get_geojson_list(folder_to_fields, field_items):
        """
            Creates a list of polygons from a given folder and list of geojsons in the folder.
            :param folder_to_fields: The folder containing the geojsons.
            :param field_items: The list of geojsons in the folder.
        """

        # Get list of field polygons path including folder name
        field_items_folder = [folder_to_fields + i for i in field_items]

        # Create array of field polygon paths
        geojson_list = []
        for j in range(0, len(field_items_folder)):
            with open(field_items_folder[j]) as data:

                if not field_items_folder[j].endswith(".geojson"):
                    print(field_items_folder[j] + " is not a geojson")
                    continue

                # Retrieve the polygon
                geoms = json.load(data)

                geojson_list.append(geoms)

        return geojson_list

    @staticmethod
    def get_amount_zero_and_nan(tiff_array, nan_value=6.9055e-41):
        """
            Returns the percentage of values in the given array that are the given nan value.
            :param tiff_array: The given array.
            :param nan_value: The given nan value.
            :return: relative_zero, relative_nan: percentage of zero values, percentage of nan values
        """

        sizex, sizey = FieldSeriesCreator.get_size(tiff_array)

        amount_zero = (tiff_array[0] == 0).sum()
        relative_zero = amount_zero / (sizey * sizex)

        amount_nan = (tiff_array[0] == nan_value).sum() + np.isnan(tiff_array[0]).sum()

        relative_nan = amount_nan / (sizey * sizex)

        return relative_zero, relative_nan

    @staticmethod
    def get_size(np_array):
        """
            This gets the size of the given array in x and y direction
            :param np_array: The numpy array to check
            :return size_x, size_y
        """
        amount_dim = np_array.ndim

        heightArr = np_array.shape[0]
        widthArr = np_array.shape[1]

        #TODO: Check this for different data
        sizey = heightArr if amount_dim == 2 else np_array.shape[1]
        sizex = widthArr if amount_dim == 2 else np_array.shape[2]
        return sizex, sizey

    @staticmethod
    def adjust_date(date_str, adjustment):
        """
        Adjusts a date by adding or subtracting days.

        Parameters:
            date_str (str): The original date in "YYYY-MM-DD" format.
            adjustment (int): The number of days to adjust (positive or negative).

        Returns:
            str: The adjusted date in "YYYY-MM-DD" format.
        """
        # Parse the input date string
        date_obj = td.strptime(date_str, "%Y-%m-%d")
        # Adjust the date
        new_date = date_obj + dt(days=adjustment)
        # Return the new date as a string
        return new_date.strftime("%Y-%m-%d")

