# --------------------------------------------------------------------------------------------------------------------------------
# Name:        timeseries_generator.py
# Purpose:     This is the main executable python file for creating timeseries.
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
# --------------------------------------------------------------------------------------------------------------------------------

import os
import sys

from modules.gui_creation import GuiCreation
from modules.field_series_creator import FieldSeriesCreator
from modules.log_output import LogOutput
from modules.create_user_setting_ts import CreateUserSettingTs


def execute_time_series(sentinel1, sentinel2, weather,
                        start_date,
                        end_date,
                        geojson,
                        output_s1,
                        output_s2,
                        output_dwd_log,
                        ras_user,
                        ras_passw,
                        log_output,
                        gdd_base,
                        fill_table
                        ):
    """
        This is the main execution function. Either activated by main or by the gui in gui mode.
        Lots of processing information is logged and added to dedicated files.
        If the class macros from the FieldSeriesCreator are set properly, the processing can work on points and a given
        bounding box size instead of polygons.
    :param sentinel1: Flag to signal derivation of sentinel-1 radar data.
    :param sentinel2: Flag to signal derivation of sentinel-2 optical data.
    :param weather: Flag to signal derivation of DWD weather data and GDDs.
    :param start_date: The starting date to derive the data for.
    :param end_date: The end date to derive the data for.
    :param geojson: The geojson containing the polygon to derive the data for.
    :param output_s1: The user defined output folder for the sentinel-1 data.
    :param output_s2: The user defined folder for the sentinel-2 data.
    :param output_dwd_log: The user defined folder to hold the log files and dwd csv table files.
    :param ras_user: A user password to access Rasdaman.
    :param ras_passw: A user password to access Rasdaman
    :param log_output: The log output file holder to add the log information
    :param gdd_base: The base the GDDs are calculated by.
    :param fill_table: A flag to indicate if the data should be directly added to the sql table.
    :return:
    """

    print("Execution function running.")

    # This is the case to derive and enter the data directly in the sql table.
    # This only works on a given folder containing geojson polygons.
    if fill_table and not start_date and not end_date and os.path.isdir(geojson):
        field_items = os.listdir(geojson)
        field_items.sort()

        field_series_creator = FieldSeriesCreator(start_date, end_date, ras_user, ras_passw, log_output)
        field_series_creator.create_S_data_to_table(FieldSeriesCreator.SQL_TABLE_NAME, geojson, output_dwd_log,
                                                    FieldSeriesCreator.CROP_TYPE)

    # This is the regular case where processing is performed for each data type according to the flag values.
    else:
        geojson_name = os.path.basename(geojson).replace(".geojson", "")
        field_series_creator = FieldSeriesCreator(start_date, end_date, ras_user, ras_passw, log_output)

        if sentinel2:
            log_pixel_count = "s2_series_"+ ((geojson_name + "_") if geojson_name else "") + str(start_date) + "_" + str(end_date)
            field_series_creator.create_S2_field_series(geojson, output_s2, output_dwd_log + log_pixel_count,
                                                        FieldSeriesCreator.FROM_POINT, FieldSeriesCreator.POINT_BB)
        if sentinel1:
            log_pixel_count = "s1_series_" + ((geojson_name + "_") if geojson_name else "") + str(start_date) + "_" + str(end_date)
            field_series_creator.create_S1_field_series(geojson, output_s1, output_dwd_log + log_pixel_count,
                                                        FieldSeriesCreator.FROM_POINT, FieldSeriesCreator.POINT_BB)
        if weather:
            field_series_creator.create_dwd_files(geojson, output_dwd_log, gdd_base)

    log_output.closeCurrentFiles()
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__


def main():
    args = sys.argv[1:]

    if len(args) == 0:
        GuiCreation(execute_time_series, True).runGui()

    fill_table = False

    # Activation in headless mode without gui
    if len(args) > 0 and args[0] == "--noGui":
        gui_execution = GuiCreation(execute_time_series, False)

        # Fill table mode. Data is entered directly in sql table.
        if len(args) == 2 and args[1] == "--fillTbl":
            fill_table = True

        userSettings = CreateUserSettingTs()
        log_output = LogOutput(userSettings)
        log_output.createLogOutputFile("")

        log_file_path = userSettings.weather_folder + "log_output/"
        print("Log file created at: " + log_file_path)
        print("Rasdaman benchmarking at: " + log_file_path)

        gui_execution.check_settings()
        execute_time_series(gui_execution.sentinel1_active,
                            gui_execution.sentinel2_active,
                            gui_execution.weather_active,
                            gui_execution.userSettings.start_date,
                            gui_execution.userSettings.end_date,
                            gui_execution.userSettings.aoi_geojson,
                            gui_execution.userSettings.s1_folder,
                            gui_execution.userSettings.s2_folder,
                            gui_execution.userSettings.weather_folder,
                            gui_execution.userSettings.rasdaman_user,
                            gui_execution.userSettings.rasdaman_passw,
                            log_output,
                            0,
                            fill_table)


if __name__ == "__main__":
    main()
