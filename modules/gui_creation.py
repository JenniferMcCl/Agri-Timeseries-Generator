# --------------------------------------------------------------------------------------------------------------------------------
# Name:        gui_creation
# Purpose:     This creates the KTinker window to enter the necessary paths and information for the desired time series
#              calculation and start the processing.
#              The entered information is saved to the automatically pre-created user_settings_ts.xml file.
#              All processing information is redirected to the logfiles pre-created in the given output path.
#              See Readme for details.
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
# --------------------------------------------------------------------------------------------------------------------------------


from tkinter import *
import threading, sys, queue

from modules.geo_position import *
from modules.create_user_setting_ts import CreateUserSettingTs
from modules.redirect_text import RedirectText
from modules.log_output import LogOutput


class GuiCreation:
    master = None
    entryStartDate = None
    entryEndDate = None
    entryWkt = None
    entryGeoJson = None
    outputS1 = None
    outputS2 = None
    outputDwdLog = None
    rasdamanUser = None
    rasdamanPassword = None
    processingSequence = None
    displayLabelVar = None
    displayLabelVar2 = None
    gddbase = None
    executionfunction = None
    queue = None

    sentinel1_active = False
    sentinel2_active = False
    weather_active = False
    thread_running = False

    def __init__(self, execution_function, redirect):
        self.userSettings = CreateUserSettingTs()
        self.executionfunction = execution_function
        self.log_output = LogOutput(self.userSettings)

        log_file_path = self.log_output.logfile_dir + "log_output/"
        print("Log file created at: " + self.log_output.logfile_dir)
        print("Rasdaman benchmarking at: " + self.log_output.logfile_dir)

        self.processingThread = threading.Thread(target=self.executionfunction,
                                              args=(self.sentinel1_active,
                                                    self.sentinel2_active,
                                                    self.weather_active,
                                                    self.userSettings.start_date,
                                                    self.userSettings.end_date,
                                                    self.userSettings.aoi_geojson,
                                                    self.userSettings.s1_folder,
                                                    self.userSettings.s2_folder,
                                                    self.userSettings.weather_folder,
                                                    self.userSettings.rasdaman_user,
                                                    self.userSettings.rasdaman_passw,
                                                    self.log_output,
                                                    self.userSettings.gdd_base,
                                                    False))

        if redirect:
            self.queue = queue.Queue()

            # Redirect stdout and stderr
            sys.stdout = RedirectText(self.queue, self.log_output)
            sys.stderr = RedirectText(self.queue, self.log_output)

    def start_thread(self):
        self.processingThread = threading.Thread(target=self.executionfunction,
                                                 args=(self.sentinel1_active,
                                                       self.sentinel2_active,
                                                       self.weather_active,
                                                       self.userSettings.start_date,
                                                       self.userSettings.end_date,
                                                       self.userSettings.aoi_geojson,
                                                       self.userSettings.s1_folder,
                                                       self.userSettings.s2_folder,
                                                       self.userSettings.weather_folder,
                                                       self.userSettings.rasdaman_user,
                                                       self.userSettings.rasdaman_passw,
                                                       self.log_output,
                                                       self.userSettings.gdd_base,
                                                       False))
        self.processingThread.start()

    def process_queue(self):
        try:
            while True:
                line = self.queue.get_nowait()
                content = self.displayLabelVar2.get()
                length_con = len(content + line)
                self.displayLabelVar2.set(content + line if length_con < 250 else line)
        except queue.Empty:
            pass
        self.master.after(100, self.process_queue)  # Check the queue every 100ms

    def restart_thread(self):
        if self.processingThread and self.processingThread.is_alive():
            self.displayLabelVar.set("Process is still running, can't restart yet.")
            print("Process is still running, can't restart yet.")
            return

        self.log_output = LogOutput(self.userSettings)
        self.log_output.createLogOutputFile("")

        # Redirect stdout and stderr
        sys.stdout = RedirectText(self.queue, self.log_output)
        sys.stderr = RedirectText(self.queue, self.log_output)

        error_return = self.checkInput()

        if error_return:
            self.displayLabelVar.set(error_return)
            print(error_return)
        elif self.userSettings.processing_seq == "DWD Weather" or self.userSettings.processing_seq == "DWD Weather":
            self.start_thread()
            display_txt = ("Creating timeseries: " + str(self.processingSequence.get()) +
                           " including GDD = (DWD Tmax + DWD Tmin) / 2 - " + self.userSettings.gdd_base)

            self.displayLabelVar.set(display_txt)
            print(display_txt)
        else:
            self.start_thread()
            self.displayLabelVar.set("Creating timeseries: " + str(self.processingSequence.get()))
            print("Creating timeseries: " + str(self.processingSequence.get()))

    def runGui(self):
        self.makeGui()
        mainloop()

    def makeGui(self):
        # Create the main window and set its title and size
        self.master = Tk()
        self.master.title("Timeseries Creator")  # Set window title
        self.master.geometry("1000x750")  # Increase window size for better readability
        self.master.grid_columnconfigure(0, weight=1)
        self.master.grid_columnconfigure(1, weight=4)

        # Use a larger font for better visual appearance
        larger_font = ("Arial", 18)

        # Title label
        Label(self.master, text="----------------------Enter All Attributes for Processing!!!----------------------", font=("Arial", 16, "bold")).grid(row=0,
                                                                                                        column=1,
                                                                                                        sticky='w',
                                                                                                        padx=10,
                                                                                                        pady=10)

        # Input labels and fields
        Label(self.master, text="Start Date (YYYY-MM-DD)", font=larger_font).grid(row=1, column=0, sticky='w',
                                                                                  padx=10, pady=10)
        self.entryStartDate = Entry(self.master, width=30, font=larger_font)
        self.entryStartDate.grid(row=1, column=1, sticky='w', padx=10, pady=10)

        Label(self.master, text="End Date (YYYY-MM-DD)", font=larger_font).grid(row=2, column=0, sticky='w',
                                                                                padx=10, pady=10)
        self.entryEndDate = Entry(self.master, width=30, font=larger_font)
        self.entryEndDate.grid(row=2, column=1, sticky='w', padx=10, pady=10)

        Label(self.master, text="AOI as Wkt", font=larger_font).grid(row=3, column=0, sticky='w', padx=10, pady=10)
        self.entryWkt = Entry(self.master, font=larger_font)
        self.entryWkt.grid(row=3, column=1, sticky='ew', padx=10, pady=10)

        Label(self.master, text="AOI GeoJson File/Folder", font=larger_font).grid(row=4, column=0, sticky='w',
                                                                                  padx=10, pady=10)
        self.entryGeoJson = Entry(self.master, font=larger_font)
        self.entryGeoJson.grid(row=4, column=1, sticky='ew', padx=10, pady=10)

        Label(self.master, text="Output Folder path S1", font=larger_font).grid(row=5, column=0, sticky='w',
                                                                                padx=10, pady=10)
        self.outputS1 = Entry(self.master, font=larger_font)
        self.outputS1.grid(row=5, column=1, sticky='ew', padx=10, pady=10)

        Label(self.master, text="Output Folder path S2", font=larger_font).grid(row=6, column=0, sticky='w',
                                                                                padx=10, pady=10)
        self.outputS2 = Entry(self.master, font=larger_font)
        self.outputS2.grid(row=6, column=1, sticky='ew', padx=10, pady=10)

        Label(self.master, text="Output Folder Weather/Log", font=larger_font).grid(row=7, column=0, sticky='w',
                                                                                    padx=10, pady=10)
        self.outputDwdLog = Entry(self.master, font=larger_font)
        self.outputDwdLog.grid(row=7, column=1, sticky='ew', padx=10, pady=10)

        Label(self.master, text="Rasdaman Username", font=larger_font).grid(row=8, column=0, sticky='w', padx=10,
                                                                            pady=10)
        self.rasdamanUser = Entry(self.master, width=30, font=larger_font)
        self.rasdamanUser.grid(row=8, column=1, sticky='w', padx=10, pady=10)

        Label(self.master, text="Rasdaman Password", font=larger_font).grid(row=9, column=0, sticky='w', padx=10,
                                                                            pady=10)
        self.rasdamanPassword = Entry(self.master, width=30, font=larger_font, show="*")  # Hide password input
        self.rasdamanPassword.grid(row=9, column=1, sticky='w', padx=10, pady=10)

        Label(self.master, text="Processing Sequence", font=larger_font).grid(row=10, column=0, sticky='w', padx=10,
                                                                              pady=10)

        data = ['Sentinel-1 Backscatter', 'Sentinel-2 Optical', 'Sentinel 1/2', 'DWD Weather', 'All']
        self.processingSequence = StringVar(self.master)
        processingSequence = Spinbox(self.master, values=data, textvariable=self.processingSequence, width=60,
                                     font=larger_font)
        self.processingSequence.set('All')
        processingSequence.grid(row=10, column=1, sticky='w', padx=10, pady=10)

        Label(self.master, text="GDD Days Base", font=larger_font).grid(row=11, column=0, sticky='w', padx=10,
                                                                        pady=10)

        self.gddbase = Entry(self.master, width=10, font=larger_font)
        self.gddbase.grid(row=11, column=1, sticky='w', padx=10, pady=10)

        # Execute button
        Button(self.master, text='Execute Selection', font=larger_font, command=self.restart_thread).grid(row=12,
                                                                                                          column=0,
                                                                                                          sticky='w',
                                                                                                          padx=10,
                                                                                                          pady=20)

        # Display output area with a larger, styled label
        self.displayLabelVar = StringVar(self.master)
        Label(self.master, textvariable=self.displayLabelVar, bg='yellow', fg='red', height=6,
              font=larger_font).grid(row=12, column=1, sticky='ew', padx=10, pady=10)

        # Log output area
        self.displayLabelVar2 = StringVar(self.master)
        Label(self.master, textvariable=self.displayLabelVar2, bg='black', fg='white', height=12,
              anchor='nw', padx=10, pady=10, font=("Arial", 16)).grid(row=25, column=0, sticky='ew', columnspan=10, rowspan=10,
                                                                      padx=10, pady=10)

        # Additional methods to load settings or process data
        self.process_queue()
        self.loadSettings()

    def loadSettings(self):

        if (self.userSettings.start_date != None and self.userSettings.start_date != ""):
            self.entryStartDate.insert(0, self.userSettings.start_date)

        if (self.userSettings.end_date != None and self.userSettings.end_date != ""):
            self.entryEndDate.insert(0, self.userSettings.end_date)

        if (self.userSettings.aoi_geojson != None and self.userSettings.aoi_geojson != ""):
            self.entryGeoJson.insert(0, self.userSettings.aoi_geojson)

        if (self.userSettings.s1_folder != None and self.userSettings.s1_folder != ""):
            self.outputS1.insert(0, self.userSettings.s1_folder)

        if (self.userSettings.s2_folder != None and self.userSettings.s2_folder != ""):
            self.outputS2.insert(0, self.userSettings.s2_folder)

        if (self.userSettings.weather_folder != None and self.userSettings.weather_folder != ""):
            self.outputDwdLog.insert(0, self.userSettings.weather_folder)

        if (self.userSettings.rasdaman_user != None and self.userSettings.rasdaman_user != ""):
            self.rasdamanUser.insert(0, self.userSettings.rasdaman_user)

        if (self.userSettings.rasdaman_passw != None and self.userSettings.rasdaman_passw != ""):
            self.rasdamanPassword.insert(0, self.userSettings.rasdaman_passw)

        if (self.userSettings.processing_seq != None and self.userSettings.processing_seq != ""):
            self.processingSequence.set(self.userSettings.processing_seq)

        if (self.userSettings.gdd_base != None and self.userSettings.gdd_base != ""):
            self.gddbase.insert(0, self.userSettings.gdd_base)

    def checkInput(self):
        entriesComplete = ""

        self.userSettings.start_date = self.entryStartDate.get()
        self.userSettings.end_date = self.entryEndDate.get()
        self.userSettings.aoi_geojson = self.entryGeoJson.get()
        self.userSettings.s1_folder = self.outputS1.get()
        self.userSettings.s2_folder = self.outputS2.get()
        self.userSettings.weather_folder = self.outputDwdLog.get()
        self.userSettings.rasdaman_user = self.rasdamanUser.get()
        self.userSettings.rasdaman_passw = self.rasdamanPassword.get()
        self.userSettings.processing_seq = self.processingSequence.get()
        self.userSettings.gdd_base = self.gddbase.get()

        self.userSettings.setAttribute("start_date", self.userSettings.start_date)
        self.userSettings.setAttribute("end_date", self.userSettings.end_date)
        self.userSettings.setAttribute("aoi_geojson", self.userSettings.aoi_geojson)
        self.userSettings.setAttribute("s1_folder", self.userSettings.s1_folder)
        self.userSettings.setAttribute("s2_folder", self.userSettings.s2_folder)
        self.userSettings.setAttribute("weather_folder", self.userSettings.weather_folder)
        self.userSettings.setAttribute("rasdaman_user", self.userSettings.rasdaman_user)
        self.userSettings.setAttribute("rasdaman_passw", self.userSettings.rasdaman_passw)
        self.userSettings.setAttribute("pros_seq", self.userSettings.processing_seq)
        self.userSettings.setAttribute("gdd_base", self.userSettings.gdd_base)

        entryWkt = self.entryWkt.get()

        if (self.userSettings.aoi_geojson != "" and  entryWkt != ""):
            self.displayLabelVar.set("Enter only one type of Aoi. Either wkt or geojson")

        elif (entryWkt != "" and self.userSettings.aoi_geojson == ""):
            geoJson = wkt_to_geojson(entryWkt)
            with open(self.userSettings.user_folder + '/aoi.geojson', 'w') as f:
                f.writelines(str(geoJson))
                f.close()

            self.displayLabelVar.set("Geojson: " + self.userSettings.user_folder + "\n/aoi.geojson" + " saved.")
            self.userSettings.setAttribute("aoiLocation", self.userSettings.user_folder + '/aoi.geojson')
            self.userSettings.aoi_geojson = self.userSettings.user_folder + '/aoi.geojson'
            self.entryGeoJson.insert(0, self.userSettings.user_folder + '/aoi.geojson')

            return ""

        return self.check_settings()

    def check_settings(self):

        if self.userSettings.processing_seq == 'Sentinel-1 Backscatter' or self.userSettings.processing_seq == 'Sentinel 1/2' or self.userSettings.processing_seq == "All":
            self.sentinel1_active = True
        if self.userSettings.processing_seq == 'Sentinel-2 Optical' or self.userSettings.processing_seq == 'Sentinel 1/2' or self.userSettings.processing_seq == "All":
            self.sentinel2_active = True
        if self.userSettings.processing_seq == 'DWD Weather' or self.userSettings.processing_seq == "All":
            self.weather_active = True

        if self.userSettings.processing_seq == 'Sentinel-1 Backscatter':
            self.sentinel2_active = self.weather_active = False
        if self.userSettings.processing_seq == 'Sentinel-2 Optical':
            self.sentinel1_active = self.weather_active = False
        if self.userSettings.processing_seq == 'DWD Weather':
            self.sentinel1_active = self.sentinel2_active = False
        if self.userSettings.processing_seq == 'Sentinel 1/2':
            self.weather_active = False

        if (self.userSettings.processing_seq == 'DWD Weather' or self.userSettings.processing_seq == "All") and not self.userSettings.gdd_base:
            return "Base T for GDD must be set for GDD = (DWD Tmax + DWD Tmin) / 2 – T calculation!"

        if (self.userSettings.s1_folder == "" or self.userSettings.s2_folder == "" or self.userSettings.weather_folder == "" or self.userSettings.rasdaman_user == ""
                or self.userSettings.rasdaman_passw == "" or self.userSettings.aoi_geojson == ""):
            return "User settings are not complete for selected process!"

        return ""
