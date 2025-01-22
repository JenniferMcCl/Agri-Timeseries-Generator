# -------------------------------------------------------------------------------
# Name:        rasdaman_request
# Purpose:     Executes the requests from the Rasdaman datacubes
#
# Author:      florian.beyer, jennifer.mcclelland, tanja.riedel
#
# Created:     07.02.2024
# Licence:
# -------------------------------------------------------------------------------

import rasterio
import numpy as np
import io
from datetime import date, datetime, timedelta
import requests
import xmltodict

from ipyleaflet import Map, Marker, Polygon  # interactive maps
from requests.auth import HTTPBasicAuth

import rasterio.shutil


class RasdamanRequest:

    @staticmethod
    def get_coverages(host, user='', pw='', use_credentials=False):
        '''
        get a list of all available data cubes

        PARAMETERS:
            host (str): host adress of data cube service
            user (str): credentials username
            pw (str): credentials password
            use_credentials (bool(opt)): If True: personal credentials for datacube service will be used. Defaults to False.

        RETURNS:
            coverages (list): list of all available data cubes on given host
        '''

        query = host + '?SERVICE=WCS&version=2.0.1&request=GetCapabilities'
        if use_credentials == True:
            response = requests.get(query, auth=HTTPBasicAuth(user,pw))
        else:
            response = requests.get(query)
        dict_data = xmltodict.parse(response.content)

        coverages = []
        for i in dict_data['wcs:Capabilities']['wcs:Contents']['wcs:CoverageSummary']:
            coverages.append(i['wcs:CoverageId'])
        return coverages

    @staticmethod
    def get_metadata_from_datacube(layer, host, user='', pw='', use_credentials=False):
        """
        Get a list of all available data cubes.

        PARAMETERS:
            layer (str): coverage name of the data cube
            host (str): host address of data cube service
            user (str): credentials username
            pw (str): credentials password
            use_credentials (bool(opt)): If True: personal credentials for datacube service will be used. Defaults to False.

        RETURNS:
             metadata (dict): list of all available data cubes on given host
        """

        query = host+'?&SERVICE=WCS&VERSION=2.0.1&REQUEST=DescribeCoverage&COVERAGEID='+layer

        if use_credentials:
            response = requests.get(query, auth=HTTPBasicAuth(user,pw))
        else:
            response = requests.get(query)

        metadata = xmltodict.parse(response.content)

        return metadata

    @staticmethod
    def create_s2_tiff(img, name, valid_pixel_portion):
        with rasterio.open(io.BytesIO(img.content), 'r', nodata=0) as src:
            meta = src.meta
            data = src.read()
            valid = RasdamanRequest().check_valid_non_zero(src, valid_pixel_portion)
            if valid[0]:
                with rasterio.open(name, 'w', **meta) as dest:
                    dest.write(data)
            return valid

    @staticmethod
    def check_valid_non_zero(src, valid_pixel_portion):
        data = src.read()
        size = src.read(1).size
        for i in range(data.shape[0]):
            band = src.read(i + 1)
            amount_pix = np.count_nonzero(band != 0)
            vp = amount_pix / band.size * 100
            if vp > valid_pixel_portion:
                return True, vp, size, amount_pix
        return False, vp, size, amount_pix

    @staticmethod
    def calculate_savi(img, valid_pixel_portion=50, noData=0):
        """
        Calculate the Soil adjusted vegetation index from a given img. Must have at least 2 bands

        PARAMETERS:
            img:
                The image to derive the SAVI from.
            valid_pixel_portion: integer
                The minimum portion of valid pixel.
            noData:
                The value to set as no data value.

        RETURNS:
            savi, meta: list
            The calculated SAVI and the updated metadata of the given image
        """
        try:
            with rasterio.open(io.BytesIO(img.content), nodata=noData) as src:
                nir = src.read(1)
                red = src.read(2)
                meta = src.meta
                meta.update({
                    'dtype': rasterio.float32,
                    'count': 1
                })

            vp = np.count_nonzero(nir != 0) / nir.size * 100

            if nir.sum() > 0 and vp > valid_pixel_portion:
                red = red/10000
                nir = nir/10000
                a = (nir-red)
                b = (nir+red+0.5)
                savi = np.divide(a, b, out=np.zeros_like(a), where=b != 0) * 1.5
                savi[savi == 0.0] = np.nan
                return [savi, meta]

        except Exception as e:
            print(e)

    @staticmethod
    def get_map_coords(geometry):
        import geopandas as gpd

        if isinstance(geometry, list) and len(geometry) == 2:
            return [(geometry[1],geometry[0]),[]]

        elif isinstance(geometry, list) and len(geometry) > 2:
            lon = []
            lat = []
            for x,y in geometry:
                lon.append(x)
                lat.append(y)
            polygon = []
            for i in range(len(lon)):
                polygon.append((lat[i],lon[i]))

            return [(sum(lat)/len(lat), sum(lon)/len(lon)), polygon]

        elif (isinstance(geometry, str) and geometry.endswith('.geojson')) or (isinstance(geometry, str) and geometry.endswith('.shp')):
            location = gpd.read_file(geometry).to_crs('EPSG:4326')
            polygones = location['geometry']

            if len(polygones) == 1:
                coords = list(polygones[0].exterior.coords)
                center = (polygones[0].centroid.coords.xy[1][0], polygones[0].centroid.coords.xy[0][0])

                lon = []
                lat = []
                for x, y in coords:
                    lon.append(x)
                    lat.append(y)
                polygon = []
                for i in range(len(lon)):
                    polygon.append((lat[i],lon[i]))

                return [center, polygon]

            elif len(polygones) > 1:

                all_polygones = []

                for polygon in polygones:
                    coords_ = list(polygon.exterior.coords)
                    center = (polygon.centroid.coords.xy[1][0], polygon.centroid.coords.xy[0][0])

                    lon = []
                    lat = []
                    for x,y in coords_:
                        lon.append(x)
                        lat.append(y)
                    polygon_ = []
                    for i in range(len(lon)):
                        polygon_.append((lat[i],lon[i]))

                    all_polygones.append([center, polygon_])

                return all_polygones

        else:
            print('Something went wrong with geometry input. Pleace insert point as list of tuples or polygone(s) as geojson file!')

    @staticmethod
    def get_map(geometry, zoom=8):

        if len(RasdamanRequest().get_map_coords(geometry)) == 2 and isinstance(RasdamanRequest().get_map_coords(geometry)[0], tuple):
            center = RasdamanRequest().get_map_coords(geometry)[0]  # lat , lon
            polygon = Polygon(locations=[RasdamanRequest().get_map_coords(geometry)[1]], color="green", fill_color="green")
            m = Map(center=center, zoom=zoom)
            marker = Marker(location=center, draggable=True)
            m.add_layer(marker);
            m.add_layer(polygon);
            RasdamanRequest().display(m)

        else:
            center = RasdamanRequest().get_map_coords(geometry)[0][0]
            m = Map(center=center, zoom=zoom)

            RasdamanRequest().get_map_coords(geometry)[1]
            for area in RasdamanRequest().get_map_coords(geometry):
                marker = Marker(location=area[0], draggable=True)
                m.add_layer(marker);
                polygon = Polygon(locations=[area[1]], color="green", fill_color="green")
                m.add_layer(polygon);

            # This is from Jupyter Notebook
            # display(m)

    @staticmethod
    def get_all_dates(year):
        """This function returns a list of all days of the given year.

        Args:
            year (str): year YYYY

        Returns:
            list: List of days of the given year
        """
        try:
            from datetime import date, timedelta

            start = date(int(year),1,1)
            end = date(int(year),12,31)

            delta = end - start

            days = []

            for i in range(delta.days + 1):
                day = start + timedelta(days=i)
                day = day.strftime('%Y-%m-%d')
                days.append(day)

            return days
        except Exception as e:
            print('Error in get_all_dates function: {}'.format(e))

    @staticmethod
    def get_coverage_subset(startdate, enddate, rasdaman_layer, easting, northing, user, passwd,
                            epsg_output=None, band=None, day_begin='T00:00:00.000Z'):

        url = 'https://datacube.julius-kuehn.de/flf/ows'
        service = '?&SERVICE=WCS'
        version = '&VERSION=2.0.1'
        request = '&REQUEST=GetCoverage'
        coverage_id = '&COVERAGEID=' + rasdaman_layer
        subset_time = '&SUBSET=ansi("' + startdate + day_begin + '","' + enddate + day_begin +'")'
        subsetting_crs = '&subsettingCrs=http://ows.rasdaman.org/def/crs/EPSG/0/' + str(epsg_output) if epsg_output is not None\
            else ""
        subset_lat = '&SUBSET=E(' + str(float(easting)) + ')'
        subset_long = '&SUBSET=N(' + str(float(northing)) + ')'
        subset_band =' &RANGESUBSET=' + band if band is not None else ""
        encode_format = '&FORMAT=text/csv'

        query=url + service + version + request + coverage_id + subset_time + subsetting_crs + subset_lat + \
              subset_long + subset_band + encode_format

        print("Query: " + query + " Request sent to Rastaman server")
        return RasdamanRequest.try_rastaman_request(query, user, passwd)

    @staticmethod
    def try_rastaman_request(url_query, user, passwd, dwd=False):
        try:
            data = requests.get(url_query, auth=HTTPBasicAuth(user, passwd))

            if data.status_code == 200 and not dwd:
                print('Request successful')
                float_map = str(data.content).split("'")[1].split(',')
                return list(float_map)
            elif data.status_code == 200 and dwd:
                print('Request successful')
                float_map = str(data.content).split("'")[1].split(',')
                float_list = list(float_map)
                if float_list[0] != -9999 and len(float_list) > 1:
                    for ind, i in enumerate(float_list):
                        if i == -9999.0:
                            float_list[ind] = 0.0
                        float_list[ind] = float_list[ind] / 10
                    return float_list
                elif float_list[0] == -9999:
                    print('No data, returning a list of zeros...')
                    float_list = [0] * len(float_list)
                    return float_list
            else:
                print('something went wrong. Request was answered with request code: {}. URL: {}'.format(data.status_code, data.url))
        except Exception as e:
            print(e)

    @staticmethod
    def get_veg_period(crop_type, year):

    #crop_types=['sunflowers', 'maize', 'soybeans', 'rape', 'barley', 'wheat']
    #crop=[x for x in crop_types if x in file_in]

        if crop_type == 'sunflowers':
            startdate=datetime.datetime.strptime("01.03." + year, "%d.%m.%Y")
            enddate=datetime.datetime.strptime("01.11." + year, "%d.%m.%Y")
            #print(startdate, enddate)

        if crop_type == 'maize' or crop_type == 'soybeans' or crop_type == 'sugarbeets':
            startdate=datetime.datetime.strptime("01.03." + year, "%d.%m.%Y")
            enddate=datetime.datetime.strptime("30.11." + year, "%d.%m.%Y")

        if crop_type == 'summerbarley' or crop_type == 'oat' or crop_type == 'potatoes':
            startdate=datetime.datetime.strptime("01.02." + year, "%d.%m.%Y")
            enddate=datetime.datetime.strptime("31.08." + year, "%d.%m.%Y")

        if crop_type == 'rape' or crop_type == 'barley' or crop_type == 'wheat' or crop_type == 'triticale' or crop_type == 'rye':
            year_start=str(int(year)-1)
            startdate=datetime.datetime.strptime("01.08." + year_start, "%d.%m.%Y")
            enddate=datetime.datetime.strptime("31.08." + year, "%d.%m.%Y")

        return startdate, enddate

    @staticmethod
    def get_dates_in_range(start_date_str, end_date_str):
        # Convert the string dates to datetime objects
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

        # Initialize an empty list to hold the dates
        date_list = []

        # Use a loop to generate all dates in the range
        current_date = start_date
        while current_date <= end_date:
            date_list.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)

        return date_list
