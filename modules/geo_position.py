# --------------------------------------------------------------------------------------------------------------------------------
# Name:        geo_position
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2023
# Copyright:   (c) jennifer.mcclelland 2023
#
# --------------------------------------------------------------------------------------------------------------------------------


import json
import geojson

from shapely.geometry import Point
from shapely.geometry import shape, Polygon, MultiPolygon
from shapely import wkt, ops
from pyproj import Transformer

# Lots of helpers to translate coordinate types and systems.


def load_geojson(geo_json_file):
    with open(geo_json_file) as data:
        return json.load(data)


def load_wkt_from_geojson(geo_json_file):
    with open(geo_json_file) as data:
        geo = json.load(data)
    return geojson_to_wkt(geo)


def wkt_to_geojson(wkt_coords):
    g1 = wkt.loads(wkt_coords)
    g2 = geojson.Feature(geometry=g1, properties={})
    return g2.geometry


def geojson_to_wkt(geojson_coords):
    s = json.dumps(geojson_coords)
    g1 = geojson.loads(s)
    g2 = shape(g1)
    return g2.wkt


#
def transfer_geom(poly_json, old_crs, new_crs):
    """
        This function takes a geojson and returns a crs transformed shape polygon
        :param poly_json:
        :param old_crs:
        :param new_crs:
        :return:
    """

    # access geojson geometry as shape polygon
    with open(poly_json) as data:
        geoms = json.load(data)
        #poly = shape(geoms)
        poly = shape(geoms['geometry'])

        trans = Transformer.from_crs(old_crs, new_crs, always_xy=True)
        new_shape = ops.transform(trans.transform, poly)
        return new_shape


def transfer_point(point, old_crs, new_crs):
    """
        This function takes a point and returns a crs transformed shape polygon.
        :param poly_json:
        :param old_crs:
        :param new_crs:
        :return:
    """
    point = Point(point[0], point[1])

    trans = Transformer.from_crs(old_crs, new_crs, always_xy=True)
    new_point = ops.transform(trans.transform, point)
    return new_point


def get_centroid_bounds_area(polygon):
    """
        This derives and returns the centroid, the bounds and the area from a give polygon.
    """
    centroid = polygon.centroid
    area = polygon.area
    bounds = polygon.bounds

    return centroid, bounds, area


def calculate_area(geojson_path):
    """
    Calculate the area of a GeoJSON polygon in square meters.

    :param geojson_path: Path to the GeoJSON file.
    :return: Area in square meters.
    """

    with open(geojson_path, 'r') as file:
        geojson_data = geojson.load(file)

    if geojson_data.get("type") == "Feature":
        geojson_data = geojson_data.get("geometry")

    coords = geojson_data['coordinates']

    if isinstance(coords[0][0][0], list):
        multi_polygon = MultiPolygon([Polygon(coord[0]) for coord in coords])
        area = multi_polygon.area
    else:
        polygon = Polygon(coords[0])
        geom = shape(polygon)
        area = geom.area

    return area


def create_bounding_box(center_geometry, width, height):
    """
        Function to create a bounding box polygon around a point.
        :param center_geometry:
        :param width:
        :param height:
        :return: polygon: The polygon created
    """
    if isinstance(center_geometry, Polygon):
        print("FROM_POINT macro set to false. Can not run with Polygons")
        return None
    elif isinstance(center_geometry, Point):
        # Extract coordinates from the Point object
        cx, cy = center_geometry.x, center_geometry.y
    elif isinstance(center_geometry, tuple) and len(center_geometry) == 2:
        # Assume it's a tuple of (x, y) coordinates
        cx, cy = center_geometry

    half_width = width / 2
    half_height = height / 2
    polygon = Polygon([
        (cx - half_width, cy - half_height),
        (cx + half_width, cy - half_height),
        (cx + half_width, cy + half_height),
        (cx - half_width, cy + half_height),
        (cx - half_width, cy - half_height)
    ])
    return polygon
