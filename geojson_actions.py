import json
import os
from shapely.geometry import Point, Polygon, GeometryCollection, shape, mapping
from pyproj import Transformer
from shapely.ops import transform as trans
from shapely import wkt
from shapely.wkt import dumps

# This is a convince script to work on geojsons in ESPG 4326 and 25832, containing agriculture relevant information
# in the properties. The methods work with geojson and wkt formats.


# Initialize the transformer for coordinate transformation
trans_4326_to_25832 = Transformer.from_crs("EPSG:4326", "EPSG:25832", always_xy=True)
trans_25832_to_4326 = Transformer.from_crs("EPSG:25832", "EPSG:4326", always_xy=True)


def get_containing_polygon(geometry_collection_wkt, point_wkt):
    """
    Finds the polygon in the geometry collection that contains the point.

    Parameters:
        geometry_collection_wkt (str): WKT string of a geometry collection containing polygons.
        point_wkt (str): WKT string of the point.

    Returns:
        str: WKT of the containing polygon, or None if no polygon contains the point.
    """
    # Load the geometry collection and point from WKT
    geometry_collection = wkt.loads(geometry_collection_wkt)
    point = wkt.loads(point_wkt)

    # Check each polygon in the collection
    for geometry in geometry_collection.geoms:
        if geometry.contains(point):
            return geometry  # Return the WKT of the containing polygon

    return None  # Return None if no containing polygon is found


def create_geometry_collection(folder_path):
    """
    Takes a folder containing GeoJSON files, retrieves points, transforms them to EPSG:4326,
    and creates a GeometryCollection in WKT format.

    Args:
        folder_path (str): Path to the folder containing GeoJSON files.

    Returns:
        str: WKT representation of the GeometryCollection.
    """
    geometries = []

    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if file_name.endswith('.geojson') and os.path.isfile(file_path):
            with open(file_path, 'r') as f:
                geojson_data = json.load(f)
                if geojson_data.get("type") == "Feature" and geojson_data.get("geometry", {}).get("type") == "Point":
                    coordinates = geojson_data["geometry"]["coordinates"]
                    # Transform point to EPSG:4326
                    lon, lat = trans_4326_to_25832.transform(*coordinates)
                    geometries.append(Point(lon, lat))

    # Create GeometryCollection and return its WKT
    geometry_collection = GeometryCollection(geometries)
    return dumps(geometry_collection)


def replace_point_with_polygon(folder_path_1, folder_path_2):
    """
        Takes 2 folder containing GeoJSON files, retrieves points from first, polygons from second
        and replaced the points with polygons for each geojson with a match. Matches are defined by
        a town name in the file name.

        Args:
            folder_path_1 (str): Path to the folder containing GeoJSON point files.
            folder_path_2 (str): Path to the folder containing GeoJSON polygon files.
    """

    # Get sorted file lists
    files1 = sorted([f for f in os.listdir(folder_path_1) if f.endswith(".geojson")])
    files2 = sorted([f for f in os.listdir(folder_path_2) if f.endswith(".geojson")])

    for file1 in files1:
        # Find corresponding file in folder_path_2
        town_name = os.path.splitext(file1)[0]
        corresponding_file2 = f"{town_name}-field.geojson"

        if corresponding_file2 not in files2:
            print(f"Warning: No matching file for {file1} in folder_path_2.")
            continue

        # Load GeoJSON data from both files
        path1 = os.path.join(folder_path_1, file1)
        path2 = os.path.join(folder_path_2, corresponding_file2)

        with open(path1, 'r') as f1, open(path2, 'r') as f2:
            geojson1 = json.load(f1)
            geojson2 = json.load(f2)

        # Extract the MultiPolygon and simplify to Polygon
        try:
            multipolygon = geojson2["features"][0]["geometry"]
            if multipolygon["type"] != "MultiPolygon":
                print(f"Error: Geometry in {corresponding_file2} is not a MultiPolygon.")
                continue

            polygon_coords = multipolygon["coordinates"][0]  # Take the first Polygon
            polygon = Polygon(polygon_coords[0])

            # Replace the Point geometry in geojson1 with the new Polygon geometry
            if geojson1["geometry"]["type"] != "Point":
                print(f"Error: Geometry in {file1} is not a Point.")
                continue

            geojson1["geometry"] = mapping(polygon)

            # Save the updated GeoJSON back to folder_path_1
            with open(path1, 'w') as f1:
                json.dump(geojson1, f1, indent=4)

        except (KeyError, IndexError) as e:
            print(f"Error processing {file1} and {corresponding_file2}: {e}")


def transform_geojson_polygon_to_4326(folder_path):
    """
        Takes a folder containing GeoJSON files, retrieves polygon coordinates, transforms them to EPSG:4326,
        and updates the coordinates .

        Args:
            folder_path (str): Path to the folder containing GeoJSON files.
    """

    # Iterate through the GeoJSON files in the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".geojson"):
            input_path = os.path.join(folder_path, file_name)
            output_path = os.path.join(folder_path, file_name.removesuffix('.geojson') + "-4326.geojson")

            try:
                # Load the GeoJSON file
                with open(input_path, 'r') as f:
                    geojson_data = json.load(f)

                # Check if the geometry is a Polygon
                geometry = shape(geojson_data["geometry"])
                if isinstance(geometry, Polygon):
                    # Transform the Polygon geometry
                    transformed_geometry = trans(trans_25832_to_4326.transform, geometry)

                    # Update the geometry in the GeoJSON content
                    geojson_data["geometry"] = mapping(transformed_geometry)

                    # Save the transformed GeoJSON to a new file
                    with open(output_path, 'w') as f:
                        json.dump(geojson_data, f, indent=4)
                else:
                    print(f"Skipping file {file_name}: Geometry is not a Polygon.")

            except Exception as e:
                print(f"Error processing {file_name}: {e}")


# Example usage
geojson_folder = "Example/Folder/Path"

geometry_collection_wkt = create_geometry_collection(geojson_folder)
print(geometry_collection_wkt)
