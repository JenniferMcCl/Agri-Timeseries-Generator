# --------------------------------------------------------------------------------------------------------------------------------
# Name:        veg_specific_tiff_operations
# Purpose:     This class holds functions to calculate the NDVI and RVI on raster backscatter and optical geotiffs.
#
# Author:      jennifer.mcclelland
#
# Created:     2023
# Copyright:   (c) jennifer.mcclelland 2022
#
# --------------------------------------------------------------------------------------------------------------------------------

import numpy as np


class VegSpecificTiffOperations:

    @staticmethod
    def calculate_norm_rvi(np_array):
        """
        This method is to calculate the normalized Radar Vegetation Index (RVI)
        Params:
         - np_array: The array to perform calculation. Must have 3 dimensions.
        Return:
         - The 2-dimensional RVI result.
        """

        # Ensure the input array is 3-dimensional
        if np_array.ndim != 3:
            print("Dimensions for RVI calculation are not correct.")
            return None

        # Avoid division by zero: Check for cases where np_array[0] + np_array[1] == 0
        with np.errstate(divide='ignore', invalid='ignore'):
            bsc_rvi = 4 * np_array[1] / (np_array[1] + np_array[0])
            bsc_rvi[np.isinf(bsc_rvi)] = 0  # Set infinite results (from division by zero) to 0
            bsc_rvi = np.nan_to_num(bsc_rvi, nan=0)  # Replace NaNs with 0

        # Normalize to [0, 1]
        min_val = np.nanmin(bsc_rvi)
        max_val = np.nanmax(bsc_rvi)

        if max_val - min_val == 0:
            return np.zeros(bsc_rvi.shape)  # Avoid divide by zero in normalization

        return (bsc_rvi - min_val) / (max_val - min_val)

    @staticmethod
    def calculate_norm_ndvi(np_array):
        # TODO: Test functionality. Something seems to be wrong with the output. Maybe bands are wrong.
        """
        This method calculates the normalized NDVI (Normalized Difference Vegetation Index).
        Params:
         - np_array: The array to perform the calculation. Must have at least 8 bands.
        Return:
         - The 2-dimensional normalized NDVI result.
        """

        if np_array.shape[0] < 8:
            print("NDVI cannot be calculated with this array. At least 8 bands required.")
            return None

        # Check for invalid data where both bands (8 and 3) are 0 to avoid division by zero
        with np.errstate(divide='ignore', invalid='ignore'):
            # Calculate NDVI: (NIR - Red) / (NIR + Red)
            ndvi_data = (np_array[7, :, :] - np_array[2, :, :]) / (np_array[7, :, :] + np_array[2, :, :])

            # Handle invalid values: set divisions by zero and NaNs to 0
            ndvi_data = np.nan_to_num(ndvi_data, nan=0, posinf=0, neginf=0)

        # Normalize NDVI to range [0, 1]
        min_val = np.nanmin(ndvi_data)
        max_val = np.nanmax(ndvi_data)

        if max_val - min_val == 0:
            return np.zeros(ndvi_data.shape)  # Avoid divide by zero in normalization

        ndvi_data_norm = (ndvi_data - min_val) / (max_val - min_val)

        return ndvi_data_norm

