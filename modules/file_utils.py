# --------------------------------------------------------------------------------------------------------------------------------
# Name:        file_utils
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
# --------------------------------------------------------------------------------------------------------------------------------


import os
import csv
from itertools import groupby


class FileUtils:
    """
    A utility class for file-related operations.
    """

    @staticmethod
    def extract_number_from_folder_name(folder_path):
        """
        Extracts a number from the given folder path.

        Args:
            folder_path (str): The path to the folder.

        Returns:
            int or None: The extracted number, or None if no number is found.
        """
        if folder_path is None:
            return None
        folder_name = os.path.basename(folder_path)
        return next((int(word) for word in folder_name.split('_') if word.isdigit()), None)

    @staticmethod
    def find_number_in_ordered_paths(ordered_paths, target_number):
        """
        Finds the path containing a specific number in a list of ordered paths.

        Args:
            ordered_paths (list of str): The list of ordered paths.
            target_number (int): The target number to search for.

        Returns:
            str or None: The path containing the target number, or None if not found.
        """
        if ordered_paths is None or target_number is None:
            return None
        return next((folder_path for folder_path in ordered_paths if FileUtils.extract_number_from_folder_name(folder_path) == target_number), None)

    @staticmethod
    def read_column(file_path, column_id, empty_entries):
        """
        Reads a column from a CSV file.

        Args:
            file_path (str): The path to the CSV file.
            column_id (int): The index of the column to read.
            empty_entries (bool): Flag indicating whether to include empty entries.

        Returns:
            list or None: The values of the specified column, or None if file or arguments are invalid.
        """
        if file_path is None or column_id is None or empty_entries is None:
            return None
        if not os.path.isfile(file_path):
            return None
        with open(file_path, 'r') as csvfile:
            reader = csv.reader(csvfile)

            # Check if the column index exceeds the row length
            for row in reader:
                if column_id >= len(row):
                    return []

            # Reset the file pointer to the beginning
            csvfile.seek(0)

            return [
                int(row[column_id]) if not empty_entries and row and row[column_id].isdigit() and int(
                    row[column_id]) > 0 else row[column_id]
                for row in reader if row
            ]

    @staticmethod
    def create_list_of_lists(original_list):
        """
        Creates a list of lists by grouping consecutive non-empty elements of the original list.

        Args:
            original_list (list): The original list.

        Returns:
            list or None: The list of lists, or None if the original list is None.
        """
        if original_list is None:
            return None
        return [list(group) for key, group in groupby(original_list, lambda x: x != '') if key]

    @staticmethod
    def create_list_of_dicts(original_list, field_id_list):
        """
        Creates a list of dictionaries from a list of pairs.

        Args:
            original_list (list): The original list of pairs.

        Returns:
            list or None: The list of dictionaries, or None if the original list is None.
        """
        if original_list is None:
            return None

        # Initialize an empty list to store dictionaries
        list_of_dicts = []

        # Initialize an empty dictionary to accumulate pairs
        current_dict = {}

        field_id_index = 0
        field_id = field_id_list[field_id_index]
        last_field_id = 0

        # Iterate over the original list
        for i, (key, value) in enumerate(original_list):
            if key == '' and field_id != last_field_id:
                if current_dict:
                    list_of_dicts.append(current_dict)
                current_dict = {}
            else:
                current_dict[key] = value

            if i + 1 < len(original_list):
                next_key, next_value = original_list[i + 1]

            if key == '' and next_key != '':
                last_field_id = field_id
                field_id_index = field_id_index + 1 if field_id_index < len(field_id_list) - 1 else field_id_index
                field_id = field_id_list[field_id_index]

        # Ensure the last dictionary is added if not empty
        if current_dict:
            list_of_dicts.append(current_dict)

        return list_of_dicts

    @staticmethod
    def create_date_value_pair_dict(dates, values1, values2, values3, values4, values5):
        """
        Create a dictionary using dates as keys and paired items from values1 and values2 as values.

        Parameters:
            dates (list): A list of dates.
            values1 (list): A list of floats.
            values2 (list): A list of floats.

        Returns:
            dict: A dictionary with dates as keys and paired items from values1 and values2 as values.
                  Returns None if the lengths of dates, values1, and values2 are not the same.
        """
        # Check if the lengths of the lists are the same
        if len(dates) != len(values1) or len(dates) != len(values2) or len(values1) != len(values2):
            print("Lists must have the same length.")
            return None

        # Create the dictionary
        result = {}
        for i in range(len(dates)):
            result[dates[i]] = (values1[i], values2[i], values3[i], values4[i], values5[i])

        return result

    @staticmethod
    def write_dict_to_csv(dictionary, filename):
        """
        Write the content of a dictionary to a CSV file.

        Parameters:
            dictionary (dict): The dictionary to write to the CSV file.
            filename (str): The name of the CSV file.

        Returns:
            None
        """
        # Extract field names (dictionary keys)
        fieldnames = list(dictionary.keys())

        # Open the CSV file in write mode
        with open(filename, 'w', newline='') as csvfile:
            # Create a CSV writer object
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write the header
            writer.writeheader()

            # Write the content (dictionary values)
            for row in zip(*dictionary.values()):
                writer.writerow({key: value for key, value in zip(fieldnames, row)})

            print ("DWD weather .csv created: " + filename)

    @staticmethod
    def read_csv_to_dict(filename):
        """
        Read the content of a CSV file and extract it into a dictionary.

        Parameters:
            filename (str): The name of the CSV file to read.

        Returns:
            dict: A dictionary containing the content read from the CSV file.
        """
        # Initialize an empty dictionary
        data = {}

        # Open the CSV file in read mode
        with open(filename, 'r', newline='') as csvfile:
            # Create a CSV reader object
            reader = csv.DictReader(csvfile)

            # Extract field names
            fieldnames = reader.fieldnames

            # Iterate over rows in the CSV file
            for row in reader:
                # Extract values from the row
                for field in fieldnames:
                    if field not in data:
                        data[field] = []
                    data[field].append(row[field])

        return data
    
    @staticmethod
    def extract_date_from_tiff_path(tiff_name):
        """
        Extracts a date from the given TIFF file path.

        Args:
            tiff_path (str): The path to the TIFF file.

        Returns:
            str or None: The extracted date in SQL format (YYYY-MM-DD), or None if no valid date is found.
        """
        if tiff_name is None:
            return None

        # Split the filename by underscores
        parts = tiff_name.split('_')

        # Iterate over parts to find a segment that matches the date pattern
        for part in parts:
            if len(part) == 8 and part.isdigit():
                # Check if the part is a valid date in the format YYYYMMDD
                year = part[:4]
                month = part[4:6]
                day = part[6:]

                if year.startswith("20"):
                    return f"{year}-{month}-{day}"

        # If no valid date is found, return None
        return None

    @staticmethod
    def remove_consecutive_empty_pairs(pair_list):
        if not pair_list:
            return pair_list

        result = [pair_list[0]]
        for i in range(1, len(pair_list)):
            if pair_list[i] == ('', '') and pair_list[i - 1] == ('', ''):
                continue
            result.append(pair_list[i])

        return result

    @staticmethod
    def remove_aux_xml(files):
        """
        Remove all items in the list that end with 'aux.xml'.

        Args:
            files (list of str): The list of file names.

        Returns:
            list of str: The filtered list of file names.
        """
        return [file for file in files if not file.endswith('aux.xml')]






