# -------------------------------------------------------------------------------------------------------------------------------
# Name:        date_transformer
# Purpose:
#
# Author:      jennifer.mcclelland
#
# Created:     2024
# Copyright:   (c) jennifer.mcclelland 2024
#
# --------------------------------------------------------------------------------------------------------------------------------


from datetime import datetime, timedelta


class DateTransformer:

    @staticmethod
    def trans_d_m_y_dates_to_compact_dates(date_list):
        """
        Transform a list of dates from the format "yy/mm/dd" to "yyyymmdd".

        Args:
            date_list (list): A list of date strings in the format "yy/mm/dd".

        Returns:
            list: A list of date strings in the format "yyyymmdd".
        """
        transformed_dates = []

        for date_str in date_list:
            if date_str == "":
                transformed_dates.append("")
                continue
            # Parse the date string into a datetime object
            date_obj = datetime.strptime(date_str, "%Y/%m/%d")
            # Format the datetime object into the desired output format
            formatted_date = date_obj.strftime("%Y%m%d")
            # Append the formatted date string to the new list
            transformed_dates.append(formatted_date)
        return transformed_dates

    @staticmethod
    def trans_compact_d_m_y_dates_to_sql_format(date_list):
        """
        Transforms a list of dates from the format 'YYYYMMDD' to 'YYYY-MM-DD'.

        Parameters:
            date_list (list): A list of dates in the format 'YYYYMMDD'.

        Returns:
            list: A list of dates in the format 'YYYY-MM-DD'.
        """
        transformed_dates = []

        for date in date_list:
            if date == "":
                transformed_dates.append("")
                continue
            if len(date) == 8:
                transformed_date = f"{date[:4]}-{date[4:6]}-{date[6:]}"
                transformed_dates.append(transformed_date)
            else:
                print(f"Ignoring invalid date: {date}")
        return transformed_dates

    @staticmethod
    def trans_d_m_y_dates_to_sql_format(date_list):
        """
        Transforms a list of dates from the format 'DD/MM/YY' to 'YYYY-MM-DD'.

        Parameters:
            date_list (list): A list of dates in the format 'DD/MM/YY'.

        Returns:
            list: A list of dates in the format 'YYYY-MM-DD'.
        """
        transformed_dates = []

        for date_str in date_list:
            try:
                # Parse the date string to a datetime object
                date_obj = datetime.strptime(date_str, '%d/%m/%y')
                # Format the datetime object to 'YYYY-MM-DD' and append to the list
                transformed_dates.append(date_obj.strftime('%Y-%m-%d'))
            except ValueError:
                print(f"Ignoring invalid date: {date_str}")
        return transformed_dates

    @staticmethod
    def generate_date_range(start_date, end_date):
        # Convert start and end dates to datetime objects
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Initialize a list to store the generated dates
        date_range = []

        # Iterate through the date range, including the end date
        current_dt = start_dt
        while current_dt <= end_dt:
            date_range.append(current_dt.strftime('%Y-%m-%d'))
            current_dt += timedelta(days=1)

        return date_range
