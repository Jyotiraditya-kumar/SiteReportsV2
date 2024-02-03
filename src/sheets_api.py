import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
# from google.oauth2.service_account import Credentials
import pandas as pd
creds_path="/home/jyotiraditya/PycharmProjects/jupyterProject/src/google_service_credentials.json"
class GoogleSheetsManager:
    """
    Class for managing Google Sheets.
    """

    def __init__(self, creds_file=creds_path, folder_id='1dXOU84Aw_c-Jr3PXOtIWI6T9xXD1CexF'):
        """
        Initialize the GoogleSheetsManager.

        Args:
            creds_file (str): Path to the Google service account credentials file.
            folder_id (str, optional): ID of the folder where new sheets will be created.

        Returns:
            None
        """
        self.creds_file = creds_file
        self.client = self.authenticate_with_service_account()
        self.folder_id = folder_id

    def authenticate_with_service_account(self):
        """
        Authenticate with the Google Sheets API using the provided service account credentials.

        Returns:
            gspread.Client: Authenticated client for interacting with Google Sheets API.
        """
        client = gspread.service_account(self.creds_file)
        return client

    def read_worksheet_as_dataframe(self, worksheet):
        """
        Read data from a worksheet and return it as a DataFrame.

        Args:
            worksheet (gspread.Worksheet): The worksheet to read data from.

        Returns:
            pandas.DataFrame: DataFrame containing the data from the worksheet.
        """
        df = get_as_dataframe(worksheet,evaluate_formulas=True)
        return df

    def write_dataframe_to_worksheet(self, df, worksheet):
        """
        Write a DataFrame to a worksheet.

        Args:
            df (pandas.DataFrame): The DataFrame to write.
            worksheet (gspread.Worksheet): The target worksheet.

        Returns:
            None
        """
        worksheet.clear()
        set_with_dataframe(worksheet, df)

    def create_new_sheet(self, sheet_name):
        """
        Create a new Google Sheet.

        Args:
            sheet_name (str): Name of the new sheet.

        Returns:
            gspread.Spreadsheet: The newly created Google Sheet.
        """
        if self.folder_id:
            title = sheet_name
            parent_folder_id = self.folder_id
            client = self.client
            sheet = client.create(title, folder_id=parent_folder_id)
        else:
            sheet = self.client.create(sheet_name)
        return sheet

    def append_dataframe_to_worksheet(self, df, worksheet):
        """
        Append a DataFrame to an existing worksheet.

        Args:
            df (pandas.DataFrame): The DataFrame to append.
            worksheet (gspread.Worksheet): The target worksheet.

        Returns:
            None
        """
        existing_data = get_as_dataframe(worksheet)
        combined_df = existing_data.append(df, ignore_index=True)
        set_with_dataframe(worksheet, combined_df)

    def delete_sheet(self, sheet_id):
        """
        Delete a Google Sheet by its ID.

        Args:
            sheet_id (str): ID of the sheet to be deleted.

        Returns:
            None
        """
        self.client.del_spreadsheet(sheet_id)

    def create_new_worksheet(self, spreadsheet, worksheet_title):
        """
        Create a new worksheet inside an existing spreadsheet.

        Args:
            spreadsheet (gspread.Spreadsheet): The target spreadsheet.
            worksheet_title (str): Name of the new worksheet.

        Returns:
            gspread.Worksheet: The newly created worksheet.
        """
        worksheet = spreadsheet.add_worksheet(worksheet_title, 1, 1)
        return worksheet

    def clear_worksheet(self, worksheet):
        """
        Clear all content from a worksheet.

        Args:
            worksheet (gspread.Worksheet): The target worksheet.

        Returns:
            None
        """
        worksheet.clear()

    def format_cells(self, worksheet, cell_range, cell_format):
        """
        Apply a specific format to a range of cells.

        Args:
            worksheet (gspread.Worksheet): The target worksheet.
            cell_range (str): Range of cells to apply the format (e.g., 'A1:B5').
            cell_format (str): The format to apply (e.g., 'bold', 'italic', 'underline').

        Returns:
            None
        """
        cell_list = worksheet.range(cell_range)
        for cell in cell_list:
            cell.value = cell_format
        worksheet.update_cells(cell_list)

    def filter_data(self, worksheet, criteria):
        """
        Filter data in a worksheet based on specified criteria.

        Args:
            worksheet (gspread.Worksheet): The target worksheet.
            criteria (str): Filtering criteria (e.g., 'A > 10').

        Returns:
            pandas.DataFrame: Filtered data as a DataFrame.
        """
        data = get_as_dataframe(worksheet)
        filtered_data = data[data.eval(criteria)]
        return filtered_data

    def search_and_retrieve(self, worksheet, search_value):
        """
        Search for a value in a worksheet and retrieve matching rows.

        Args:
            worksheet (gspread.Worksheet): The target worksheet.
            search_value: The value to search for.

        Returns:
            pandas.DataFrame: Rows containing the search value as a DataFrame.
        """
        data = get_as_dataframe(worksheet)
        search_results = data[data.eq(search_value).any(axis=1)]
        return search_results

    def get_cell_range_as_dataframe(self, worksheet, cell_range):
        """
        Retrieve a range of cells from a worksheet as a DataFrame.

        Args:
            worksheet (gspread.Worksheet): The target worksheet.
            cell_range (str): Range of cells to retrieve (e.g., 'A1:B5').

        Returns:
            pandas.DataFrame: Data from the specified cell range.
        """
        cell_list = worksheet.range(cell_range)
        data = [[cell.value for cell in row] for row in cell_list]
        df = pd.DataFrame(data)
        return df

    def update_cell_value(self, worksheet, cell, value):
        """
        Update the value of a specific cell in a worksheet.

        Args:
            worksheet (gspread.Worksheet): The target worksheet.
            cell (str): The cell reference (e.g., 'A1').
            value: The value to set.

        Returns:
            None
        """
        worksheet.update(cell, value)

    def export_data_to_csv(self, worksheet, file_path):
        """
        Export data from a worksheet to a CSV file.

        Args:
            worksheet (gspread.Worksheet): The target worksheet.
            file_path (str): Path where the CSV file will be saved.

        Returns:
            None
        """
        data = get_as_dataframe(worksheet)
        data.to_csv(file_path, index=False)

    def add_chart(self, worksheet, chart_type, start_cell, end_cell):
        """
        Add a chart to a worksheet.

        Args:
            worksheet (gspread.Worksheet): The target worksheet.
            chart_type (str): Type of chart (e.g., 'line', 'bar').
            start_cell (str): Starting cell for the chart (e.g., 'A1').
            end_cell (str): Ending cell for the chart (e.g., 'D10').

        Returns:
            None
        """
        chart = worksheet.new_chart(chart_type)
        chart.range(start_cell, end_cell)
        worksheet.insert_chart(chart)

    def share_sheet(self, sheet, email, role='reader'):
        """
        Share a Google Sheet with a specific email address.

        Args:
            sheet_id (str): ID of the Google Sheet.
            email (str): Email address of the recipient.
            role (str, optional): Role for the recipient (default is 'reader').

        Returns:
            None
        """
        # sheet = self.client.open_by_key(sheet_id)
        sheet.share(email, perm_type='user', role=role, notify=True)

    def unshare_sheet(self, sheet, email):
        """
        Remove sharing permissions for a specific email address.

        Args:
            sheet_id (str): ID of the Google Sheet.
            email (str): Email address of the recipient.

        Returns:
            None
        """
        # sheet = self.client.open_by_key(sheet_id)
        permissions = sheet.list_permissions()
        for permission in permissions:
            if permission['emailAddress'] == email:
                sheet.remove_permission(permission['id'])
                break

    def get_sheet_by_id(self, sheet_id):
        """
        Get a sheet by its ID.

        Args:
            sheet_id (str): ID of the sheet.

        Returns:
            gspread.Spreadsheet: The requested Google Sheet.
        """
        sheet = self.client.open_by_key(sheet_id)
        return sheet

    def set_column_width(self, worksheet, column, width):
        """
        Set the width of a specific column in a worksheet.

        Args:
            worksheet (gspread.Worksheet): The target worksheet.
            column (str): The column letter (e.g., 'A', 'B').
            width (int): The width to set, in pixels.

        Returns:
            None
        """
        worksheet.set_column_width(col=column, width=width)

    def freeze_rows(self, worksheet, num_rows):
        """
        Freeze a specific number of rows at the top of a worksheet.

        Args:
            worksheet (gspread.Worksheet): The target worksheet.
            num_rows (int): The number of rows to freeze.

        Returns:
            None
        """
        worksheet.freeze(rows=num_rows)

    def freeze_columns(self, worksheet, num_cols):
        """
        Freeze a specific number of columns on the left side of a worksheet.

        Args:
            worksheet (gspread.Worksheet): The target worksheet.
            num_cols (int): The number of columns to freeze.

        Returns:
            None
        """
        worksheet.freeze(cols=num_cols)

# Example usage
if __name__ == '__main__':
    credentials_file = 'path_to_your_service_account_json_file.json'
    folder_id = 'YOUR_FOLDER_ID'

    manager = GoogleSheetsManager(credentials_file, folder_id=folder_id)

    # Rest of your code...
