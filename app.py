import logging
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('drive_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
CONFIG = {
    'scopes': ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets'],
    'service_account_file': 'service_account.json',
    'user_register_sheet': 'User Register',
    'start_date_filter': '2025-04-15',
    'date_format': '%d/%m/%Y',
    'column_mappings': {
        'source_name': lambda col: 'source' in col.lower() and 'name' in col.lower(),
        'ref_by': lambda col: 'ref' in col.lower() and 'by' in col.lower(),
        'created_at': lambda col: 'created' in col.lower() and 'at' in col.lower(),
    }
}

class GoogleServiceManager:
    """Handles Google Drive and Sheets API interactions."""
    
    def __init__(self, service_account_file, scopes):
        self.drive_service, self.sheets_service = self._authenticate(service_account_file, scopes)
        if not self.drive_service or not self.sheets_service:
            logger.error("Failed to authenticate with Google API")
            raise Exception("Google API authentication error")
    
    def _authenticate(self, service_account_file, scopes):
        """Authenticate with Google API and return Drive and Sheets services."""
        try:
            creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
            return build('drive', 'v3', credentials=creds), build('sheets', 'v4', credentials=creds)
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return None, None
    
    def find_file_in_folder(self, folder_id, file_name):
        """Find a file in the specified folder by name."""
        try:
            query = f"'{folder_id}' in parents and name = '{file_name}' and trashed = false"
            results = self.drive_service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get('files', [])
            if files:
                logger.info(f"Found file '{file_name}' with ID: {files[0]['id']}")
                return files[0]['id']
            logger.info(f"File '{file_name}' not found in folder")
            return None
        except Exception as e:
            logger.error(f"Error finding file '{file_name}': {e}")
            return None
    
    def create_spreadsheet(self, folder_id, file_name):
        """Create a new Google Spreadsheet and move it to the specified folder."""
        try:
            spreadsheet_body = {'properties': {'title': file_name}}
            spreadsheet = self.sheets_service.spreadsheets().create(body=spreadsheet_body).execute()
            spreadsheet_id = spreadsheet['spreadsheetId']
            
            file = self.drive_service.files().get(fileId=spreadsheet_id, fields='parents').execute()
            previous_parents = ",".join(file.get('parents', []))
            self.drive_service.files().update(
                fileId=spreadsheet_id,
                addParents=folder_id,
                removeParents=previous_parents,
                fields='id, parents'
            ).execute()
            
            logger.info(f"Created new file '{file_name}' with ID: {spreadsheet_id}")
            return spreadsheet_id
        except Exception as e:
            logger.error(f"Error creating spreadsheet '{file_name}': {e}")
            return None
    
    def read_spreadsheet_data(self, spreadsheet_id, range_name):
        """Read data from a Google Spreadsheet."""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=range_name).execute()
            values = result.get('values', [])
            logger.info(f"Read {len(values)} rows from {range_name}")
            
            # Ensure each row has at least 7 columns
            for row in values:
                while len(row) < 7:
                    row.append('')
            return values
        except Exception as e:
            logger.error(f"Error reading data from {range_name}: {e}")
            return []
    
    def write_spreadsheet_data(self, spreadsheet_id, range_name, values):
        """Write data to a Google Spreadsheet."""
        try:
            body = {'values': values}
            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            logger.info(f"Wrote {result.get('updatedCells')} cells to {range_name}")
            return result
        except Exception as e:
            logger.error(f"Error writing data to {range_name}: {e}")
            return None
    
    def get_or_create_result_file(self, folder_id, file_name):
        """Find or create a spreadsheet in the folder."""
        file_id = self.find_file_in_folder(folder_id, file_name)
        if not file_id:
            logger.info(f"Creating new file '{file_name}'...")
            file_id = self.create_spreadsheet(folder_id, file_name)
        return file_id

class DataFrameProcessor:
    """Handles DataFrame processing and formatting for Google Sheets."""
    
    @staticmethod
    def extract_date(series, date_format='%Y-%m-%d'):
        """Extract and standardize date from a series, handling multiple formats."""
        def parse_date(x):
            if pd.isna(x) or x == '':
                return pd.NaT
            if isinstance(x, str):
                # Handle ISO 8601 format (e.g., 2025-05-11T19:50:53Z)
                if 'T' in x:
                    return pd.to_datetime(x, errors='coerce').date()
                # Handle DD-MM-YYYY format (e.g., 20-03-2025)
                if '-' in x and x.count('-') == 2:
                    try:
                        return pd.to_datetime(x, format='%d-%m-%Y', errors='coerce').date()
                    except:
                        pass
            # Fallback to general parsing
            return pd.to_datetime(x, errors='coerce').date()
        
        return pd.to_datetime(series.apply(parse_date), errors='coerce')
    
    @staticmethod
    def filter_by_date(df, date_column, milestone, filter_enabled=True, field = "Source Name"):
        """Filter DataFrame by date, starting from milestone."""
        if not filter_enabled:
            return df
        df = df.copy()
        df['Date_Obj'] = pd.to_datetime(df[date_column], errors='coerce')
        if field == "Source Name":
            filtered_df = df[df['Date_Obj'] >= pd.to_datetime(milestone)].drop('Date_Obj', axis=1)
            logger.info(f"Filtered data from {milestone}, remaining rows: {len(filtered_df)}")
        else:
            filtered_df = df[df['Date_Obj'] < pd.to_datetime(milestone)].drop('Date_Obj', axis=1)
            logger.info(f"Filtered data before {milestone}, remaining rows: {len(filtered_df)}")
        return filtered_df
    
    @staticmethod
    def create_pivot_table(df, index, columns, values, aggfunc='count', fill_value=0):
        """Create a pivot table from the DataFrame."""
        return pd.pivot_table(
            df, index=index, columns=columns, values=values,
            aggfunc=aggfunc, fill_value=fill_value
        )
    
    @staticmethod
    def sort_columns(pivot_table, sort_key_func):
        """Sort pivot table columns using the provided key function."""
        if pivot_table.columns.size > 0:
            try:
                sorted_columns = sorted(pivot_table.columns, key=sort_key_func)
                return pivot_table[sorted_columns]
            except Exception as e:
                logger.warning(f"Could not sort columns: {e}")
        return pivot_table
    
    @staticmethod
    def format_for_sheets(pivot_table, index_name, date_format=None):
        """Format pivot table for Google Sheets, ensuring all Timestamp objects are converted to strings."""
        result_df = pivot_table.reset_index()
        
        # Convert index column (e.g., Source Name or Registration Date) to string if datetime
        if pd.api.types.is_datetime64_any_dtype(result_df[index_name]):
            result_df[index_name] = result_df[index_name].dt.strftime(date_format or '%Y-%m-%d')
        
        # Convert all datetime columns in result_df to strings
        for col in result_df.columns:
            if pd.api.types.is_datetime64_any_dtype(result_df[col]):
                result_df[col] = result_df[col].dt.strftime(date_format or '%Y-%m-%d')
        
        # Handle NaT or NaN values
        result_df = result_df.fillna('')
        
        # Convert pivot table column names (dates) to strings
        formatted_columns = [
            col.strftime(date_format or '%Y-%m-%d') if isinstance(col, pd.Timestamp)
            else str(col)
            for col in pivot_table.columns
        ]
        
        header = [index_name] + formatted_columns
        result_data = [
            [
                val.strftime(date_format or '%Y-%m-%d') if isinstance(val, pd.Timestamp)
                else '' if pd.isna(val)
                else val.item() if hasattr(val, 'item')
                else val
                for val in row
            ] for row in result_df.values
        ]
        
        totals = ['Total'] + [int(pivot_table[col].sum()) for col in pivot_table.columns]
        return [header] + result_data + [totals]
    
    @staticmethod
    def simple_count(df, column, count_column='Count'):
        """Count occurrences of values in a column."""
        counts = df[column].value_counts().reset_index()
        counts.columns = [column, count_column]
        total = pd.DataFrame([[f'Total', counts[count_column].sum()]], columns=[column, count_column])
        counts = pd.concat([counts, total], ignore_index=True)
        
        header = counts.columns.tolist()
        result_data = [
            [val.item() if hasattr(val, 'item') else val for val in row]
            for row in counts.values
        ]
        return [header] + result_data

class DriveDataProcessor:
    """Processes data from Google Drive spreadsheets and generates reports."""
    
    def __init__(self, folder_id, data_spreadsheet_name = 'data', filter_date=True):
        """Initialize processor with Google services and data."""
        self.google_service = GoogleServiceManager(CONFIG['service_account_file'], CONFIG['scopes'])
        self.folder_id = folder_id
        self.filter_date = filter_date
        self.user_register_dataframe = self._get_user_register_dataframe(data_speadsheet_name)
        self.videos_dataframe = self._get_videos_dataframe(data_speadsheet_name)
        logger.info("DriveDataProcessor initialized successfully")
    
    def _get_user_register_dataframe(self, data_spreadsheet_name):
        """Load user register data from the 'User Register' sheet."""
        spreadsheet_id = self.google_service.find_file_in_folder(self.folder_id, data_spreadsheet_name)
        if not spreadsheet_id:
            logger.error(f"Could not find '{data_spreadsheet_name}' spreadsheet")
            return None
        
        data = self.google_service.read_spreadsheet_data(spreadsheet_id, CONFIG['user_register_sheet'])
        if not data or len(data) <= 1:
            logger.warning(f"No data in '{CONFIG['user_register_sheet']}' sheet")
            return None
        
        df = self._to_dataframe(data)
        if df is None:
            return None
        
        logger.info(f"Loaded {len(df)} rows from User Register sheet")
        return df
    
    def _get_videos_dataframe(self, data_spreadsheet_name):
        """Load video data from all relevant sheets except 'User Register'."""
        spreadsheet_id = self.google_service.find_file_in_folder(self.folder_id, data_spreadsheet_name)
        if not spreadsheet_id:
            logger.error(f"Could not find '{data_spreadsheet_name}' spreadsheet")
            return None
        
        sheets_metadata = self.google_service.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_names = [sheet['properties']['title'] for sheet in sheets_metadata.get('sheets', [])]
        
        df_dict = {}
        for sheet_name in sheet_names:
            if sheet_name == CONFIG['user_register_sheet']:
                continue
            data = self.google_service.read_spreadsheet_data(spreadsheet_id, sheet_name)
            if data and len(data) > 1:
                df = self._to_dataframe(data)
                if df is not None:
                    df_dict[sheet_name] = df
                    logger.info(f"Loaded {len(df)} rows from sheet '{sheet_name}'")
        
        if not df_dict:
            logger.warning("No valid sheets found with data")
            return None
        
        sheet_dfs = {
            name: df for name, df in df_dict.items()
            if any(char.isdigit() for char in name)
        } or {name: df for name, df in df_dict.items()}
        
        if not sheet_dfs:
            logger.warning("No valid sheets found")
            return None
        
        combined_df = pd.concat(
            [df.assign(SheetName=name) for name, df in sheet_dfs.items()],
            axis=0, ignore_index=True
        )
        logger.info(f"Combined {len(combined_df)} rows into videos_dataframe")
        return combined_df
    
    def _to_dataframe(self, data):
        """Convert raw sheet data to a DataFrame with standardized Created At column."""
        if not data or len(data) <= 1:
            logger.warning("Empty data or only header")
            return None
        
        df = pd.DataFrame(data[1:], columns=data[0])
        if 'Created At' in df.columns:
            df['Created At'] = DataFrameProcessor.extract_date(df['Created At'])
            invalid_dates = df['Created At'].isna().sum()
            if invalid_dates > 0:
                logger.warning(f"Found {invalid_dates} invalid 'Created At' values")
        return df
    
    def _find_column(self, df, column_key):
        """Find a column by its key in the configuration."""
        if df is None:
            return None
        for col in df.columns:
            if CONFIG['column_mappings'][column_key](col):
                return col
        return None
    
    def _prepare_combined_df(self, required_columns):
        """Prepare the combined DataFrame with required columns and filters."""
        if self.videos_dataframe is None:
            logger.error("videos_dataframe is not initialized")
            return None
        
        combined_df = self.videos_dataframe
        for column in required_columns:
            if column not in combined_df.columns:
                logger.error(f"Required column '{column}' not found")
                return None
        
        combined_df = combined_df[combined_df['ID'].notna()].copy()
        logger.info(f"Filtered non-null ID, remaining rows: {len(combined_df)}")
        return combined_df
    
    def _generate_pivot_sheet(self, df, index_col, value_col='ID', output_spreadsheet_name=None, 
                             default_sheet_name='', sort_index_desc=False):
        """Generate a pivot table and write it to a Google Sheet."""
        if df is None or index_col not in df.columns or value_col not in df.columns or 'SheetName' not in df.columns:
            logger.error("Invalid DataFrame or missing required columns")
            return None
        
        pivot_table = DataFrameProcessor.create_pivot_table(
            df, index=index_col, columns='SheetName', values=value_col
        )
        pivot_table = DataFrameProcessor.sort_columns(pivot_table, self._extract_number)
        
        if sort_index_desc:
            pivot_table = pivot_table.sort_index(ascending=False)
        
        sheet_data = DataFrameProcessor.format_for_sheets(
            pivot_table, index_col, date_format=CONFIG['date_format'] if index_col == 'Registration Date' else None
        )
        
        output_spreadsheet_name = output_spreadsheet_name or default_sheet_name or 'Pivot_Sheet_Default'
        output_id = self.google_service.get_or_create_result_file(self.folder_id, output_spreadsheet_name)
        if not output_id:
            logger.error(f"Failed to create or find output spreadsheet '{output_spreadsheet_name}'")
            return None
        
        self.google_service.write_spreadsheet_data(output_id, 'Sheet1', sheet_data)
        logger.info(f"Created pivot sheet in '{output_spreadsheet_name}'")
        return output_id
    
    def _extract_number(self, sheet_name):
        """Extract number from sheet name for sorting."""
        try:
            number_part = sheet_name.split('.')[0].strip()
            return int(number_part)
        except:
            return 999
    
    def count_daily_registers_by_source_name(self, output_spreadsheet_name=None):
        """Count daily user registrations by source name."""
        user_df = self.user_register_dataframe
        if user_df is None:
            logger.error("user_register_dataframe is not initialized")
            return None
        
        source_name_col = self._find_column(user_df, 'source_name')
        created_at_col = self._find_column(user_df, 'created_at')
        if not source_name_col or not created_at_col:
            logger.error("Required columns 'Source Name' or 'Created At' not found")
            return None
        
        user_df = user_df.copy()
        user_df['Registration Date'] = DataFrameProcessor.extract_date(user_df[created_at_col])
        user_df = DataFrameProcessor.filter_by_date(
            user_df, 'Registration Date', CONFIG['start_date_filter'], self.filter_date, "Source Name"
        )
        
        if user_df.empty:
            logger.warning("No data after filtering")
            return None
        
        user_df['count'] = 1
        pivot_table = DataFrameProcessor.create_pivot_table(
            user_df, index=source_name_col, columns='Registration Date', values='count', aggfunc='sum'
        )
        
        # logger.info(f"Pivot table columns: {list(pivot_table.columns)}")
        sheet_data = DataFrameProcessor.format_for_sheets(
            pivot_table, source_name_col, date_format=CONFIG['date_format']
        )
        
        output_spreadsheet_name = output_spreadsheet_name or "Daily_Registers_by_Source_Name"
        output_id = self.google_service.get_or_create_result_file(self.folder_id, output_spreadsheet_name)
        if not output_id:
            logger.error(f"Failed to create or find output spreadsheet '{output_spreadsheet_name}'")
            return None
        
        self.google_service.write_spreadsheet_data(output_id, 'Sheet1', sheet_data)
        logger.info(f"Created daily source count in '{output_spreadsheet_name}'")
        return output_id
    
    def count_daily_registers_by_ref(self, output_spreadsheet_name=None):
        """Count daily user registrations by referral source."""
        user_df = self.user_register_dataframe
        if user_df is None:
            logger.error("user_register_dataframe is not initialized")
            return None
        
        ref_by_col = self._find_column(user_df, 'ref_by')
        created_at_col = self._find_column(user_df, 'created_at')
        if not ref_by_col or not created_at_col:
            logger.error("Required columns 'Ref By' or 'Created At' not found")
            return None
        
        user_df = user_df.copy()
        user_df['Registration Date'] = DataFrameProcessor.extract_date(user_df[created_at_col])
        user_df = DataFrameProcessor.filter_by_date(
            user_df, 'Registration Date', CONFIG['start_date_filter'], self.filter_date, "Ref By"
        )
        
        if user_df.empty:
            logger.warning("No data after filtering")
            return None
        
        user_df[ref_by_col] = user_df[ref_by_col].fillna('direct').replace('', 'direct')
        user_df['count'] = 1
        pivot_table = DataFrameProcessor.create_pivot_table(
            user_df, index=ref_by_col, columns='Registration Date', values='count', aggfunc='sum'
        )
        
        # logger.info(f"Pivot table columns: {list(pivot_table.columns)}")
        sheet_data = DataFrameProcessor.format_for_sheets(
            pivot_table, ref_by_col, date_format=CONFIG['date_format']
        )
        
        output_spreadsheet_name = output_spreadsheet_name or "Daily_Registers_by_Ref"
        output_id = self.google_service.get_or_create_result_file(self.folder_id, output_spreadsheet_name)
        if not output_id:
            logger.error(f"Failed to create or find output spreadsheet '{output_spreadsheet_name}'")
            return None
        
        self.google_service.write_spreadsheet_data(output_id, 'Sheet1', sheet_data)
        logger.info(f"Created daily referral count in '{output_spreadsheet_name}'")
        return output_id

    def count_users_by_source_name(self, output_spreadsheet_name=None):
        """Count total users by source name."""
        user_df = self.user_register_dataframe
        if user_df is None:
            logger.error("user_register_dataframe is not initialized")
            return None
        
        source_name_col = self._find_column(user_df, 'source_name')
        created_at_col = self._find_column(user_df, 'created_at')
        if not source_name_col or not created_at_col:
            logger.error("Required columns 'Source Name' or 'Created At' not found")
            return None
        
        user_df = user_df.copy()
        user_df['Registration Date'] = DataFrameProcessor.extract_date(user_df[created_at_col])
        user_df = DataFrameProcessor.filter_by_date(
            user_df, 'Registration Date', CONFIG['start_date_filter'], self.filter_date, "Source Name"
        )
        
        if user_df.empty:
            logger.warning("No data after filtering")
            return None
        
        sheet_data = DataFrameProcessor.simple_count(user_df, source_name_col)
        
        output_spreadsheet_name = output_spreadsheet_name or "Users_by_Source_Name"
        output_id = self.google_service.get_or_create_result_file(self.folder_id, output_spreadsheet_name)
        if not output_id:
            logger.error(f"Failed to create or find output spreadsheet '{output_spreadsheet_name}'")
            return None
        
        self.google_service.write_spreadsheet_data(output_id, 'Sheet1', sheet_data)
        logger.info(f"Created source name count in '{output_spreadsheet_name}'")
        return output_id

    def count_users_by_ref(self, output_spreadsheet_name=None):
        """Count total users by referral source."""
        user_df = self.user_register_dataframe
        if user_df is None:
            logger.error("user_register_dataframe is not initialized")
            return None
        
        ref_by_col = self._find_column(user_df, 'ref_by')
        created_at_col = self._find_column(user_df, 'created_at')
        if not ref_by_col or not created_at_col:
            logger.error("Required columns 'Ref By' or 'Created At' not found")
            return None
        
        user_df = user_df.copy()
        user_df['Registration Date'] = DataFrameProcessor.extract_date(user_df[created_at_col])
        user_df = DataFrameProcessor.filter_by_date(
            user_df, 'Registration Date', CONFIG['start_date_filter'], self.filter_date, "Ref By"
        )
        
        if user_df.empty:
            logger.warning("No data after filtering")
            return None
        
        user_df[ref_by_col] = user_df[ref_by_col].fillna('direct').replace('', 'direct')
        sheet_data = DataFrameProcessor.simple_count(user_df, ref_by_col, 'User Count')
        
        output_spreadsheet_name = output_spreadsheet_name or "Users_by_Ref"
        output_id = self.google_service.get_or_create_result_file(self.folder_id, output_spreadsheet_name)
        if not output_id:
            logger.error(f"Failed to create or find output spreadsheet '{output_spreadsheet_name}'")
            return None
        
        self.google_service.write_spreadsheet_data(output_id, 'Sheet1', sheet_data)
        logger.info(f"Created referrer count in '{output_spreadsheet_name}'")
        return output_id
    
    def count_users_each_sheet_by_source_name(self, output_spreadsheet_name=None):
        """Count users by source name across sheets."""
        required_columns = ['ID', 'Source Name', 'Created At', 'SheetName']
        df = self._prepare_combined_df(required_columns)
        if df is None:
            return None
        
        df = df.dropna(subset=['Source Name'])
        logger.info(f"Filtered non-null Source Name, remaining rows: {len(df)}")
        
        return self._generate_pivot_sheet(
            df, 'Source Name', output_spreadsheet_name=output_spreadsheet_name,
            default_sheet_name="Users_Each_Sheet_by_Source_Name"
        )
    
    def count_users_each_sheet_by_ref(self, output_spreadsheet_name=None):
        """Count users by referral source across sheets."""
        required_columns = ['ID', 'Ref By', 'Created At', 'SheetName']
        df = self._prepare_combined_df(required_columns)
        if df is None:
            return None
        
        if 'Ref By' not in df.columns:
            logger.warning("'Ref By' column not found")
            return None
        
        df['Ref By'] = df['Ref By'].fillna('direct').replace('', 'direct')
        logger.info(f"Filtered non-null ID and set default 'Ref By', remaining rows: {len(df)}")
        
        return self._generate_pivot_sheet(
            df, 'Ref By', output_spreadsheet_name=output_spreadsheet_name,
            default_sheet_name="Users_Each_Sheet_by_Ref"
        )
    
    def count_users_each_sheet_by_date(self, output_spreadsheet_name=None):
        """Count users by date across sheets."""
        required_columns = ['ID', 'Created At', 'SheetName']
        df = self._prepare_combined_df(required_columns)
        if df is None:
            return None
        
        df = df[df['Created At'].notna()].copy()
        df['Registration Date'] = DataFrameProcessor.extract_date(df['Created At'])
        
        if df['Registration Date'].isna().all():
            logger.warning("No valid dates after processing")
            return None
        
        logger.info(f"Processed dates, remaining rows: {len(df)}")
        return self._generate_pivot_sheet(
            df, 'Registration Date', output_spreadsheet_name=output_spreadsheet_name,
            default_sheet_name="Users_Each_Sheet_by_Date", sort_index_desc=True
        )


def process(folder_id, data_speadsheet_name = 'data', filter_date=True):
    processor = DriveDataProcessor(folder_id=folder_id,  filter_date=filter_date)
    processor.count_daily_registers_by_source_name(output_spreadsheet_name='daily_registers_by_source_name')
    processor.count_daily_registers_by_ref(output_spreadsheet_name='daily_registers_by_ref')
    processor.count_users_by_ref(output_spreadsheet_name="users_by_ref")
    processor.count_users_by_source_name(output_spreadsheet_name="users_by_source_name")
    processor.count_users_each_sheet_by_ref(output_spreadsheet_name="users_each_sheet_by_ref")
    processor.count_users_each_sheet_by_source_name(output_spreadsheet_name="users_each_sheet_by_source_name")
    processor.count_users_each_sheet_by_date(output_spreadsheet_name="users_each_sheet_by_date")
    

if __name__ == '__main__':
    folder_id = '1OAgBQotTPAhSnreHt3rR0VW61j-k6_1g'
    data_speadsheet_name = 'data1'
    filter_date = False
    process(folder_id, data_speadsheet_name, filter_date)