import time
import pandas as pd
import logging
from datetime import date, datetime
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

# Google Drive folder path
FOLDER_ID = '1OAgBQotTPAhSnreHt3rR0VW61j-k6_1g'

# Required access scopes
SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/spreadsheets']

class DriveDataProcessor:
    def __init__(self, folder_id, filter_date = True):
        """Initialize processor with Google Drive and Sheets connections."""
        self.drive_service, self.sheets_service = self.authenticate_google_services()
        self.folder_id = folder_id
        self.filter_date = filter_date
        self.user_register_dataframe = self.get_user_register_dataframe()
        self.videos_dataframe = self.get_videos_dateframe()
        if not self.drive_service or not self.sheets_service:
            logger.error("Cannot authenticate with Google API. Please check service_account.json file")
            raise Exception("Google API authentication error")
        logger.info("DriveDataProcessor initialized successfully")
    
    def get_videos_dateframe(self):
        try:
            # Step 1: Find the 'data' spreadsheet
            data_spreadsheet_id = self.find_file_in_folder('data')
            if not data_spreadsheet_id:
                logger.error("Could not find 'data' spreadsheet")
                return None
                
            # Step 2: Get all sheet names from the spreadsheet
            sheets_metadata = self.sheets_service.spreadsheets().get(
                spreadsheetId=data_spreadsheet_id
            ).execute()
            
            sheet_names = [sheet['properties']['title'] for sheet in sheets_metadata.get('sheets', [])]
            
            # Step 3: Read data from each sheet and store in a dictionary
            df_dict = {}
            for sheet_name in sheet_names:
                sheet_data = self.read_spreadsheet_data(data_spreadsheet_id, sheet_name)
                if sheet_data and len(sheet_data) > 1:  # Has header and at least one row
                    df = self.convert_to_dataframe(sheet_data)
                    if df is not None:
                        df_dict[sheet_name] = df
            
            if not df_dict:
                logger.warning("No valid sheets found with data")
                return None
                
            # Step 4: Filter sheets with course content (numbered sheets)
            # Exclude 'User Register' sheet as it's not part of the course content
            sheet_dfs = {sheet_name: df for sheet_name, df in df_dict.items() 
                        if sheet_name != 'User Register' and any(char.isdigit() for char in sheet_name)}
            
            if not sheet_dfs:
                # Try all sheets except User Register if no numbered sheets found
                sheet_dfs = {sheet_name: df for sheet_name, df in df_dict.items() 
                            if sheet_name != 'User Register'}
                
            if not sheet_dfs:
                logger.warning("No valid sheets found")
                return None
            
            # Step 5: Combine DataFrames with SheetName column
            combined_df = pd.concat(
                [df.assign(SheetName=sheet_name) for sheet_name, df in sheet_dfs.items()],
                axis=0,
                ignore_index=True
            )
            
            return combined_df
        except Exception as e:
            logger.error(f"Error getting video data: {e}")
            return None

    def get_user_register_dataframe(self):
        try:
            # Step 1: Find the 'data' spreadsheet
            data_spreadsheet_id = self.find_file_in_folder('data')
            if not data_spreadsheet_id:
                logger.error("Could not find 'data' spreadsheet")
                return None
                
            # Step 2: Read user data from 'User Register' sheet
            user_data = self.read_spreadsheet_data(data_spreadsheet_id, 'User Register')
            if not user_data or len(user_data) <= 1:
                logger.warning("No user data available to process in 'User Register' sheet")
                return None
            
            user_df = self.convert_to_dataframe(user_data)
            if user_df is None:
                return None
            
            return user_df
        except Exception as e:
            logger.error(f"Error getting user register data: {e}")
            return None

    def authenticate_google_services(self):
        """Authenticate with Google API and return Drive and Sheets services."""
        try:
            # Get credentials from service_account.json file
            creds = Credentials.from_service_account_file('service_account.json', scopes=SCOPES)
            
            # Create Drive and Sheets services
            drive_service = build('drive', 'v3', credentials=creds)
            sheets_service = build('sheets', 'v4', credentials=creds)
            
            logger.info("Google API authentication successful")
            return drive_service, sheets_service
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return None, None

    def find_file_in_folder(self, file_name):
        """Find file in folder with specific name."""
        try:
            query = f"'{self.folder_id}' in parents and name = '{file_name}' and trashed = false"
            results = self.drive_service.files().list(
                q=query,
                fields="files(id, name)"
            ).execute()
            files = results.get('files', [])
            
            if files:
                logger.info(f"Found file '{file_name}' with ID: {files[0]['id']}")
                return files[0]['id']
            else:
                logger.info(f"File '{file_name}' not found in folder")
                return None
        except Exception as e:
            logger.error(f"Error finding file '{file_name}': {e}")
            return None

    def create_spreadsheet(self, file_name):
        """Create a new Google Spreadsheet in the folder."""
        try:
            # Create empty file
            spreadsheet_body = {
                'properties': {
                    'title': file_name
                }
            }
            spreadsheet = self.sheets_service.spreadsheets().create(body=spreadsheet_body).execute()
            spreadsheet_id = spreadsheet['spreadsheetId']
            
            # Move file to specified folder
            file = self.drive_service.files().get(fileId=spreadsheet_id, fields='parents').execute()
            previous_parents = ",".join(file.get('parents', []))
            
            self.drive_service.files().update(
                fileId=spreadsheet_id,
                addParents=FOLDER_ID,
                removeParents=previous_parents,
                fields='id, parents'
            ).execute()
            
            logger.info(f"Created new file '{file_name}' with ID: {spreadsheet_id}")
            return spreadsheet_id
        except Exception as e:
            logger.error(f"Error creating spreadsheet '{file_name}': {e}")
            return None

    def read_spreadsheet_data(self, spreadsheet_id, range_name):
        """Read data from Google Spreadsheet."""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            logger.info(f"Successfully read {len(values)} rows from {range_name}")

            for row in values:
                if len(row) < 7:
                    row.append('')

            return values
        except Exception as e:
            logger.error(f"Error reading data from {range_name}: {e}")
            return []

    def write_spreadsheet_data(self, spreadsheet_id, range_name, values):
        """Write data to Google Spreadsheet."""
        try:
            body = {
                'values': values
            }
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

    def get_or_create_result_file(self, file_name):
        """Find or create result file."""
        file_id = self.find_file_in_folder(file_name)
        if not file_id:
            logger.info(f"Creating new file '{file_name}'...")
            file_id = self.create_spreadsheet(file_name)
            if not file_id:
                logger.error(f"Cannot create file '{file_name}'")
                return None
        return file_id

    def convert_to_dataframe(self, data):
        """Convert sheet data to DataFrame."""
        if not data or len(data) <= 1:
            logger.warning("Empty data or only header")
            return None
        
        headers = data[0]
        df = pd.DataFrame(data[1:], columns=headers)
        return df

    def format_for_sheets(self, df):
        """Format DataFrame for writing to Google Sheets."""
        # Convert DataFrame to list of lists
        if df is None or df.empty:
            return [["No data available"]]
        
        # Get headers and records
        headers = df.columns.tolist()
        records = df.values.tolist()
        
        # Kết hợp headers và records
        result = [headers] + records
        return result

    def count_daily_registers_by_source_name(self, output_spreadsheet_name=None):
        """
        Create a table that counts users registered per day from each source.
        Reads data from the spreadsheet 'data', sheet 'User Register'.
        
        Args:
            output_spreadsheet_name: Name for the output spreadsheet (if None, will use default name)
        
        Returns:
            The ID of the created/updated spreadsheet with count statistics
        """
        try:
            user_df = self.user_register_dataframe
            if user_df is None:
                return None
                
            # Step 3: Find the Source Name and Created At columns
            source_name_col = None
            created_at_col = None
            
            for col in user_df.columns:
                if 'source' in col.lower() and 'name' in col.lower():
                    source_name_col = col
                if 'creat' in col.lower() and 'at' in col.lower():
                    created_at_col = col
            
            if not source_name_col or not created_at_col:
                logger.error("Required columns 'Source Name' and 'Created At' not found")
                return None
                
            # Step 4: Extract just the date part from the datetime string
            # Example format: 2025-05-05T07:56:26Z
            user_df['Registration Date'] = user_df[created_at_col].apply(
                lambda x: x.split('T')[0] if isinstance(x, str) and 'T' in x else x
            )
            
            # Step 4.5: Filter for dates from April 15, 2025 onwards (no upper limit)
            
            # Define the start date (April 15, 2025)
            start_date = '2025-04-15'
            
            # Convert Registration Date to datetime for comparison
            user_df['Date_Obj'] = pd.to_datetime(user_df['Registration Date'], errors='coerce')
            
            # Filter the DataFrame to include only rows with dates from April 15, 2025 onwards
            if self.filter_date:
                user_df = user_df[user_df['Date_Obj'] >= pd.to_datetime(start_date)]
            
            logger.info(f"Filtered data from {start_date} onwards, remaining rows: {len(user_df)}")
            
            # Drop the helper column
            user_df = user_df.drop('Date_Obj', axis=1)
            
            # Check if we have any data left after filtering
            if len(user_df) == 0:
                logger.warning("No data available after date filtering")
                return None
            
            # Step 5: Group by date and source name and count
            
            # Create a count column (each row is one user)
            user_df['count'] = 1
            
            # Create pivot table with dates as columns and source names as rows
            pivot_df = pd.pivot_table(
                user_df, 
                values='count',
                index=[source_name_col],
                columns=['Registration Date'],
                aggfunc='sum',
                fill_value=0
            )
            
            # Convert the pivot table to a regular DataFrame
            result_df = pivot_df.reset_index()
            
            # Step 6: Add a 'total' row at the bottom
            # First, calculate column sums excluding the source name column
            totals = ['total']
            for col in pivot_df.columns:
                # Convert NumPy int64 to standard Python int
                totals.append(int(pivot_df[col].sum()))
            
            # Convert to list of lists for Google Sheets and ensure all values are JSON serializable
            result_data = []
            for row in result_df.values:
                # Convert each value in the row to standard Python types
                converted_row = []
                for val in row:
                    if hasattr(val, 'item'):  # Check if it's a NumPy type with item() method
                        converted_row.append(val.item())
                    else:
                        converted_row.append(val)
                result_data.append(converted_row)
            
            # Create header row with Source Name and all dates
            header = [source_name_col] + [str(col) for col in pivot_df.columns]
            
            # Final data for sheet
            sheet_data = [header] + result_data + [totals]
            
            output_id = self.get_or_create_result_file(output_spreadsheet_name)
            if not output_id:
                logger.error("Failed to create or find output spreadsheet")
                return None
                
            # Step 8: Write data to the spreadsheet
            self.write_spreadsheet_data(output_id, 'Sheet1', sheet_data)
            
            logger.info(f"Successfully created daily source count in '{output_spreadsheet_name}'")
            return output_id
            
        except Exception as e:
            logger.error(f"Error creating daily source count: {e}")
            return None
        
    def count_daily_registers_by_ref(self, output_spreadsheet_name=None):
        """
        Create a table that counts users registered per day, grouped by referral source (Ref By).
        Reads data from the spreadsheet 'data', sheet 'User Register'.
        
        Args:
            output_spreadsheet_name: Name for the output spreadsheet (if None, will use default name)
        
        Returns:
            The ID of the created/updated spreadsheet with count statistics
        """
        try:    
            user_df = self.user_register_dataframe
            if user_df is None:
                return None
                
            # Step 3: Find the Ref By and Created At columns
            ref_by_col = None
            created_at_col = None
            
            for col in user_df.columns:
                if 'ref' in col.lower() and 'by' in col.lower():
                    ref_by_col = col
                if 'creat' in col.lower() and 'at' in col.lower():
                    created_at_col = col
            
            if not ref_by_col or not created_at_col:
                logger.error("Required columns 'Ref By' and 'Created At' not found")
                return None
                
            # Step 4: Extract just the date part from the datetime string
            # Example format: 2025-05-05T07:56:26Z
            user_df['Registration Date'] = user_df[created_at_col].apply(
                lambda x: x.split('T')[0] if isinstance(x, str) and 'T' in x else x
            )
            
            # Step 5: Filter for dates from April 15, 2025 onwards (no upper limit)
            import datetime
            import pandas as pd
            
            # Define the start date (April 15, 2025)
            start_date = '2025-04-15'
            
            # Convert Registration Date to datetime for comparison
            user_df['Date_Obj'] = pd.to_datetime(user_df['Registration Date'], errors='coerce')
            
            # Filter the DataFrame to include only rows with dates from April 15, 2025 onwards
            if self.filter_date:
                user_df = user_df[user_df['Date_Obj'] < pd.to_datetime(start_date)]
            
            logger.info(f"Filtered data from {start_date} onwards, remaining rows: {len(user_df)}")
            
            # Drop the helper column
            user_df = user_df.drop('Date_Obj', axis=1)
            
            # Check if we have any data left after filtering
            if len(user_df) == 0:
                logger.warning("No data available after date filtering")
                return None
                
            # Step 6: Handle empty or null referrals
            # Replace empty or null values in Ref By column with "direct" or another appropriate label
            user_df[ref_by_col] = user_df[ref_by_col].fillna('direct')
            user_df.loc[user_df[ref_by_col] == '', ref_by_col] = 'direct'
            
            # Step 7: Group by date and referral source and count
            # Create a count column (each row is one user)
            user_df['count'] = 1
            
            # Create pivot table with dates as columns and referral sources as rows
            pivot_df = pd.pivot_table(
                user_df, 
                values='count',
                index=[ref_by_col],
                columns=['Registration Date'],
                aggfunc='sum',
                fill_value=0
            )
            
            # Convert the pivot table to a regular DataFrame
            result_df = pivot_df.reset_index()
            
            # Step 8: Add a 'total' row at the bottom
            # First, calculate column sums excluding the ref by column
            totals = ['total']
            for col in pivot_df.columns:
                # Convert NumPy int64 to standard Python int
                totals.append(int(pivot_df[col].sum()))
            
            # Convert to list of lists for Google Sheets and ensure all values are JSON serializable
            result_data = []
            for row in result_df.values:
                # Convert each value in the row to standard Python types
                converted_row = []
                for val in row:
                    if hasattr(val, 'item'):  # Check if it's a NumPy type with item() method
                        converted_row.append(val.item())
                    else:
                        converted_row.append(val)
                result_data.append(converted_row)
            
            # Create header row with Ref By and all dates
            header = [ref_by_col] + [str(col) for col in pivot_df.columns]
            
            # Final data for sheet
            sheet_data = [header] + result_data + [totals]
            
            # Step 9: Create or update the output spreadsheet
            if not output_spreadsheet_name:
                output_spreadsheet_name = "Daily_User_Registration_by_Referral"
            
            output_id = self.get_or_create_result_file(output_spreadsheet_name)
            if not output_id:
                logger.error("Failed to create or find output spreadsheet")
                return None
                
            # Step 10: Write data to the spreadsheet
            self.write_spreadsheet_data(output_id, 'Sheet1', sheet_data)
            
            logger.info(f"Successfully created daily referral count in '{output_spreadsheet_name}'")
            return output_id
            
        except Exception as e:
            logger.error(f"Error creating daily referral count: {e}")
            return None

    def count_users_by_source_name(self, output_spreadsheet_name=None):
        """
        Create a simple table that counts total users grouped by source name.
        Reads data from the spreadsheet 'data', sheet 'User Register'.
        
        Args:
            output_spreadsheet_name: Name for the output spreadsheet (if None, will use default name)
        
        Returns:
            The ID of the created/updated spreadsheet with count statistics
        """
        try:    
            user_df = self.user_register_dataframe
            if user_df is None:
                return None
                
            # Step 3: Find the Source Name and Created At columns
            source_name_col = None
            created_at_col = None
            
            for col in user_df.columns:
                if 'source' in col.lower() and 'name' in col.lower():
                    source_name_col = col
                if 'creat' in col.lower() and 'at' in col.lower():
                    created_at_col = col
            
            if not source_name_col or not created_at_col:
                logger.error("Required columns 'Source Name' and 'Created At' not found")
                return None
                
            # Step 4: Extract just the date part from the datetime string and filter by date
            # Example format: 2025-05-05T07:56:26Z
            user_df['Registration Date'] = user_df[created_at_col].apply(
                lambda x: x.split('T')[0] if isinstance(x, str) and 'T' in x else x
            )
            
            # Step 5: Filter for dates from April 15, 2025 onwards
            import pandas as pd
            
            # Define the start date (April 15, 2025)
            start_date = '2025-04-15'
            
            # Convert Registration Date to datetime for comparison
            user_df['Date_Obj'] = pd.to_datetime(user_df['Registration Date'], errors='coerce')
            
            # Filter the DataFrame to include only rows with dates from April 15, 2025 onwards
            if self.filter_date:    
                user_df = user_df[user_df['Date_Obj'] >= pd.to_datetime(start_date)]
            
            logger.info(f"Filtered data from {start_date} onwards, remaining rows: {len(user_df)}")
            
            # Drop the helper columns
            user_df = user_df.drop(['Date_Obj', 'Registration Date'], axis=1)
            
            # Check if we have any data left after filtering
            if len(user_df) == 0:
                logger.warning("No data available after date filtering")
                return None
                
            # Step 6: Count users by source name
            source_counts = user_df[source_name_col].value_counts().reset_index()
            source_counts.columns = ['Source Name', 'Count']
            
            # Step 7: Add total row
            total_users = source_counts['Count'].sum()
            total_row = pd.DataFrame([['Total', total_users]], columns=['Source Name', 'Count'])
            source_counts = pd.concat([source_counts, total_row], ignore_index=True)
            
            # Step 8: Convert to list of lists for Google Sheets and ensure all values are JSON serializable
            header = source_counts.columns.tolist()
            
            result_data = []
            for row in source_counts.values:
                # Convert each value in the row to standard Python types
                converted_row = []
                for val in row:
                    if hasattr(val, 'item'):  # Check if it's a NumPy type with item() method
                        converted_row.append(val.item())
                    else:
                        converted_row.append(val)
                result_data.append(converted_row)
            
            # Final data for sheet
            sheet_data = [header] + result_data
            
            # Step 9: Create or update the output spreadsheet
            if not output_spreadsheet_name:
                output_spreadsheet_name = "User_Count_by_Source"
            
            output_id = self.get_or_create_result_file(output_spreadsheet_name)
            if not output_id:
                logger.error("Failed to create or find output spreadsheet")
                return None
                
            # Step 10: Write data to the spreadsheet
            self.write_spreadsheet_data(output_id, 'Sheet1', sheet_data)
            
            logger.info(f"Successfully created source name count in '{output_spreadsheet_name}'")
            return output_id
            
        except Exception as e:
            logger.error(f"Error creating source name count: {e}")
            return None

    def count_users_by_ref(self, output_spreadsheet_name=None):
        """
        Create a table that counts users registered from each referrer.
        Reads data from the spreadsheet 'data', sheet 'User Register'.
        
        Args:
            output_spreadsheet_name: Name for the output spreadsheet (if None, will use default name)
        
        Returns:
            The ID of the created/updated spreadsheet with count statistics
        """
        try:    
            user_df = self.user_register_dataframe
            if user_df is None:
                return None
                
            # Step 3: Find the Ref By and Created At columns
            ref_by_col = None
            created_at_col = None
            
            for col in user_df.columns:
                if 'ref' in col.lower() and 'by' in col.lower():
                    ref_by_col = col
                if 'creat' in col.lower() and 'at' in col.lower():
                    created_at_col = col
            
            if not ref_by_col or not created_at_col:
                logger.error("Required columns 'Ref By' and 'Created At' not found")
                return None
                
            # Step 4: Extract just the date part from the datetime string
            # Example format: 2025-05-05T07:56:26Z
            user_df['Registration Date'] = user_df[created_at_col].apply(
                lambda x: x.split('T')[0] if isinstance(x, str) and 'T' in x else x
            )
            
            # Step 4.5: Filter for dates from April 15, 2025 onwards (no upper limit)
            
            # Define the start date (April 15, 2025)
            start_date = '2025-04-15'
            
            # Convert Registration Date to datetime for comparison
            user_df['Date_Obj'] = pd.to_datetime(user_df['Registration Date'], errors='coerce')
            
            # Filter the DataFrame to include only rows with dates from April 15, 2025 onwards
            if self.filter_date:    
                user_df = user_df[user_df['Date_Obj'] < pd.to_datetime(start_date)]
            
            logger.info(f"Filtered data from {start_date} onwards, remaining rows: {len(user_df)}")
            
            # Drop the helper column
            user_df = user_df.drop('Date_Obj', axis=1)
            
            # Check if we have any data left after filtering
            if len(user_df) == 0:
                logger.warning("No data available after date filtering")
                return None
            
            # Step 5: Group by referrer and count
            # Replace empty referrers with "direct"
            user_df[ref_by_col] = user_df[ref_by_col].fillna("direct")
            user_df[ref_by_col] = user_df[ref_by_col].replace('', "direct")
            
            # Count users by referrer
            ref_counts = user_df[ref_by_col].value_counts().reset_index()
            ref_counts.columns = [ref_by_col, 'User Count']
            
            # Step 6: Prepare data for Google Sheets
            # Convert to list of lists for Google Sheets and ensure all values are JSON serializable
            result_data = []
            for row in ref_counts.values:
                # Convert each value in the row to standard Python types
                converted_row = []
                for val in row:
                    if hasattr(val, 'item'):  # Check if it's a NumPy type with item() method
                        converted_row.append(val.item())
                    else:
                        converted_row.append(val)
                result_data.append(converted_row)
            
            # Add total row (convert to standard int)
            total_users = int(ref_counts['User Count'].sum())
            result_data.append(["Total", total_users])
            
            # Create header
            header = [ref_by_col, 'User Count']
            
            # Final data for sheet
            sheet_data = [header] + result_data
            
            # Step 7: Create or update the output spreadsheet
            if not output_spreadsheet_name:
                output_spreadsheet_name = "User_Count_by_Referrer"
            
            output_id = self.get_or_create_result_file(output_spreadsheet_name)
            if not output_id:
                logger.error("Failed to create or find output spreadsheet")
                return None
                
            # Step 8: Write data to the spreadsheet
            self.write_spreadsheet_data(output_id, 'Sheet1', sheet_data)
            
            logger.info(f"Successfully created referrer count in '{output_spreadsheet_name}'")
            return output_id
            
        except Exception as e:
            logger.error(f"Error creating referrer count: {e}")
            return None

    def extract_number(self, sheet_name):
        """Extract number from sheet name for sorting."""
        try:
            # Extract the first number from the sheet name (e.g., "1." from "1. Giới thiệu...")
            number_part = sheet_name.split('.')[0].strip()
            return int(number_part)
        except:
            # If extraction fails, return a high number to sort at the end
            return 999

    def count_users_each_sheet_by_source_name(self, output_spreadsheet_name=None):
        """
        Create a table that counts users from each source name across different sheets.
        Reads data from the spreadsheet 'data', all available sheets that contain user data.
        
        Args:
            output_spreadsheet_name: Name for the output spreadsheet (if None, will use default name)
        
        Returns:
            The ID of the created/updated spreadsheet with count statistics
        """
        try:
            combined_df = self.videos_dataframe
            
            # Step 6: Filter data
            required_columns = ['ID', 'Source Name', 'Created At', 'SheetName']
            
            # Check if all required columns exist
            for column in required_columns[:-1]:  # SheetName was added by us
                if column not in combined_df.columns:
                    logger.error(f"Required column '{column}' not found")
                    return None
            
            # Filter rows with valid User ID
            combined_df = combined_df[combined_df['ID'].notna()].copy()
            
            # Filter rows with valid Source Name
            combined_df = combined_df.dropna(subset=['Source Name']).copy()
            
            # Step 7: Create pivot table
            pivot_table = pd.pivot_table(
                combined_df,
                index='Source Name',
                columns='SheetName',
                values='ID',
                aggfunc='count',
                fill_value=0
            )


            
            if pivot_table.columns.size > 0:
                try:
                    sorted_columns = sorted(pivot_table.columns, key=self.extract_number)
                    pivot_table = pivot_table[sorted_columns]
                except Exception as e:
                    logger.warning(f"Could not sort columns: {e}")
            
            # Step 9: Convert pivot table to list format for Google Sheets
            # First, reset index to make Source Name a column
            result_df = pivot_table.reset_index()
            
            # Convert to list of lists for Google Sheets and ensure all values are JSON serializable
            header = ['Source Name'] + list(pivot_table.columns)
            
            result_data = []
            for row in result_df.values:
                # Convert each value in the row to standard Python types
                converted_row = []
                for val in row:
                    if hasattr(val, 'item'):  # Check if it's a NumPy type with item() method
                        converted_row.append(val.item())
                    else:
                        converted_row.append(val)
                result_data.append(converted_row)
                
            # Step 10: Calculate totals for each column
            totals = ['Total']
            for col in pivot_table.columns:
                totals.append(int(pivot_table[col].sum()))
                
            # Step 11: Final data to write to sheet
            sheet_data = [header] + result_data + [totals]
            
            # Step 12: Create or update the output spreadsheet
            if not output_spreadsheet_name:
                output_spreadsheet_name = "User_Count_by_Source_Across_Sheets"
            
            output_id = self.get_or_create_result_file(output_spreadsheet_name)
            if not output_id:
                logger.error("Failed to create or find output spreadsheet")
                return None
                
            # Step 13: Write data to the spreadsheet
            self.write_spreadsheet_data(output_id, 'Sheet1', sheet_data)
            
            logger.info(f"Successfully created source count across sheets in '{output_spreadsheet_name}'")
            return output_id
            
        except Exception as e:
            logger.error(f"Error creating source count across sheets: {e}")
            return None

    def count_users_each_sheet_by_ref(self, output_spreadsheet_name=None):
        """
        Create a table that counts users from each referral source across different sheets.
        Reads data from the spreadsheet 'data', all available sheets that contain user data.
        
        Args:
            output_spreadsheet_name: Name for the output spreadsheet (if None, will use default name)
        
        Returns:
            The ID of the created/updated spreadsheet with count statistics
        """
        try:
            
            combined_df = self.videos_dataframe
            
            # Step 6: Filter data
            required_columns = ['ID', 'Ref By', 'Created At', 'SheetName']
            
            # Check if all required columns exist except 'Ref By' which we'll handle specially
            for column in ['ID', 'Created At']:
                if column not in combined_df.columns:
                    logger.error(f"Required column '{column}' not found")
                    return None
            
            # Check if 'Ref By' column exists, if not check if we can use an alternative column
            if 'Ref By' not in combined_df.columns:
                logger.warning("'Ref By' column not found")
                return None
            
            # Filter rows with valid User ID
            combined_df = combined_df[combined_df['ID'].notna()].copy()
            
            # Fill empty 'Ref By' values with 'direct'
            combined_df.loc[(combined_df['Ref By'] == ''), 'Ref By'] = 'direct'
            combined_df['Ref By'] = combined_df['Ref By'].fillna('direct')
            
            # Step 7: Create pivot table
            pivot_table = pd.pivot_table(
                combined_df,
                index='Ref By',
                columns='SheetName',
                values='ID',
                aggfunc='count',
                fill_value=0
            )
            
            if pivot_table.columns.size > 0:
                try:
                    sorted_columns = sorted(pivot_table.columns, key=self.extract_number)
                    pivot_table = pivot_table[sorted_columns]
                except Exception as e:
                    logger.warning(f"Could not sort columns: {e}")
            
            # Step 9: Convert pivot table to list format for Google Sheets
            # First, reset index to make Ref By a column
            result_df = pivot_table.reset_index()
            
            # Convert to list of lists for Google Sheets and ensure all values are JSON serializable
            header = ['Ref By'] + list(pivot_table.columns)
            
            result_data = []
            for row in result_df.values:
                # Convert each value in the row to standard Python types
                converted_row = []
                for val in row:
                    if hasattr(val, 'item'):  # Check if it's a NumPy type with item() method
                        converted_row.append(val.item())
                    else:
                        converted_row.append(val)
                result_data.append(converted_row)
                
            # Step 10: Calculate totals for each column
            totals = ['Total']
            for col in pivot_table.columns:
                totals.append(int(pivot_table[col].sum()))
                
            # Step 11: Final data to write to sheet
            sheet_data = [header] + result_data + [totals]
            
            # Step 12: Create or update the output spreadsheet
            if not output_spreadsheet_name:
                output_spreadsheet_name = "User_Count_by_Referral_Across_Sheets"
            
            output_id = self.get_or_create_result_file(output_spreadsheet_name)
            if not output_id:
                logger.error("Failed to create or find output spreadsheet")
                return None
                
            # Step 13: Write data to the spreadsheet
            self.write_spreadsheet_data(output_id, 'Sheet1', sheet_data)
            
            logger.info(f"Successfully created referral count across sheets in '{output_spreadsheet_name}'")
            return output_id
            
        except Exception as e:
            logger.error(f"Error creating referral count across sheets: {e}")
            return None

    def count_users_each_sheet_by_date(self, output_spreadsheet_name=None):
        """
        Create a table that counts users for each date across different sheets.
        Reads data from the spreadsheet 'data', all available sheets that contain user data.
        
        Args:
            output_spreadsheet_name: Name for the output spreadsheet (if None, will use default name)
        
        Returns:
            The ID of the created/updated spreadsheet with count statistics
        """
        try:
            
            combined_df = self.videos_dataframe
            
            # Step 6: Filter data
            required_columns = ['ID', 'Created At', 'SheetName']
            
            # Check if required columns exist
            for column in required_columns:
                if column not in combined_df.columns:
                    logger.error(f"Required column '{column}' not found")
                    return None
            
            # Filter rows with valid User ID and Created At
            combined_df = combined_df[combined_df['ID'].notna() & combined_df['Created At'].notna()].copy()
            
            # Check if we have any data left after filtering
            if len(combined_df) == 0:
                logger.warning("No data available after filtering")
                return None
            
            # Step 7: Extract date from 'Created At' column
            # Example format: 2025-05-05T07:56:26Z
            combined_df['Registration Date'] = combined_df['Created At'].apply(
                lambda x: x.split('T')[0] if isinstance(x, str) and 'T' in x else x
            )

            combined_df['Registration Date'] = pd.to_datetime(combined_df['Registration Date'], errors='coerce', dayfirst=True)
            
            # Step 8: Create pivot table with dates as rows and sheets as columns
            pivot_table = pd.pivot_table(
                combined_df,
                index='Registration Date',
                columns='SheetName',
                values='ID',
                aggfunc='count',
                fill_value=0
            )
            
            # Step 9: Sort columns by their course order (1., 2., 3., etc.)
            
            if pivot_table.columns.size > 0:
                try:
                    sorted_columns = sorted(pivot_table.columns, key=self.extract_number)
                    pivot_table = pivot_table[sorted_columns]
                except Exception as e:
                    logger.warning(f"Could not sort columns: {e}")
            
            # Step 10: Sort the index (dates) in descending order (newest first)
            pivot_table = pivot_table.sort_index(ascending=False)
            
            # Step 11: Convert pivot table to list format for Google Sheets
            # First, reset index to make Registration Date a column
            result_df = pivot_table.reset_index()

            result_df['Registration Date'] = result_df['Registration Date'].dt.strftime('%d/%m/%Y')

            
            # Convert to list of lists for Google Sheets and ensure all values are JSON serializable
            header = ['Registration Date'] + list(pivot_table.columns)
            
            result_data = []
            for row in result_df.values:
                # Convert each value in the row to standard Python types
                converted_row = []
                for val in row:
                    if hasattr(val, 'item'):  # Check if it's a NumPy type with item() method
                        converted_row.append(val.item())
                    else:
                        converted_row.append(val)
                result_data.append(converted_row)
                
            # Step 12: Calculate totals for each column
            totals = ['Total']
            for col in pivot_table.columns:
                totals.append(int(pivot_table[col].sum()))
                
            # Step 13: Final data to write to sheet
            sheet_data = [header] + result_data + [totals]
            
            # Step 14: Create or update the output spreadsheet
            if not output_spreadsheet_name:
                output_spreadsheet_name = "User_Count_by_Date_Across_Sheets"
            
            output_id = self.get_or_create_result_file(output_spreadsheet_name)
            if not output_id:
                logger.error("Failed to create or find output spreadsheet")
                return None
                
            # Step 15: Write data to the spreadsheet
            self.write_spreadsheet_data(output_id, 'Sheet1', sheet_data)
            
            logger.info(f"Successfully created date-based user count across sheets in '{output_spreadsheet_name}'")
            return output_id
            
        except Exception as e:
            logger.error(f"Error creating date-based user count across sheets: {e}")
            return None

if __name__ == '__main__':
    processor = DriveDataProcessor(folder_id = FOLDER_ID, filter_date = True)
    # processor.count_daily_registers_by_source_name(output_spreadsheet_name="Daily_Registers_by_Source_Name")
    # processor.count_daily_registers_by_ref(output_spreadsheet_name="Daily_Registers_by_Ref")
    # processor.count_users_by_source_name(output_spreadsheet_name="Users_by_Source_Name")
    # processor.count_users_by_ref(output_spreadsheet_name="Users_by_Ref")
    # processor.count_users_each_sheet_by_source_name(output_spreadsheet_name="Users_Each_Sheet_by_Source_Name")
    # processor.count_users_each_sheet_by_ref(output_spreadsheet_name="Users_Each_Sheet_by_Ref")
    processor.count_users_each_sheet_by_date(output_spreadsheet_name="Users_Each_Sheet_by_Date")