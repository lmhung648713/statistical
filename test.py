import gradio as gr
import pandas as pd
import re
import os
import tempfile
import zipfile
from io import BytesIO
import time

# Create output directory if it doesn't exist
os.makedirs("output", exist_ok=True)

class ExcelDataProcessor:
    def __init__(self):
        self.output_files = []
    
    def _extract_number(self, sheet_name):
        """Extract number from sheet name for sorting"""
        match = re.search(r'^(\d+)', sheet_name)
        if match:
            return int(match.group(1))
        return float('inf')  # Send sheets without numbers to the end
    
    def _create_unique_filename(self, base_name):
        """Create a unique filename with timestamp"""
        timestamp = int(time.time())
        return f"{base_name}_{timestamp}.xlsx"
    
    def count_daily_registers_by_source_name(self, df):
        """Count daily registers by source name"""
        # Filter out rows with Source Name as "direct"
        df = df[df['Source Name'] != 'direct']
        
        # Keep only date part
        df['Created At'] = pd.to_datetime(df['Created At']).dt.date
        
        # Create pivot table
        pivot_table = pd.pivot_table(
            df,
            index='Source Name',
            columns='Created At',
            values='User ID',
            aggfunc='count',
            fill_value=0
        )
        
        # Add total row
        pivot_table.loc['Total'] = pivot_table.sum()
        
        # Create unique filename
        output_path = self._create_unique_filename("count_daily_registers_by_source_name")
        full_path = os.path.join("output", output_path)
        pivot_table.to_excel(full_path)
        self.output_files.append(full_path)
        
        return pivot_table
    
    def count_daily_registers_by_ref(self, df):
        """Count daily registers by reference"""
        # Filter for direct source with Ref By not NaN
        df = df[(df['Source Name'] == 'direct') & (df['Ref By'].notna())]
        
        # Keep only date part
        df['Created At'] = pd.to_datetime(df['Created At']).dt.date
        
        # Create pivot table
        pivot_table = pd.pivot_table(
            df,
            index='Ref By',
            columns='Created At',
            values='User ID',
            aggfunc='count',
            fill_value=0
        )
        
        # Add total row
        pivot_table.loc['Total'] = pivot_table.sum()
        
        # Create unique filename
        output_path = self._create_unique_filename("count_daily_registers_by_ref")
        full_path = os.path.join("output", output_path)
        pivot_table.to_excel(full_path)
        self.output_files.append(full_path)
        
        return pivot_table
    
    def count_users_by_source_name(self, df):
        """Count unique users by source name"""
        # Filter out direct sources
        df = df[df['Source Name'] != 'direct']
        
        # Remove duplicate users
        df = df.drop_duplicates(subset=['User ID'], keep='first')
        
        # Create pivot table
        pivot_table = pd.pivot_table(
            df,
            index='Source Name',
            values='User ID',
            aggfunc='count',
            fill_value=0
        )
        
        # Create unique filename
        output_path = self._create_unique_filename("count_users_by_source_name")
        full_path = os.path.join("output", output_path)
        pivot_table.to_excel(full_path)
        self.output_files.append(full_path)
        
        return pivot_table
    
    def count_users_by_ref(self, df):
        """Count unique users by reference"""
        # Filter for direct source with Ref By not NaN
        df = df[(df['Source Name'] == 'direct') & (df['Ref By'].notna())]
        
        # Remove duplicate users
        df = df.drop_duplicates(subset=['User ID'], keep='first')
        
        # Create pivot table
        pivot_table = pd.pivot_table(
            df,
            index='Ref By',
            values='User ID',
            aggfunc='count',
            fill_value=0
        )
        
        # Create unique filename
        output_path = self._create_unique_filename("count_users_by_ref")
        full_path = os.path.join("output", output_path)
        pivot_table.to_excel(full_path)
        self.output_files.append(full_path)
        
        return pivot_table
    
    def count_users_each_sheet_by_source_name(self, excel_file):
        """Count users in each sheet by source name"""
        df_dict = pd.read_excel(excel_file, sheet_name=None)
        
        # Filter sheets with '.' in name
        sheet_dfs = {sheet_name: df for sheet_name, df in df_dict.items() if '.' in sheet_name}
        
        if not sheet_dfs:
            return "No sheets found with '.' in their names", None
        
        # Combine all filtered sheets into a single DataFrame and add SheetName column
        combined_df = pd.concat(
            [df.assign(SheetName=sheet_name) for sheet_name, df in sheet_dfs.items()],
            axis=0,
            ignore_index=True
        )
        
        # Clean data
        combined_df = combined_df.dropna(how='all')
        combined_df = combined_df.dropna(subset=['Source Name'])
        combined_df = combined_df[combined_df['Source Name'] != 'direct']
        
        # Check required columns
        if not {'Source Name', 'User ID', 'SheetName'}.issubset(combined_df.columns):
            return "Required columns 'Source Name', 'User ID', or 'SheetName' not found", None
        
        # Create pivot table
        pivot_table = pd.pivot_table(
            combined_df,
            index='Source Name',
            columns='SheetName',
            values='User ID',
            aggfunc='count',
            fill_value=0
        )
        
        # Sort columns by sheet number
        sorted_columns = sorted(pivot_table.columns, key=self._extract_number)
        pivot_table = pivot_table[sorted_columns]
        
        # Add total row
        pivot_table.loc['Total'] = pivot_table.sum()
        
        # Create unique filename
        output_path = self._create_unique_filename("count_users_each_sheet_by_source_name")
        full_path = os.path.join("output", output_path)
        pivot_table.to_excel(full_path)
        self.output_files.append(full_path)
        
        return "Success", pivot_table
    
    def count_users_each_sheet_by_ref(self, excel_file):
        """Count users in each sheet by reference"""
        df_dict = pd.read_excel(excel_file, sheet_name=None)
        
        # Filter sheets with '.' in name
        sheet_dfs = {sheet_name: df for sheet_name, df in df_dict.items() if '.' in sheet_name}
        
        if not sheet_dfs:
            return "No sheets found with '.' in their names", None
        
        # Combine all filtered sheets
        combined_df = pd.concat(
            [df.assign(SheetName=sheet_name) for sheet_name, df in sheet_dfs.items()],
            axis=0,
            ignore_index=True
        )
        
        # Clean data
        combined_df = combined_df.dropna(how='all')
        combined_df = combined_df[combined_df['Ref By'].notna()]
        
        # Check required columns
        if not {'Ref By', 'User ID', 'SheetName'}.issubset(combined_df.columns):
            return "Required columns 'Ref By', 'User ID', or 'SheetName' not found", None
        
        # Create pivot table
        pivot_table = pd.pivot_table(
            combined_df,
            index='Ref By',
            columns='SheetName',
            values='User ID',
            aggfunc='count',
            fill_value=0
        )
        
        # Sort columns by sheet number
        sorted_columns = sorted(pivot_table.columns, key=self._extract_number)
        pivot_table = pivot_table[sorted_columns]
        
        # Add total row
        pivot_table.loc['Total'] = pivot_table.sum()
        
        # Create unique filename
        output_path = self._create_unique_filename("count_users_each_sheet_by_ref")
        full_path = os.path.join("output", output_path)
        pivot_table.to_excel(full_path)
        self.output_files.append(full_path)
        
        return "Success", pivot_table
    
    def count_users_each_sheet_by_date(self, excel_file):
        """Count users in each sheet by date"""
        df_dict = pd.read_excel(excel_file, sheet_name=None)
        
        # Filter sheets with '.' in name
        sheet_dfs = {sheet_name: df for sheet_name, df in df_dict.items() if '.' in sheet_name}
        
        if not sheet_dfs:
            return "No sheets found with '.' in their names", None
        
        # Combine all filtered sheets
        combined_df = pd.concat(
            [df.assign(SheetName=sheet_name) for sheet_name, df in sheet_dfs.items()],
            axis=0,
            ignore_index=True
        )
        
        # Clean data
        combined_df = combined_df.dropna(how='all')
        combined_df = combined_df[combined_df['Ref By'].notna()]
        
        # Keep only date part
        combined_df['Created At'] = pd.to_datetime(combined_df['Created At']).dt.date
        
        # Check required columns
        if not {'Created At', 'User ID', 'SheetName'}.issubset(combined_df.columns):
            return "Required columns 'Created At', 'User ID', or 'SheetName' not found", None
        
        # Create pivot table
        pivot_table = pd.pivot_table(
            combined_df,
            index='Created At',
            columns='SheetName',
            values='User ID',
            aggfunc='count',
            fill_value=0
        )
        
        # Sort columns by sheet number
        sorted_columns = sorted(pivot_table.columns, key=self._extract_number)
        pivot_table = pivot_table[sorted_columns]
        
        # Add total row
        pivot_table.loc['Total'] = pivot_table.sum()
        
        # Create unique filename
        output_path = self._create_unique_filename("count_users_each_sheet_by_date")
        full_path = os.path.join("output", output_path)
        pivot_table.to_excel(full_path)
        self.output_files.append(full_path)
        
        return "Success", pivot_table
    
    def process_file(self, excel_file, operations):
        """Process file with selected operations"""
        self.output_files = []  # Reset output files
        results = {}
        
        if not excel_file:
            return "Please upload an Excel file", None, None
        
        try:
            # For operations that work with the User Register sheet
            if any(op in operations for op in ["count_daily_registers_by_source_name", 
                                              "count_daily_registers_by_ref",
                                              "count_users_by_source_name",
                                              "count_users_by_ref"]):
                try:
                    df = pd.read_excel(excel_file, sheet_name="User Register")
                    
                    if "count_daily_registers_by_source_name" in operations:
                        results["Daily Registers by Source Name"] = self.count_daily_registers_by_source_name(df.copy())
                    
                    if "count_daily_registers_by_ref" in operations:
                        results["Daily Registers by Ref"] = self.count_daily_registers_by_ref(df.copy())
                    
                    if "count_users_by_source_name" in operations:
                        results["Users by Source Name"] = self.count_users_by_source_name(df.copy())
                    
                    if "count_users_by_ref" in operations:
                        results["Users by Ref"] = self.count_users_by_ref(df.copy())
                        
                except Exception as e:
                    return f"Error processing User Register sheet: {str(e)}", None, None
            
            # For operations that work with multiple sheets
            if "count_users_each_sheet_by_source_name" in operations:
                status, pivot = self.count_users_each_sheet_by_source_name(excel_file)
                if status != "Success":
                    return status, None, None
                results["Users Each Sheet by Source Name"] = pivot
            
            if "count_users_each_sheet_by_ref" in operations:
                status, pivot = self.count_users_each_sheet_by_ref(excel_file)
                if status != "Success":
                    return status, None, None
                results["Users Each Sheet by Ref"] = pivot
            
            if "count_users_each_sheet_by_date" in operations:
                status, pivot = self.count_users_each_sheet_by_date(excel_file)
                if status != "Success":
                    return status, None, None
                results["Users Each Sheet by Date"] = pivot
            
            # Create ZIP file with all outputs
            if self.output_files:
                zip_buffer = BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
                    for file_path in self.output_files:
                        zip_file.write(file_path, os.path.basename(file_path))
                
                zip_buffer.seek(0)
                zip_path = os.path.join("output", "excel_reports.zip")
                with open(zip_path, "wb") as f:
                    f.write(zip_buffer.getvalue())
                
                return "Processing completed successfully!", results, zip_path
            else:
                return "No operations were performed.", None, None
            
        except Exception as e:
            return f"Error: {str(e)}", None, None

# Create the processor
processor = ExcelDataProcessor()

# Define the Gradio interface
with gr.Blocks(title="Excel Data Processor") as app:
    gr.Markdown("# Excel Data Processing Tool")
    gr.Markdown("Upload your Excel file and select the operations to perform.")
    
    with gr.Row():
        with gr.Column(scale=1):
            file_input = gr.File(label="Upload Excel File")
            
            operations = gr.CheckboxGroup(
                choices=[
                    "count_daily_registers_by_source_name",
                    "count_daily_registers_by_ref",
                    "count_users_by_source_name",
                    "count_users_by_ref",
                    "count_users_each_sheet_by_source_name",
                    "count_users_each_sheet_by_ref",
                    "count_users_each_sheet_by_date"
                ],
                label="Select Operations",
                value=["count_daily_registers_by_source_name"]  # Default selection
            )
            
            process_btn = gr.Button("Process Excel File", variant="primary")
            
        with gr.Column(scale=2):
            status_output = gr.Textbox(label="Status")
            result_output = gr.Dataframe(label="Preview Results")
            download_btn = gr.File(label="Download Results (ZIP)")
    
    process_btn.click(
        fn=processor.process_file,
        inputs=[file_input, operations],
        outputs=[status_output, result_output, download_btn]
    )
    
    gr.Markdown("""
    ## Instructions
    
    1. Upload your Excel file using the file uploader
    2. Select one or more operations to perform
    3. Click "Process Excel File" button
    4. View the results in the preview table
    5. Download the ZIP file containing all generated Excel files
    
    ## Operations Description
    
    - **count_daily_registers_by_source_name**: Count daily registrations by source name (excluding 'direct')
    - **count_daily_registers_by_ref**: Count daily registrations by referral (for 'direct' source only)
    - **count_users_by_source_name**: Count unique users by source name (excluding 'direct')
    - **count_users_by_ref**: Count unique users by referral (for 'direct' source only)
    - **count_users_each_sheet_by_source_name**: Count users in each sheet by source name
    - **count_users_each_sheet_by_ref**: Count users in each sheet by referral
    - **count_users_each_sheet_by_date**: Count users in each sheet by date
    """)

# Launch the app
if __name__ == "__main__":
    app.launch()