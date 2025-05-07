import gradio as gr
import pandas as pd
import os
import zipfile
from io import BytesIO
import time
import logging
import datetime  # Add this import

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('statistical_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create output directory if it doesn't exist
os.makedirs("output", exist_ok=True)
logger.info("Output directory checked/created")

class ExcelDataProcessor:
    def __init__(self):
        self.output_files = []
        logger.info("ExcelDataProcessor initialized")
    
    def _extract_number(self, sheet_name):
        """Extract number from sheet name for sorting"""
        try:
            return int(sheet_name[:sheet_name.find('.')])
        except ValueError:
            logger.info(f"Error processing {sheet_name}: '>' not supported between instances of 'datetime.date' and 'str'")
        return float('inf')  # Send sheets without numbers to the end
    
    def _create_unique_filename(self, base_name):
        """Create a unique filename with timestamp"""
        timestamp = int(time.time())
        return f"{base_name}_{timestamp}.xlsx"
    
    def count_daily_registers_by_source_name(self, df):
        """Count daily registers by source name"""
        logger.info("Starting count_daily_registers_by_source_name")
        df_filtered = df[df['User ID'].notna()].copy()
        df_filtered.loc[:, 'Created At'] = pd.to_datetime(df_filtered['Created At']).dt.date
        target_date = datetime.date(2025, 4, 14)
        df_filtered = df_filtered[df_filtered['Created At'] > target_date].copy()
        pivot_table = pd.pivot_table(
            df_filtered,
            index='Source Name',
            columns='Created At',
            values='User ID',
            aggfunc='count',
            fill_value=0
        )
        pivot_table.loc['Total'] = pivot_table.sum()
        output_path = self._create_unique_filename("count_daily_registers_by_source_name")
        full_path = os.path.join("output", output_path)
        pivot_table.to_excel(full_path)
        self.output_files.append(full_path)
        logger.info(f"Saved count_daily_registers_by_source_name to {full_path}")
        return pivot_table
    
    def count_daily_registers_by_ref(self, df):
        """Count daily registers by reference"""
        logger.info("Starting count_daily_registers_by_ref")
        df_filtered = df[df['User ID'].notna()].copy()
        df_filtered.loc[:, 'Created At'] = pd.to_datetime(df_filtered['Created At']).dt.date
        target_date = datetime.date(2025, 4, 15)
        df_filtered = df_filtered[df_filtered['Created At'] < target_date].copy()
        df_filtered.loc[(df_filtered['Source Name'] == 'direct') & (df_filtered['Ref By'].isna()), 'Ref By'] = 'direct'
        pivot_table = pd.pivot_table(
            df_filtered,
            index='Ref By',
            columns='Created At',
            values='User ID',
            aggfunc='count',
            fill_value=0
        )
        pivot_table.loc['Total'] = pivot_table.sum()
        output_path = self._create_unique_filename("count_daily_registers_by_ref")
        full_path = os.path.join("output", output_path)
        pivot_table.to_excel(full_path)
        self.output_files.append(full_path)
        logger.info(f"Saved count_daily_registers_by_ref to {full_path}")
        return pivot_table
    
    def count_users_by_source_name(self, df):
        """Count unique users by source name"""
        logger.info("Starting count_users_by_source_name")
        df_filtered = df[df['User ID'].notna()].copy()
        df_filtered = df_filtered.drop_duplicates(subset=['User ID'], keep='first')
        target_date = datetime.date(2025, 4, 14)
        df_filtered['Created At'] = pd.to_datetime(df_filtered['Created At']).dt.date
        df_filtered = df_filtered[df_filtered['Created At'] > target_date].copy()
        pivot_table = pd.pivot_table(
            df_filtered,
            index='Source Name',
            values='User ID',
            aggfunc='count',
            fill_value=0
        )
        output_path = self._create_unique_filename("count_users_by_source_name")
        full_path = os.path.join("output", output_path)
        pivot_table.to_excel(full_path)
        self.output_files.append(full_path)
        logger.info(f"Saved count_users_by_source_name to {full_path}")
        return pivot_table
    
    def count_users_by_ref(self, df):
        """Count unique users by reference"""
        logger.info("Starting count_users_by_ref")
        df_filtered = df[df['User ID'].notna()].copy()
        df_filtered = df_filtered.drop_duplicates(subset=['User ID'], keep='first')
        target_date = datetime.date(2025, 4, 15)
        df_filtered['Created At'] = pd.to_datetime(df_filtered['Created At']).dt.date
        df_filtered = df_filtered[df_filtered['Created At'] < target_date].copy()
        df_filtered.loc[(df_filtered['Source Name'] == 'direct') & (df_filtered['Ref By'].isna()), 'Ref By'] = 'direct'
        pivot_table = pd.pivot_table(
            df_filtered,
            index='Ref By',
            values='User ID',
            aggfunc='count',
            fill_value=0
        )
        output_path = self._create_unique_filename("count_users_by_ref")
        full_path = os.path.join("output", output_path)
        pivot_table.to_excel(full_path)
        self.output_files.append(full_path)
        logger.info(f"Saved count_users_by_ref to {full_path}")
        return pivot_table
    
    def count_users_each_sheet_by_source_name(self, excel_file):
        """Count users in each sheet by source name"""
        logger.info("Starting count_users_each_sheet_by_source_name")
        df_dict = pd.read_excel(excel_file, sheet_name=None)
        sheet_dfs = {sheet_name: df for sheet_name, df in df_dict.items() if '.' in sheet_name}
        if not sheet_dfs:
            logger.warning("No sheets found with '.' in their names")
            return "No sheets found with '.' in their names", None
        combined_df = pd.concat(
            [df.assign(SheetName=sheet_name) for sheet_name, df in sheet_dfs.items()],
            axis=0,
            ignore_index=True
        )
        combined_df = combined_df[combined_df['User ID'].notna()].copy()
        combined_df_filtered = combined_df.dropna(subset=['Source Name']).copy()
        combined_df_filtered['Created At'] = pd.to_datetime(combined_df_filtered['Created At']).dt.date
        target_date = datetime.date(2025, 4, 14)
        combined_df_filtered = combined_df_filtered[combined_df_filtered['Created At'] > target_date].copy()
        if not {'Source Name', 'User ID', 'SheetName'}.issubset(combined_df_filtered.columns):
            return "Required columns 'Source Name', 'User ID', or 'SheetName' not found", None
        pivot_table = pd.pivot_table(
            combined_df_filtered,
            index='Source Name',
            columns='SheetName',
            values='User ID',
            aggfunc='count',
            fill_value=0
        )
        sorted_columns = sorted(pivot_table.columns, key=self._extract_number)
        pivot_table = pivot_table[sorted_columns]
        pivot_table.loc['Total'] = pivot_table.sum()
        output_path = self._create_unique_filename("count_users_each_sheet_by_source_name")
        full_path = os.path.join("output", output_path)
        pivot_table.to_excel(full_path)
        self.output_files.append(full_path)
        logger.info(f"Saved count_users_each_sheet_by_source_name to {full_path}")
        return "Success", pivot_table
    
    def count_users_each_sheet_by_ref(self, excel_file):
        """Count users in each sheet by reference"""
        logger.info("Starting count_users_each_sheet_by_ref")
        df_dict = pd.read_excel(excel_file, sheet_name=None)
        sheet_dfs = {sheet_name: df for sheet_name, df in df_dict.items() if '.' in sheet_name}
        if not sheet_dfs:
            logger.warning("No sheets found with '.' in their names")
            return "No sheets found with '.' in their names", None
        combined_df = pd.concat(
            [df.assign(SheetName=sheet_name) for sheet_name, df in sheet_dfs.items()],
            axis=0,
            ignore_index=True
        )
        combined_df = combined_df[combined_df['User ID'].notna()].copy()
        combined_df_filtered = combined_df.copy()
        combined_df_filtered['Created At'] = pd.to_datetime(combined_df_filtered['Created At']).dt.date
        target_date = datetime.date(2025, 4, 15)
        combined_df_filtered = combined_df_filtered[combined_df_filtered['Created At'] < target_date].copy()
        combined_df_filtered.loc[(combined_df_filtered['Source Name'] == 'direct') & (combined_df_filtered['Ref By'].isna()), 'Ref By'] = 'direct'
        if not {'Ref By', 'User ID', 'SheetName'}.issubset(combined_df_filtered.columns):
            return "Required columns 'Ref By', 'User ID', or 'SheetName' not found", None
        pivot_table = pd.pivot_table(
            combined_df_filtered,
            index='Ref By',
            columns='SheetName',
            values='User ID',
            aggfunc='count',
            fill_value=0
        )
        sorted_columns = sorted(pivot_table.columns, key=self._extract_number)
        pivot_table = pivot_table[sorted_columns]
        pivot_table.loc['Total'] = pivot_table.sum()
        output_path = self._create_unique_filename("count_users_each_sheet_by_ref")
        full_path = os.path.join("output", output_path)
        pivot_table.to_excel(full_path)
        self.output_files.append(full_path)
        logger.info(f"Saved count_users_each_sheet_by_ref to {full_path}")
        return "Success", pivot_table
    
    def count_users_each_sheet_by_date(self, excel_file):
        """Count users in each sheet by date"""
        logger.info("Starting count_users_each_sheet_by_date")
        df_dict = pd.read_excel(excel_file, sheet_name=None)
        sheet_dfs = {sheet_name: df for sheet_name, df in df_dict.items() if '.' in sheet_name}
        if not sheet_dfs:
            logger.warning("No sheets found with '.' in their names")
            return "No sheets found with '.' in their names", None
        combined_df = pd.concat(
            [df.assign(SheetName=sheet_name) for sheet_name, df in sheet_dfs.items()],
            axis=0,
            ignore_index=True
        )
        combined_df = combined_df[combined_df['User ID'].notna()].copy()
        combined_df_filtered = combined_df[combined_df['Created At'].notna()].copy()
        combined_df_filtered.loc[:, 'Created At'] = pd.to_datetime(combined_df_filtered['Created At']).dt.date
        if not {'Created At', 'User ID', 'SheetName'}.issubset(combined_df_filtered.columns):
            return "Required columns 'Created At', 'User ID', or 'SheetName' not found", None
        pivot_table = pd.pivot_table(
            combined_df_filtered,
            index='Created At',
            columns='SheetName',
            values='User ID',
            aggfunc='count',
            fill_value=0
        )
        sorted_columns = sorted(pivot_table.columns, key=self._extract_number)
        pivot_table = pivot_table[sorted_columns]
        pivot_table.loc['Total'] = pivot_table.sum()
        output_path = self._create_unique_filename("count_users_each_sheet_by_date")
        full_path = os.path.join("output", output_path)
        pivot_table.to_excel(full_path)
        self.output_files.append(full_path)
        logger.info(f"Saved count_users_each_sheet_by_date to {full_path}")
        return "Success", pivot_table
    
    def process_file(self, excel_file, operations):
        """Process file with selected operations"""
        logger.info(f"Starting file processing with operations: {operations}")
        self.output_files = []  # Reset output files
        results = {}
        result_preview = None
        
        if not excel_file:
            logger.warning("No file uploaded")
            return "Please upload an Excel file", None, None
        
        try:
            if any(op in operations for op in ["count_daily_registers_by_source_name", 
                                              "count_daily_registers_by_ref",
                                              "count_users_by_source_name",
                                              "count_users_by_ref"]):
                try:
                    df = pd.read_excel(excel_file, sheet_name="User Register")
                    if "count_daily_registers_by_source_name" in operations:
                        results["Daily Registers by Source Name"] = self.count_daily_registers_by_source_name(df)
                        if result_preview is None:
                            result_preview = results["Daily Registers by Source Name"]
                    if "count_daily_registers_by_ref" in operations:
                        results["Daily Registers by Ref"] = self.count_daily_registers_by_ref(df)
                        if result_preview is None:
                            result_preview = results["Daily Registers by Ref"]
                    if "count_users_by_source_name" in operations:
                        results["Users by Source Name"] = self.count_users_by_source_name(df)
                        if result_preview is None:
                            result_preview = results["Users by Source Name"]
                    if "count_users_by_ref" in operations:
                        results["Users by Ref"] = self.count_users_by_ref(df)
                        if result_preview is None:
                            result_preview = results["Users by Ref"]
                except Exception as e:
                    logger.error(f"Error processing User Register sheet: {str(e)}", exc_info=True)
                    return f"Error processing User Register sheet: {str(e)}", None, None
            
            if "count_users_each_sheet_by_source_name" in operations:
                status, pivot = self.count_users_each_sheet_by_source_name(excel_file)
                if status != "Success":
                    return status, None, None
                results["Users Each Sheet by Source Name"] = pivot
                if result_preview is None:
                    result_preview = pivot
            
            if "count_users_each_sheet_by_ref" in operations:
                status, pivot = self.count_users_each_sheet_by_ref(excel_file)
                if status != "Success":
                    return status, None, None
                results["Users Each Sheet by Ref"] = pivot
                if result_preview is None:
                    result_preview = pivot
            
            if "count_users_each_sheet_by_date" in operations:
                status, pivot = self.count_users_each_sheet_by_date(excel_file)
                if status != "Success":
                    return status, None, None
                results["Users Each Sheet by Date"] = pivot
                if result_preview is None:
                    result_preview = pivot
            
            if self.output_files:
                logger.info("Creating ZIP file with all outputs")
                zip_buffer = BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
                    for file_path in self.output_files:
                        if os.path.exists(file_path):
                            zip_file.write(file_path, os.path.basename(file_path))
                zip_buffer.seek(0)
                zip_path = os.path.join("output", "excel_reports.zip")
                with open(zip_path, "wb") as f:
                    f.write(zip_buffer.getvalue())
                logger.info(f"ZIP file created at {zip_path}")
                if result_preview is not None and result_preview.size > 10000:
                    result_preview = result_preview.head(100)
                return "Processing completed successfully!", result_preview, zip_path
            else:
                logger.warning("No operations were performed")
                return "No operations were performed.", None, None
            
        except Exception as e:
            logger.error(f"Error during file processing: {str(e)}", exc_info=True)
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
                value=["count_daily_registers_by_source_name"]
            )
            
            process_btn = gr.Button("Process Excel File", variant="primary")
            
        with gr.Column(scale=2):
            status_output = gr.Textbox(label="Status")
            with gr.Row():
                result_output = gr.Dataframe(label="Preview Results (Limited to avoid UI freezing)")
            download_btn = gr.File(label="Download Results (ZIP)")

    def show_processing(file, ops):
        logger.info(f"Processing started with operations: {ops}")
        return "Processing... This may take a moment. Files are being saved even if UI appears frozen.", None, None
    
    def process_excel_file(file, ops):
        logger.info(f"Processing excel file with operations: {ops}")
        status, preview, zip_file = processor.process_file(file, ops)
        logger.info(f"Processing completed with status: {status}")
        return status, preview, zip_file
    
    process_btn.click(
        fn=show_processing,
        inputs=[file_input, operations],
        outputs=[status_output, result_output, download_btn],
        queue=False
    ).then(
        fn=process_excel_file,
        inputs=[file_input, operations],
        outputs=[status_output, result_output, download_btn]
    )
    
    gr.Markdown("""
    ## Instructions
    
    1. Upload your Excel file using the file uploader
    2. Select one or more operations to perform
    3. Click "Process Excel File" button
    4. View the results in the preview table (limited to prevent UI freezing)
    5. Download the ZIP file containing all generated Excel files
    
    ## Operations Description
    
    - **count_daily_registers_by_source_name**: Count daily registrations by source name (excluding 'direct')
    - **count_daily_registers_by_ref**: Count daily registrations by referral (for 'direct' source only)
    - **count_users_by_source_name**: Count unique users by source name (excluding 'direct')
    - **count_users_by_ref**: Count unique users by referral (for 'direct' source only)
    - **count_users_each_sheet_by_source_name**: Count users in each sheet by source name
    - **count_users_each_sheet_by_ref**: Count users in each sheet by referral
    - **count_users_each_sheet_by_date**: Count users in each sheet by date
    
    ## Notes
    
    - If the UI appears to freeze, don't worry! The processing is still happening in the background.
    - All output files are saved in the 'output' folder even if the UI is unresponsive.
    - For very large Excel files, only a preview of the results will be shown to prevent UI freezing.
    """)

# Launch the app
if __name__ == "__main__":
    app.launch(share=True)