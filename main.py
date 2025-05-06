import pandas as pd
import re
    
def count_daily_registers_by_source_name(file_path: str):

    df = pd.read_excel(file_path, sheet_name="User Register")

    # Bỏ những dòng có Source Name là "direct"
    df = df[df['Source Name'] != 'direct']

    # Bỏ phần giờ chỉ giữ ngày, tháng, năm
    df['Created At'] = pd.to_datetime(df['Created At']).dt.date

    # Tạo bảng pivot: đếm số dòng theo Source Name và Created At, thêm hàng tổng
    pivot_table = pd.pivot_table(
        df,
        index='Source Name',
        columns='Created At',
        values='User ID',  # hoặc dùng 'Email', 'Username' nếu muốn đếm số dòng
        aggfunc='count',   # đếm số lượng
        fill_value=0       # thay NaN bằng 0
    )

    # Thêm hàng tổng (tổng tất cả các Source Name theo từng ngày)
    pivot_table.loc['Total'] = pivot_table.sum()

    # Ghi pivot vào output theo dạng excel
    pivot_table.to_excel("output/count_daily_registers_by_source_name.xlsx")

def count_daily_registers_by_ref(file_path: str):

    df = pd.read_excel(file_path, sheet_name="User Register")

    # Lấy những dòng có Source Name là "direct" và ref by khác NaN
    df = df[(df['Source Name'] == 'direct') & (df['Ref By'].notna())]

    # Bỏ phần giờ chỉ giữ ngày, tháng, năm
    df['Created At'] = pd.to_datetime(df['Created At']).dt.date

    # Tạo bảng pivot: đếm số dòng theo Source Name và Created At, thêm hàng tổng

    pivot_table = pd.pivot_table(
        df,
        index='Ref By',
        columns='Created At',
        values='User ID',  # hoặc dùng 'Email', 'Username' nếu muốn đếm số dòng
        aggfunc='count',   # đếm số lượng
        fill_value=0       # thay NaN bằng 0
    )

    # Thêm hàng tổng (tổng tất cả các Source Name theo từng ngày)
    pivot_table.loc['Total'] = pivot_table.sum()

    # Ghi pivot vào output theo dạng excel
    pivot_table.to_excel("output/count_daily_registers_by_ref.xlsx")

def count_users_by_source_name(file_path: str):
    
    df = pd.read_excel(file_path, sheet_name="User Register")

    df = df[df['Source Name'] != 'direct']

    # remove duplicate rows
    df = df.drop_duplicates(subset=['User ID'], keep='first')
    
    # Create pivot table counting User IDs by Source Name
    pivot_table = pd.pivot_table(
        df,
        index='Source Name',
        values='User ID',
        aggfunc='count',
        fill_value=0
    )
    
    # Save to Excel
    output_path = "output/count_users_by_source_name.xlsx"
    pivot_table.to_excel(output_path)

def count_users_by_ref(file_path: str):
    
    df = pd.read_excel(file_path, sheet_name="User Register")

    df = df[(df['Source Name'] == 'direct') & (df['Ref By'].notna())]

    # remove duplicate rows
    df = df.drop_duplicates(subset=['User ID'], keep='first')
    
    # Create pivot table counting User IDs by Source Name
    pivot_table = pd.pivot_table(
        df,
        index='Ref By',
        values='User ID',
        aggfunc='count',
        fill_value=0
    )
    
    # Save to Excel
    output_path = "output/count_users_by_ref.xlsx"
    pivot_table.to_excel(output_path)

def extract_number(sheet_name):
    match = re.search(r'^(\d+)', sheet_name)
    if match:
        return int(match.group(1))
    return float('inf')  # Đưa các sheet không có số lên cuối

def count_users_each_sheet_by_source_name(file_path: str):
    
    df_dict = pd.read_excel(file_path, sheet_name=None)
        
    # Filter sheets with '.' in name
    sheet_dfs = {sheet_name: df for sheet_name, df in df_dict.items() if '.' in sheet_name}
    
    if not sheet_dfs:
        raise ValueError("No sheets found with '.' in their names")
    
    # Combine all filtered sheets into a single DataFrame and add SheetName column
    combined_df = pd.concat(
        [df.assign(SheetName=sheet_name) for sheet_name, df in sheet_dfs.items()],
        axis=0,
        ignore_index=True
    )
    
    # Remove rows with all NaN values
    combined_df = combined_df.dropna(how='all')
    # Remove rows where 'Source Name' is NaN
    combined_df = combined_df.dropna(subset=['Source Name'])
    # remove direct rows
    combined_df = combined_df[combined_df['Source Name'] != 'direct']
    
    # Check if required columns exist
    if not {'Source Name', 'User ID', 'SheetName'}.issubset(combined_df.columns):
        raise ValueError("Required columns 'Source Name', 'User ID', or 'SheetName' not found")
    
    # Create pivot table counting User IDs by Source Name and SheetName
    pivot_table = pd.pivot_table(
        combined_df,
        index='Source Name',
        columns='SheetName',
        values='User ID',
        aggfunc='count',
        fill_value=0
    )

    # Sắp xếp cột dựa vào số thứ tự trong tên sheet
    sorted_columns = sorted(pivot_table.columns, key=extract_number)
    pivot_table = pivot_table[sorted_columns]
    
    pivot_table.loc['Total'] = pivot_table.sum()
    
    pivot_table.to_excel("output/count_users_each_sheet_by_source_name.xlsx")

def count_users_each_sheet_by_ref(file_path: str):
    df_dict = pd.read_excel(file_path, sheet_name=None)
        
    # Filter sheets with '.' in name
    sheet_dfs = {sheet_name: df for sheet_name, df in df_dict.items() if '.' in sheet_name}
    
    if not sheet_dfs:
        raise ValueError("No sheets found with '.' in their names")
    
    # Combine all filtered sheets into a single DataFrame and add SheetName column
    combined_df = pd.concat(
        [df.assign(SheetName=sheet_name) for sheet_name, df in sheet_dfs.items()],
        axis=0,
        ignore_index=True
    )
    
    # Remove rows with all NaN values
    combined_df = combined_df.dropna(how='all')
    # remove direct rows
    combined_df = combined_df[combined_df['Ref By'].notna()]
    
    # Check if required columns exist
    if not {'Ref By', 'User ID', 'SheetName'}.issubset(combined_df.columns):
        raise ValueError("Required columns 'Ref By', 'User ID', or 'SheetName' not found")
    
    # Create pivot table counting User IDs by Source Name and SheetName
    pivot_table = pd.pivot_table(
        combined_df,
        index='Ref By',
        columns='SheetName',
        values='User ID',
        aggfunc='count',
        fill_value=0
    )
    
    # Sắp xếp cột dựa vào số thứ tự trong tên sheet
    sorted_columns = sorted(pivot_table.columns, key=extract_number)
    pivot_table = pivot_table[sorted_columns]
    
    pivot_table.loc['Total'] = pivot_table.sum()
    
    pivot_table.to_excel("output/count_users_each_sheet_by_ref.xlsx")



def count_users_each_sheet_by_date(file_path: str):
    df_dict = pd.read_excel(file_path, sheet_name=None)
        
    # Filter sheets with '.' in name
    sheet_dfs = {sheet_name: df for sheet_name, df in df_dict.items() if '.' in sheet_name}
    
    if not sheet_dfs:
        raise ValueError("No sheets found with '.' in their names")
    
    # Combine all filtered sheets into a single DataFrame and add SheetName column
    combined_df = pd.concat(
        [df.assign(SheetName=sheet_name) for sheet_name, df in sheet_dfs.items()],
        axis=0,
        ignore_index=True
    )
    
    # Remove rows with all NaN values
    combined_df = combined_df.dropna(how='all')
    # remove direct rows
    combined_df = combined_df[combined_df['Ref By'].notna()]

    # Bỏ phần giờ chỉ giữ ngày, tháng, năm
    combined_df['Created At'] = pd.to_datetime(combined_df['Created At']).dt.date
    
    # Check if required columns exist
    if not {'Ref By', 'User ID', 'SheetName'}.issubset(combined_df.columns):
        raise ValueError("Required columns 'Ref By', 'User ID', or 'SheetName' not found")
    
    # Create pivot table counting User IDs by Source Name and SheetName
    pivot_table = pd.pivot_table(
        combined_df,
        index='Created At',
        columns='SheetName',
        values='User ID',
        aggfunc='count',
        fill_value=0
    )
    
    # Sắp xếp cột dựa vào số thứ tự trong tên sheet
    sorted_columns = sorted(pivot_table.columns, key=extract_number)
    pivot_table = pivot_table[sorted_columns]
    
    pivot_table.loc['Total'] = pivot_table.sum()
    
    pivot_table.to_excel("output/count_users_each_sheet_by_date.xlsx")

if __name__ == "__main__":
    file_path = "E:\\Code\\py\\statistical\\input\\data.xlsx"
    
    count_daily_registers_by_source_name(file_path)
    
    count_daily_registers_by_ref(file_path)

    count_users_by_source_name(file_path)

    count_users_by_ref(file_path)

    count_users_each_sheet_by_source_name(file_path)

    count_users_each_sheet_by_ref(file_path)

    count_users_each_sheet_by_date(file_path)