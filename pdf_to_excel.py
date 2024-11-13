import sys
import subprocess

"""
THIS SCRIPT WAS PRODUCED BY CLAUDE 3.5-SONNET AND DOESN'T WORK WORTH A SHIT...
"""

def check_and_install_dependencies():
    required_packages = ['pandas', 'PyPDF2', 'openpyxl']
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            print(f"\nInstalling required package: {package}")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            except subprocess.CalledProcessError:
                print(f"Error installing {package}. Please run: pip install {package}")
                sys.exit(1)

print("Checking and installing required packages...")
check_and_install_dependencies()

import pandas as pd
import PyPDF2
import os

def extract_data_from_pdf(pdf_path):
    """Extract and process data from PDF with detailed logging"""
    rows = []
    
    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        print(f"\nProcessing PDF with {len(pdf_reader.pages)} pages")
        
        for page_num, page in enumerate(pdf_reader.pages, 1):
            text = page.extract_text()
            lines = text.split('\n')
            print(f"Page {page_num}: Found {len(lines)} lines")
            
            for line in lines:
                parts = line.strip().split()
                if not parts:
                    continue
                    
                # Look for lines that start with a potential symbol (1-4 alphanumeric chars)
                if len(parts) >= 5 and len(parts[0]) <= 4 and parts[0].isalnum():
                    try:
                        symbol = parts[0]
                        
                        # Find S/D QTY, PRICE, MARKET VALUE
                        numeric_values = []
                        for part in parts:
                            part = part.replace(',', '')
                            try:
                                value = float(part)
                                numeric_values.append(value)
                            except ValueError:
                                continue
                        
                        # Find rebate rate (looking for values that start with 0. or -0.)
                        rebate_rate = None
                        for part in parts:
                            if part.startswith('0.') or part.startswith('-0.'):
                                try:
                                    rebate_rate = float(part)
                                    break
                                except ValueError:
                                    continue
                        
                        if len(numeric_values) >= 3 and rebate_rate is not None:
                            row = {
                                'SYMBOL': symbol,
                                'S/D QTY': int(numeric_values[0]),
                                'PRICE': numeric_values[1],
                                'MARKET VALUE': numeric_values[2],
                                'REBATE RATE': rebate_rate
                            }
                            rows.append(row)
                            
                    except (ValueError, IndexError) as e:
                        print(f"Warning: Could not process line: {line[:100]}... Error: {str(e)}")
    
    return pd.DataFrame(rows)

def validate_data(df):
    """Perform comprehensive data validation"""
    print("\nValidation Results:")
    
    # 1. Row count check
    print(f"\nTotal rows found: {len(df)}")
    if not (126 <= len(df) <= 168):
        print("WARNING: Row count outside expected range (126-168)")
    
    # 2. First/Last row validation
    print("\nFirst and Last Row Check:")
    first_row = df.iloc[0]
    last_row = df.iloc[-1]
    
    expected_first = {'SYMBOL': 'AES', 'S/D QTY': 110, 'PRICE': 13.45}
    expected_last = {'SYMBOL': 'Z', 'S/D QTY': 2446, 'PRICE': 74.35}
    
    print(f"First row found: {first_row.to_dict()}")
    print(f"Last row found: {last_row.to_dict()}")
    
    first_row_valid = (first_row['SYMBOL'] == expected_first['SYMBOL'] and 
                      first_row['S/D QTY'] == expected_first['S/D QTY'] and 
                      abs(first_row['PRICE'] - expected_first['PRICE']) < 0.01)
    
    last_row_valid = (last_row['SYMBOL'] == expected_last['SYMBOL'] and 
                     last_row['S/D QTY'] == expected_last['S/D QTY'] and 
                     abs(last_row['PRICE'] - expected_last['PRICE']) < 0.01)
    
    print(f"First row matches expected: {first_row_valid}")
    print(f"Last row matches expected: {last_row_valid}")
    
    # 3. Market Value validation
    df['CALCULATED_MV'] = df['S/D QTY'] * df['PRICE']
    df['MV_DIFFERENCE'] = abs(df['CALCULATED_MV'] - df['MARKET VALUE'])
    discrepancies = df[df['MV_DIFFERENCE'] > 0.1]
    
    print(f"\nMarket Value Check:")
    print(f"Rows with market value discrepancies: {len(discrepancies)}")
    if len(discrepancies) > 0:
        print("\nDiscrepancies found:")
        print(discrepancies[['SYMBOL', 'S/D QTY', 'PRICE', 'MARKET VALUE', 'CALCULATED_MV', 'MV_DIFFERENCE']])
    
    # 4. Data type validation
    print("\nColumn Data Type Check:")
    print(f"SYMBOL: All strings of length 1-4: {df['SYMBOL'].str.len().between(1, 4).all()}")
    print(f"S/D QTY: All integers: {df['S/D QTY'].dtype == 'int64'}")
    print(f"PRICE: All numeric: {pd.to_numeric(df['PRICE'], errors='coerce').notnull().all()}")
    print(f"MARKET VALUE: All numeric: {pd.to_numeric(df['MARKET VALUE'], errors='coerce').notnull().all()}")
    print(f"REBATE RATE: All numeric: {pd.to_numeric(df['REBATE RATE'], errors='coerce').notnull().all()}")
    
    return df

def convert_pdf_to_excel():
    """Main function to convert PDF to Excel with validation"""
    print("\nExample file path formats:")
    print('1. C:/Users/kjone/Downloads/SSS_DETAIL_REPORT_FINAL_BASE_4JSX (9).pdf')
    print('2. "C:\\Users\\kjone\\Downloads\\SSS_DETAIL_REPORT_FINAL_BASE_4JSX (9).pdf"')
    
    while True:
        pdf_path = input("\nPlease enter the full path to your PDF file: ").strip('"').strip("'")
        pdf_path = os.path.normpath(pdf_path)
        
        if os.path.exists(pdf_path) and pdf_path.lower().endswith('.pdf'):
            break
        else:
            print(f"Error: File not found or not a PDF at path: {pdf_path}")
            print("Please check the path and try again.")
    
    output_path = input("\nWhere would you like to save the Excel file? (Press Enter for current directory): ").strip('"').strip("'")
    if not output_path:
        output_path = os.getcwd()
    output_path = os.path.normpath(output_path)
    
    output_filename = input("\nWhat would you like to name the Excel file? (Press Enter for 'stock_rebate_detail.xlsx'): ")
    if not output_filename:
        output_filename = 'stock_rebate_detail.xlsx'
    elif not output_filename.endswith('.xlsx'):
        output_filename += '.xlsx'
    
    full_output_path = os.path.join(output_path, output_filename)
    
    try:
        # Extract and validate data
        df = extract_data_from_pdf(pdf_path)
        df = validate_data(df)
        
        # Save to Excel
        final_df = df[['SYMBOL', 'S/D QTY', 'PRICE', 'MARKET VALUE', 'REBATE RATE']]
        final_df.to_excel(full_output_path, index=False)
        print(f"\nExcel file has been created successfully at: {full_output_path}")
        
    except Exception as e:
        print(f"\nError processing the PDF: {str(e)}")
        print("Please ensure the PDF is not encrypted and contains the expected data format.")

if __name__ == "__main__":
    print("PDF to Excel Converter")
    print("=====================")
    print("This script will convert your Goldman Sachs rebate detail PDF to Excel.")
    
    try:
        convert_pdf_to_excel()
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
    
    input("\nPress Enter to exit...")