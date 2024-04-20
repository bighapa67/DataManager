import xml.dom.minidom
import pandas as pd
from sec_edgar_downloader import Downloader
import os
import tabula

destination_path = r'C:\Users\Ken\Google Drive\Trading\SourceFiles'
filing_type = 'NPORT-P'
symbol = 'BGY'
start_date = '2023-01-01'
sharadar_csv = r'C:\Users\Ken\Google Drive\Trading\Research\Sharadar_Cusip_Ticker.csv'

# The xml_path must be copied from the folder, in the source directory, that the sec_edgar_downloader library created.
# Since the sec_edgar_downloader library creates a folder for each download, there must be a way to get the path to the
# folder that was created.  I don't know how to do that yet.
xml_path = r'\sec-edgar-filings'
xml_path_extension = r'\filing-details.xml'
xml_file_path = destination_path + xml_path + fr'\{symbol}' + fr'\{filing_type}'


# Download the XML file from the SEC Edgar website
def download_xml_file():
    dl = Downloader(destination_path)
    dl.get(filing_type, symbol, after=start_date, download_details=True)

    # Retrieve the path to the downloaded XML file
    # It's nice to know that the directory created by the sec_edgar_downloader library is unique to the specific
    # download.  In other words, if I download the same filing type and symbol again, the folder that is created
    # will have the same name each time.
    download_folders = [d for d in os.listdir(xml_file_path) if os.path.isdir(os.path.join(xml_file_path, d))]

    if len(download_folders) == 1:
        download_dir = download_folders[0]
        return download_dir
    else:
        print('Error:  More than one folder in the download directory')
        return


def process_xml_file(xml_file):
    # Read the XML data from the file
    with open(xml_file, 'r', encoding='utf-8') as file:
    # with open(file_path, 'r') as file:
        xml_data = file.read()

    # Replace '&' character with '&amp;'
    xml_data = xml_data.replace('&', '&amp;')

    # Parse the XML data
    dom = xml.dom.minidom.parseString(xml_data)

    # Get a list of all 'invstOrSec' elements
    invstOrSec_elements = dom.getElementsByTagName("invstOrSec")

    # Initialize an empty dictionary to store the parsed data
    holdings_dict = {}

    # Iterate through each 'invstOrSec' element
    for invstOrSec_element in invstOrSec_elements:
        # Extract values for each element
        cusip = invstOrSec_element.getElementsByTagName("cusip")[0].firstChild.data
        name = invstOrSec_element.getElementsByTagName("name")[0].firstChild.data
        valUSD = invstOrSec_element.getElementsByTagName("valUSD")[0].firstChild.data
        pctVal = invstOrSec_element.getElementsByTagName("pctVal")[0].firstChild.data

        # Store the extracted values in the dictionary using cusip as the key
        holdings_dict[cusip] = {
            "name": name,
            "valUSD": float(valUSD),
            "pctVal": float(pctVal)
        }

    holdings_df = pd.DataFrame.from_dict(holdings_dict).T
    holdings_df.reset_index(inplace=True)
    holdings_df.rename(columns={'index': 'cusip'}, inplace=True)
    holdings_df['pctVal'] = pd.to_numeric(holdings_df['pctVal'])
    holdings_df.sort_values(by='pctVal', ascending=False, inplace=True)
    return holdings_df
    # print(holdings_df)

def get_ticker_from_cusip(xml_df):
    # Get the data from the Sharadar csv and convert it to a dataframe
    sharadar_df = pd.read_csv(sharadar_csv)
    sharadar_df = sharadar_df[['cusip', 'ticker', 'name']]

    # CUSIP numbers are nine characters long.  Some of the Sharadar cusips are missing the leading zero, which needs
    # to be added to the dataframe.  Other cusips are actually several cusips concatenated together, in which case
    # I only want the first nine characters.
    sharadar_df['cusip'] = sharadar_df['cusip'].astype(str)
    sharadar_df['cusip'] = sharadar_df['cusip'].str[:9]
    sharadar_df['cusip'] = sharadar_df['cusip'].str.zfill(9)

    # Convert the cusip column to a string in the xml_df to facilitate the merge
    xml_df['cusip'] = xml_df['cusip'].astype(str)

    # Merge the two dataframes on cusip to get the ticker symbol
    merged_df = pd.merge(xml_df, sharadar_df, on='cusip', how='left')
    merged_df.drop(columns=['name_y'], inplace=True)
    merged_df.rename(columns={'name_x': 'name'}, inplace=True)
    merged_df = merged_df.reindex(columns=['cusip', 'ticker', 'name', 'valUSD', 'pctVal'])

    return merged_df


if __name__ == '__main__':
    # Download the desired XML file from the SEC Edgar website.
    # Retrieve the name of the folder that was created by the sec_edgar_downloader library.
    edgar_dir = download_xml_file()

    xml_file = xml_file_path + f'\{edgar_dir}' + xml_path_extension
    csv_write_path = xml_file_path + f'\{edgar_dir}'

    # Pull the desired data from the XML file
    xml_df = process_xml_file(xml_file)

    # Cross-reference the CUSIP numbers provided by running the sec_edgar_downloader with the Sharadar csv file
    # that contains the cusips and ticker symbols for all stocks in the Sharadar database.
    final_df = get_ticker_from_cusip(xml_df)

    # Write the data to a csv file
    # Remember that the date added to the file name is the date we searched FROM, NOT the date of the filing!
    final_df.to_csv(csv_write_path + fr'\{symbol}_{filing_type}_{start_date}.csv', index=False)