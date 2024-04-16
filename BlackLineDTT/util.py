
import calendar
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import os
from bs4 import BeautifulSoup
import logging

def month_to_text(month_number):
    return calendar.month_name[month_number]

def createSession():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504,302])
    adapter = HTTPAdapter(pool_connections=300, pool_maxsize=300, max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)  
    return session

def getHeader(referer,accept):
    headers = {
            # 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0',
            'Accept': accept,
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'X-Request-Verification-Token': 'lFoIOxqfrWxguJgua3G0H6KKzCTeDBXY9-4CSkSJNSuMmrl3BHXnbbpdCsE3ANNAWh7wiGaWDy_n-QA8CEA6Lio37Tc1',
            'Content-Type': 'application/json',
            'Origin': 'https://hyatthotels.us2.blackline.com',
            'Connection': 'keep-alive',
            'Referer': referer,
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
        }
    return headers

def readExcelFile(excel_file_path):
    # Read the Excel file into a DataFrame
    df = pd.read_excel(excel_file_path,dtype=str)
    return df

def readCSVFile(excel_file_path):
    # Read the Excel file into a DataFrame
    df = pd.read_csv(excel_file_path,dtype=str)
    return df

def saveExcelFile(df,excel_file_path):
    # Read the Excel file into a DataFrame
    df.to_excel(excel_file_path, index=False)
    
def saveCSVFile(df,csv_file_path):
    # Read the Excel file into a DataFrame
    df.to_csv(csv_file_path, index=False)
    
def deduplicateDataframe(data,target_column):
        return data.drop_duplicates(subset=[target_column])

def removeProcessedRecords(source,tracking):
    merged_df = source.merge(tracking, on='reconciliationId', how='inner')
    return source[~source.index.isin(merged_df.index)]

def create_relative_directory_link(directory_name):
    return f'=HYPERLINK("{os.path.relpath(directory_name)}", "Open Directory")'

def find_all_elements_with_suffix(html_content,element_type, suffixes):
    """
    Finds all input elements within an HTML document where the id attribute ends with any of the specified suffixes.

    Args:
        html_content (BeautifulSoup): The BeautifulSoup object or a tag within it to search through.
        suffixes (list): A list of suffixes to match against the id attribute of input elements.

    Returns:
        list: A list of BeautifulSoup elements that match the criteria.
    """
    # if not isinstance(html_content, BeautifulSoup):
    #     raise ValueError("html_content must be a BeautifulSoup object 1")

    # Find all 'input' elements where 'id' ends with any of the specified suffixes
    elements = html_content.find_all(element_type, id=lambda x: any(x and x.endswith(suffix) for suffix in suffixes))
    return elements

def find_elements_containing_id_values(html_content, element_type, id_values):
    """
    Finds all elements of a specified type within an HTML document where the id attribute contains any of the specified values.

    Args:
        html_content (BeautifulSoup): The BeautifulSoup object or a tag within it to search through.
        element_type (str): The type of HTML element to search for (e.g., 'div', 'table').
        id_values (list): A list of values to match against the id attribute of elements.

    Returns:
        list: A list of BeautifulSoup elements that match the criteria.
    """
    # if not isinstance(html_content, BeautifulSoup):
    #     raise ValueError("html_content must be a BeautifulSoup object 2")

    # Find all elements of specified type where 'id' contains any of the specified values
    elements = html_content.find_all(element_type, id=lambda x: any(value in x for value in id_values if x))
    return elements

def find_element_containing_id_values(html_content, element_type, id_values):
    """
    Finds all elements of a specified type within an HTML document where the id attribute contains any of the specified values.

    Args:
        html_content (BeautifulSoup): The BeautifulSoup object or a tag within it to search through.
        element_type (str): The type of HTML element to search for (e.g., 'div', 'table').
        id_values (list): A list of values to match against the id attribute of elements.

    Returns:
        list: A list of BeautifulSoup elements that match the criteria.
    """
    # if not isinstance(html_content, BeautifulSoup):
    #     raise ValueError("html_content must be a BeautifulSoup object 2")

    # Find all elements of specified type where 'id' contains any of the specified values
    elements = html_content.find(element_type, id=lambda x: any(value in x for value in id_values if x))
    return elements

def save_file_with_unique_name(folder_path, filename):
    base_name, ext = os.path.splitext(filename)
    counter = 0
    while True:
        new_filename = filename if counter == 0 else f"{base_name}-{counter}{ext}"
        if not os.path.exists(os.path.join(folder_path, new_filename)):
            return new_filename
        counter += 1
        
def clean_folder_of_Support_files(folder_path,file_exception):
    # print(file_exception)
    for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            # Check if it's a file and if it's not the source.html file
            if os.path.isfile(file_path) and filename != file_exception:
                try:
                    # Attempt to remove the file
                    os.remove(file_path)
                    logging.info(f"Deleted: {file_path}")
                except Exception as e:
                    logging.info(f"Error deleting {file_path}: {e}")

def is_divisible_by(divisor,number):
    return number % divisor == 0

def to_safe_filename(s, max_length=255):
    if not isinstance(s, str):
        s = str(s)
    # Remove or replace invalid characters
    s = re.sub(r'[<>:"/\\|?*]', '_', s)
    # Truncate to max_length
    # s = s[:max_length].rstrip()
    # Remove leading and trailing spaces and dots
    s = s.strip(" .")
    # Replace spaces with underscores (optional)
    s = s.replace(' ', '_')
    # Ensure the filename is not empty
    if not s:
        s = "default_filename"
    return s

def change_tracking_filenames(directory,startname,endname):
    # Iterate over each file in the directory
    for filename in os.listdir(directory):
        if filename.startswith(f'{startname}-') and filename.endswith('.csv'):
            # Extract the number from the filename
            number = filename.split('-')[1].split('.')[0]
            # Generate the new filename
            new_filename = f'{endname}-{number}.csv'
            # Rename the file
            os.rename(os.path.join(directory, filename), os.path.join(directory, new_filename))

if __name__ == "__main__":
    directory='/media/william/Movies2/Blackline_Documents/ALG/Audit/'
    startname='verify'
    endname="restart"
    change_tracking_filenames(directory,startname,endname)

    
    