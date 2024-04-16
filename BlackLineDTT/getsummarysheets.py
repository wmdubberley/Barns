from blacklineDTT import DataProcessingUtility
import util
import json
import pandas as pd
import logging
from bs4 import BeautifulSoup
import os
from urllib.parse import unquote
import sys
import re
from yaspin import yaspin
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

with yaspin(text="Executing Script", color="cyan") as sp:
    if len(sys.argv) < 3:
        print("Usage: python auditfiles.py <config_name> <periodId>")
        sys.exit(1)

    config_path = f"config/{sys.argv[1]}.json"  # First argument after the script name
    
    periodId = int(sys.argv[2])  # Second argument after the script name, converted to integer

    with open(config_path, 'r') as file:
        config_data = json.load(file)
        
    periodfile=f"config/{config_data['source']['periodfile']}"
    base_url=config_data['source']['base_url']
    base_path=config_data['target']['directoryMetadata']['sharepointBasePath']
    instance=config_data['source']['instance']
    
    logging.basicConfig(filename=f'{base_path}/{instance}/sheet-{periodId}.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    utility = DataProcessingUtility(config_path=config_path,periodPath=periodfile,periodId=periodId)
    with open(periodfile, 'r') as file:
        data = json.load(file)
    df = utility.extract_data_to_dataframe(data)
    try:
        # get reconcilations.xlsx for period as rec_df and dedupe on reconciliationId
        periodPath=f"{base_path}/{config_data['source']['instance']}/Reconciliation_Sheets/{periodId}/reconciliations.xlsx"
        rec_df=util.deduplicateDataframe(util.readExcelFile(periodPath),'reconciliationId')
        # logging.info(rec_df.columns)
        totalRows=rec_df.shape[0]
        count=0
        for i,row in rec_df.iterrows():
            count=count+1
            percentage_completed = round((((count)/totalRows)*100), 2)
            utility.saveSummarySheet(row)
            sp.text=f'completed: {percentage_completed}%'
        
    except Exception as e:
        logging.info("Stack trace:", exc_info=True)            
  