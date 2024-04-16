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
import csv


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
    audit_path = f"{base_path}/{instance}/Audit"
    os.makedirs(audit_path, exist_ok=True)
    logging.basicConfig(filename=f'{audit_path}/audit-{periodId}.log', filemode='a', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    audit_restart_file=f"restart-{periodId}.csv"
    
    file_exists = os.path.isfile(f"{audit_path}/{audit_restart_file}")
    if not file_exists:
        with open(f"{audit_path}/{audit_restart_file}", 'w') as file:
                    file.write('reconciliationId\n')
                    
    utility = DataProcessingUtility(config_path=config_path,periodPath=periodfile,periodId=periodId)
    restartDf = pd.read_csv(f"{audit_path}/{audit_restart_file}")
    
    with open(periodfile, 'r') as file:
        data = json.load(file)
    df = utility.extract_data_to_dataframe(data)
    # print(df)
    try:
        with open(f"{audit_path}/{audit_restart_file}", mode='w', newline='') as file:
            # Create a csv writer object
            writer = csv.writer(file)
            # get reconcilations.xlsx for period as rec_df and dedupe on reconciliationId
            periodPath=f"{base_path}/{instance}/Reconciliation_Sheets/{periodId}/reconciliations.xlsx"
            rec_df=util.deduplicateDataframe(util.readExcelFile(periodPath),'reconciliationId')
            restartDf['reconciliationId'] = restartDf['reconciliationId'].astype(str)
            merged_df = rec_df.merge(restartDf, on='reconciliationId', how='left', indicator=True)
            filtered_df = merged_df[merged_df['_merge'] == 'left_only'].drop('_merge', axis=1)
    
            # logging.info(rec_df.columns)
            totalRows=rec_df.shape[0]
            count=restartDf.drop_duplicates(subset=['reconciliationId']).shape[0]
            for i,row in filtered_df.iterrows():
                count=count+1
                recId=row['reconciliationId']
                logging.info(recId)
                try:
                    percentage_completed = round((((count)/totalRows)*100), 2)
                    sp.text=f" completed = {percentage_completed}%"
                    outputDir,filename=utility.getdirectory(row)
                    filePath=os.path.join(outputDir, filename)
                    if os.path.exists(filePath):
                        with open(filePath, 'r', encoding='utf-8') as file:
                            html_content = file.read()
                        # print(html_content)
                        soup = BeautifulSoup(html_content, 'html.parser')
                        support_files=0   
                        location_dfs =  utility.getfileElements(soup)
                        support_files=len(location_dfs)
                        directory_path = f"{outputDir}"
                        all_entries = os.listdir(directory_path)
                        files = [entry for entry in all_entries if os.path.isfile(os.path.join(directory_path, entry))]
                        number_of_files = len(files)
                        logging.info("#########################################")
                        logging.info(f"Number of files: {number_of_files}")
                        logging.info(f"Number of support files: {support_files}")
                        logging.info(f'path={directory_path}')
                        if number_of_files == (support_files+1):
                            writer.writerow([recId])            
                        else:
                            logging.info(filePath)
                            util.clean_folder_of_Support_files(directory_path,filename)
                                                  
                        logging.info('#########################################')
                    else:
                        logging.info(f"can't find {filePath}")
                        logging.info(f"download rec id: {recId}")    
                    # writer.writerow([recId])            
                except Exception as e:
                    logging.info("An error occurred while processing the file:")
                    logging.info(f"File path: {filePath}")
                    logging.info("Stack trace:", exc_info=True)            
                    continue        
    except Exception as e:
        print("An error occurred during merging:", e)
        logging.info("Stack trace:", exc_info=True) 
    sp.ok(f"✔ ")

