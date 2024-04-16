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
from urllib3.exceptions import MaxRetryError  # Assuming Max retries exceeded relates to this
from concurrent.futures import ThreadPoolExecutor, as_completed

def download_file(href, base_url, directory_path, session, headers, config_data):
    full_url = href.replace("../..", base_url)
    try:
        # Ensure the save directory exists
        os.makedirs(directory_path, exist_ok=True)
        
        response = session.head(full_url, headers=headers, cookies=config_data['source']['request']['cookies'], allow_redirects=False)
        if response.status_code == 200:
            content_disposition = response.headers.get('Content-Disposition')
            if content_disposition:
                sfilename = content_disposition.split('filename=')[1].strip('"\'')
                sfilename = unquote(sfilename)
                sfilename = re.sub(r'[<>:"/\\|?*]', '_', sfilename)
            else:
                sfilename = "delete_me.ext"
            
            ufilename = util.save_file_with_unique_name(directory_path, sfilename)
            pathtoFile = os.path.join(directory_path, ufilename)
            if not os.path.exists(pathtoFile) and sfilename != "delete_me.ext":
                response2 = session.get(full_url, headers=headers, cookies=config_data['source']['request']['cookies'], allow_redirects=False)
                with open(pathtoFile, 'wb') as file:
                    file.write(response2.content)
                logging.info(f"{ufilename} saved ")
        else:
            print(f"Failed to download the file. Status code: {response.status_code}")
    except MaxRetryError as e:
        logging.error(f"Encountered a max retries error: {e}")
    except Exception as e:
        logging.error("An error occurred while processing the file:")
        logging.error(f"Exception: {e}", exc_info=True)

def parallel_process_files(location_dfs, base_url, directory_path, session, headers, config_data, writer, recId,numberoffiles):
    with ThreadPoolExecutor(max_workers=numberoffiles) as executor:
        future_to_url = {executor.submit(download_file, href, base_url, directory_path, session, headers, config_data): href for href in location_dfs}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (url, exc))
    writer.writerow([recId])

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
        with open(f"{audit_path}/{audit_restart_file}", mode='a', newline='') as file:
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
            session=util.createSession()
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                # 'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Referer': 'https://hyatthotels.us2.blackline.com/account/reconciliation/10397475',
                # 'Cookie': 'BLSIAPPEN=!cKqV+u9ysLjDLcJmiOBa4m4YLrxynJq2EYif57Movnyp1tk+v6/DXWz8OtS0af5AQfLzlr7YAcJ8wQ==; ASP.NET_SessionId=j4j0i0cgmaz0nu0fxu5pgcig; __AntiXsrfToken=9c73128a46f941978af8ea5516947624; __RequestVerificationToken=3uNN65SzwKXuPH26Ge9HCJCO00c6MbOiBp-a5hZq6qNTRdIbzWRM6Q0M5d7sIXwP9Su61-EP3VsX9rqpxxWVmCd9xnY1; .MIXEDAUTH=915BD6487D9588AEB066BDADBBC26D55B45DE240F170DC4FE8A48A8BCE7058963A01CE7DC4EF7B4B132D5AF42A91602AC225984CC34A5B53F8F0F12F19DB45A38A360365A32F8C5F3CDBBD9C877CEF9DA58CE59A687EC15594EF8A1EFCCB83BB4E5C1975975915DE1D327D68ED059D7366F686D4173788A150A39568A593D0C7E8CDA0E39D576296F325743E57461B92DC3C7019819ADD6E696C259037C0FDA8CA21179B569FC5D2760157D1183487E8F7B0B05088182FAA5DA4D58F2F76D1F4607FCE15B68FE98A519DA2FA53BC3098C3B6A763011E2CC0118D228BA7896A68DB6DC39DFFA32AD36F90F05DEA53A8A7BF5B4757AA1F9FD139667916B0AEC9C2A5AE43C49E21A028CE587D672D13BC996EB7D21E0D26EE6CA4CCC94BA205E7EF478E42D22E261B8D2F43BCF76D20C04E90E40ACBCB7065FBC0EE77F7860757DC3E27E7E9; .BLSIAUTH_711=1EA32FBB4C39AC57EF8D9A8E95DECEE9E147FC47ACE0F8A07952C61E3498401BF4DDB7A68E1D6E7F45730CB1E60D549C15E5C59FAD8CB8DDE67549A486909366762DCC51284CFD46E6BF765D795C15486C91942EABBD5FAD0DAC9933D726212FAD5D196AA7D8C2D0DED41C75509C25E3D967DAC4BAF29601379B73B41CBBA24923EC1A2BCFA3F6F1F56E2927929096A8D6EDA878CC354C1CD477AB4D137F5B8DF9675368BB414430FD14722669D3E765D1B596ADAC161CCCBD279AA87F5D32722AF7B67AE20D0396ABB84E1326A73F93C23BCF339E484B9C9F931D7C7ED1E14CADB8DCEB47C1896F4C5FFD9BBD711FE74DE079B55D17470EB082A1073BB75CFBF434B2259C3D30AA4BEE126AE42F2DF403D21DF39D7F747D7C03CB61DAA0782B13E1453C0AA0D2A9E7F2C0AE9E58E1F31DFA5CE1A580554B46876697C0095DA108C2687764DE7C05939656E650F81A04DC22C79844836B329DE988C4263F93CA5BE8CB8C10D4490A5987022D85ED4DAA50073FB0A8825C6A3F1ABF99CB795392FF9859B9A6BBCE91653AF06D88EAEAD2BF1BFC5AC80F5C58CD13C8A9E36F2FEEEBE098559B0319897543774F2542B7892CB15292',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
            }
            for i,row in filtered_df.iterrows():
                count=count+1
                if util.is_divisible_by(5,count)==0:
                    logging.info('running keep alive')
                    session.head(f'{base_url}/keep-alive',cookies=config_data['source']['request']['cookies'],headers=config_data['source']['request']['headers'])
                    
                recId=row['reconciliationId']
                logging.info(recId)
                try:
                    html_content=None
                    percentage_completed = round((((count)/totalRows)*100), 2)
                    sp.text=f" completed = {percentage_completed}%"
                    outputDir,filename=utility.getdirectory(row)
                    filePath=os.path.join(outputDir, filename)
                    if os.path.exists(filePath):
                        with open(filePath, 'r', encoding='utf-8') as file:
                            html_content = file.read()
                        # print(html_content)
                    else:
                        logging.info(f"download rec id: {recId}")
                        html_content= utility.saveSummarySheet(row)
 
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
                        parallel_process_files(location_dfs, base_url, directory_path, session, headers, config_data, writer, recId)
                        # for href in location_dfs:
                        #     full_url = href.replace("../..", base_url)
                        #     try:
                        #         # Ensure the save directory exists (adjust this line if running on Windows)
                        #         os.makedirs(directory_path, exist_ok=True)
                        #         # logging.info(periodId)
                        #         # logging.info(f"{base_url}/account/reconciliation/{row['reconciliationId']}")
                        #         # logging.info(directory_path)
                        #         # logging.info(full_url)
                        #         response = session.head(full_url,headers=headers,cookies=config_data['source']['request']['cookies'],allow_redirects=False)
                        #         # Check if the request was successful
                        #         if response.status_code == 200:
                        #             # Get the filename from the Content-Disposition header
                        #             # logging.info(response.headers)
                        #             content_disposition = response.headers.get('Content-Disposition')
                        #             # logging.info(content_disposition)
                        #             if content_disposition:
                        #                 # Extract the filename from the header
                        #                 sfilename = content_disposition.split('filename=')[1]
                        #                 if sfilename.startswith('"') or sfilename.startswith("'"):
                        #                     sfilename = sfilename[1:-1]  # Strip quotes
                        #                 sfilename = unquote(sfilename)  # Decode URL-encoded characters in the filename
                        #                 # logging.info(f'filename is: {sfilename}')
                        #                 sfilename = re.sub(r'[<>:"/\\|?*]', '_', sfilename)
                        #             else:
                        #                 sfilename = "delete_me.ext"  # Use a default filename or generate dynamically
                                    
                        #             # Save the content of the response to a file in the specified directory
                        #             ufilename = util.save_file_with_unique_name(directory_path,sfilename)
                        #             pathtoFile = os.path.join(directory_path, ufilename)
                        #             if os.path.exists(pathtoFile):
                        #                 logging.info("filenames: %s", ufilename)
                        #                 pass
                        #             else:
                        #                 if sfilename=="delete_me.ext":
                        #                     logging.info("change Config")
                        #                     print("change Config")
                        #                 else:
                        #                     response2 = session.get(full_url,headers=headers,cookies=config_data['source']['request']['cookies'],allow_redirects=False)
                        #                     with open(pathtoFile, 'wb') as file:
                        #                         file.write(response2.content)
                        #                     logging.info(f"{ufilename} saved ")
                        #         else:
                        #             print(f"Failed to download the file. Status code: {response.status_code}")   
                                        
                        #     except MaxRetryError as e:
                        #         # Specific handling for MaxRetryError which often encapsulates max retries exceeded scenarios
                        #         logging.error(f"Encountered a max retries error: {e}")
                        #         sys.exit(1)

                        #     except Exception as e:
                        #         # Print the exception message
                        #         logging.error("An error occurred while processing the file:")
                        #         logging.error(f"File path: {filePath}")
                        #         logging.error("Stack trace:", exc_info=True)
                        #         sys.exit(1)
                        writer.writerow([recId])
                    logging.info('#########################################')

                except Exception as e:
                    logging.error("An error occurred while processing the file:")
                    logging.error(f"File path: {filePath}")
                    logging.error("Stack trace:", exc_info=True)            
                    continue        

    except Exception as e:
        print("An error occurred during merging:", e)
        logging.info("Stack trace:", exc_info=True) 
    sp.ok(f"✔ ")

