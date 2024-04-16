#!/usr/bin/env python3
import os,json,logging,time,requests,concurrent.futures,sys,util
import pandas as pd
from bs4 import BeautifulSoup
from yaspin import yaspin
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from csv import DictWriter
import traceback  # Import the traceback module
from urllib.parse import unquote
import re
from xhtml2pdf import pisa

class DataProcessingUtility:
    
    def __init__(self, max_workers=10, config_path='config.yaml',periodPath='period.json',periodId=0):
        # session=createSession()
        self.periodId=periodId
        self.max_workers = max_workers
        self.config_path = config_path
        self.session = util.createSession()
        self.load_config()
        self.load_variables()
        self.start_logging()
        self.periodPath=periodPath
       
    def load_config(self):
        """Load configuration from a YAML file."""
        with open(self.config_path, 'r') as file:
            self.config_data = json.load(file)
            
    def load_variables(self):
        
        # Now you can access your cookies as a dictionary
        self.cookies = self.config_data['source']['request']['cookies']
        self.period=self.config_data['source']['period']
        self.base_url=self.config_data['source']['base_url']
        self.base_path=self.config_data['target']['directoryMetadata']['sharepointBasePath']
        self.instance=self.config_data['source']['instance']
        self.headers=self.config_data['source']['request']['headers']
                       
    def downloadFile(self,full_url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'X-NewRelic-ID': 'VwAAU1VTCBAFV1NWDgEGX10=',
            'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjM2NzIzMDAiLCJhcCI6IjYwMTQxNzQ4OSIsImlkIjoiMmQ1OThkODllZjQ4MjNjZiIsInRyIjoiMTY3NmVlN2Y0MGQ1OWQ1MWY3Zjc2ZDgzYzRiZTdiZmUiLCJ0aSI6MTcwODA5MTk3NTA1MiwidGsiOiIzODkzNzkzIn19',
            'traceparent': '00-1676ee7f40d59d51f7f76d83c4be7bfe-2d598d89ef4823cf-01',
            'tracestate': '3893793@nr=0-1-3672300-601417489-2d598d89ef4823cf----1708091975052',
            'X-Requested-With': 'XMLHttpRequest',
            'Alt-Used': 'applelg.na3.blackline.com',
            'Connection': 'keep-alive',
            'Referer': 'https://applelg.na3.blackline.com/HomePages/HomeExec.aspx',
            # 'Cookie': 'GCLB=CILsnPK6lszvXQ; ASP.NET_SessionId=%7b5541d7e5ec53459faf1e32740d7035f3%7d; .MIXEDAUTH=8BE049C52C67041A965130AA026D1DED5E82451788928BA1B4692EAAF1F19CE529DD92D463D68C459819A43B3C778E9D411D28C98E0ECAA503015E62FE442A111A76E9CBC65A436D61D1EEF28D344B8433646F10BA885131B4BC67FC81673E4C48BBD629DB5CB3B4379D17D691AD8470A4F90997597E9BDBE2AB981377462E58DC0E4751F6400B43EE40FE1482035C1B0FB8B1F0E4074499F1247BA7A48736771AA5704C9D65DDD132A662BBB2FFB8AFC0EDB5F9D849D4EF2397EC4D653BFE286F5F4BB420A22EF06C021ED41E4592AACE84D86FF8D6FEAE64058762D58B7289810453FF74FB2E085C3F9F07A85F595BD962A249D607B6F581A40362209C17038F04458D1FB97B5D667DD88F2354C5FCA1EEC52164C5ABFE0C5EE72048F504630D030B3679A6D599F032C0A05AE3E2BE958742857AEE17E5F40AB6081631EB7AED44B43E66BB7722A1FE697805973311367129D9DB265E9F11147D4C6198232FFBFEAF26; .BLSIAUTH_22454=0A4EEDB6EAB5E5AC513FB1401578A897A4DCAF0F2C21E14F499855348621749D6CA50EBA816C303858B4181BF70021CDD26AA0C17D11D8D6A3B3AFFD01A19EC9341EF68978788CD04D9335B20D11D8BB92A3CFEAB095AF50D028F76CED469C253141045F774F5A3A38B5A731471CB2AA1FCEF0A9EB42D504C8AB30C3CDC8CADC97A6C3E565F1F65F4003D765129BC14D436B279603470F294B4B9B8DC5B9BE4734A2B3766C6C41C209E9F71BAAE180EC512B80F24C45AA13F4394E2034EB50E4DDA632667CE99413E0506A5E4C411C0CBAEEA2FDED27F77BF32F551A41B1420364283E3F2EC7712678A80430EA6ECCCE5D6CD6DC155367602A21D0894374C12B4333127E22DFF44DDD5685F586E9DFA7239CDE8A7CF24F0D25A3F44D878D92B78D7695EC645CFA066029E026EEE71E06D64E8408DD5577D2F691A43493BC79CD380941D72702F58FBFF3A56EB0C9E497CC480E4DF4B2BBC9F203F988AE6A09F47A143B708EF4F9A34A35DC144FF3CD754D2B1F5193E596EDD686E580F1146289270C9005; __AntiXsrfToken=77193587fab34235981ed60f6b854c27; __RequestVerificationToken=Z1AfrSyeWM-XSJAjwVavspKZ2DNUCFGodA--UW78xWJI2BsprnoQ3TPZNMOPir6RYfG9W-bifepTAxYvJDbuccTzKEc1',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            # Requests doesn't support trailers
            # 'TE': 'trailers',
        }
        return self.session.get(full_url,headers=headers,allow_redirects=False)
 
    def getFilename(self,full_url,directory_path):
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            # 'Accept-Encoding': 'gzip, deflate, br',
            'X-NewRelic-ID': 'VwAAU1VTCBAFV1NWDgEGX10=',
            'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjM2NzIzMDAiLCJhcCI6IjYwMTQxNzQ4OSIsImlkIjoiMmQ1OThkODllZjQ4MjNjZiIsInRyIjoiMTY3NmVlN2Y0MGQ1OWQ1MWY3Zjc2ZDgzYzRiZTdiZmUiLCJ0aSI6MTcwODA5MTk3NTA1MiwidGsiOiIzODkzNzkzIn19',
            'traceparent': '00-1676ee7f40d59d51f7f76d83c4be7bfe-2d598d89ef4823cf-01',
            'tracestate': '3893793@nr=0-1-3672300-601417489-2d598d89ef4823cf----1708091975052',
            'X-Requested-With': 'XMLHttpRequest',
            'Alt-Used': 'applelg.na3.blackline.com',
            'Connection': 'keep-alive',
            'Referer': 'https://applelg.na3.blackline.com/HomePages/HomeExec.aspx',
            # 'Cookie': 'GCLB=CILsnPK6lszvXQ; ASP.NET_SessionId=%7b5541d7e5ec53459faf1e32740d7035f3%7d; .MIXEDAUTH=8BE049C52C67041A965130AA026D1DED5E82451788928BA1B4692EAAF1F19CE529DD92D463D68C459819A43B3C778E9D411D28C98E0ECAA503015E62FE442A111A76E9CBC65A436D61D1EEF28D344B8433646F10BA885131B4BC67FC81673E4C48BBD629DB5CB3B4379D17D691AD8470A4F90997597E9BDBE2AB981377462E58DC0E4751F6400B43EE40FE1482035C1B0FB8B1F0E4074499F1247BA7A48736771AA5704C9D65DDD132A662BBB2FFB8AFC0EDB5F9D849D4EF2397EC4D653BFE286F5F4BB420A22EF06C021ED41E4592AACE84D86FF8D6FEAE64058762D58B7289810453FF74FB2E085C3F9F07A85F595BD962A249D607B6F581A40362209C17038F04458D1FB97B5D667DD88F2354C5FCA1EEC52164C5ABFE0C5EE72048F504630D030B3679A6D599F032C0A05AE3E2BE958742857AEE17E5F40AB6081631EB7AED44B43E66BB7722A1FE697805973311367129D9DB265E9F11147D4C6198232FFBFEAF26; .BLSIAUTH_22454=0A4EEDB6EAB5E5AC513FB1401578A897A4DCAF0F2C21E14F499855348621749D6CA50EBA816C303858B4181BF70021CDD26AA0C17D11D8D6A3B3AFFD01A19EC9341EF68978788CD04D9335B20D11D8BB92A3CFEAB095AF50D028F76CED469C253141045F774F5A3A38B5A731471CB2AA1FCEF0A9EB42D504C8AB30C3CDC8CADC97A6C3E565F1F65F4003D765129BC14D436B279603470F294B4B9B8DC5B9BE4734A2B3766C6C41C209E9F71BAAE180EC512B80F24C45AA13F4394E2034EB50E4DDA632667CE99413E0506A5E4C411C0CBAEEA2FDED27F77BF32F551A41B1420364283E3F2EC7712678A80430EA6ECCCE5D6CD6DC155367602A21D0894374C12B4333127E22DFF44DDD5685F586E9DFA7239CDE8A7CF24F0D25A3F44D878D92B78D7695EC645CFA066029E026EEE71E06D64E8408DD5577D2F691A43493BC79CD380941D72702F58FBFF3A56EB0C9E497CC480E4DF4B2BBC9F203F988AE6A09F47A143B708EF4F9A34A35DC144FF3CD754D2B1F5193E596EDD686E580F1146289270C9005; __AntiXsrfToken=77193587fab34235981ed60f6b854c27; __RequestVerificationToken=Z1AfrSyeWM-XSJAjwVavspKZ2DNUCFGodA--UW78xWJI2BsprnoQ3TPZNMOPir6RYfG9W-bifepTAxYvJDbuccTzKEc1',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            # Requests doesn't support trailers
            # 'TE': 'trailers',
        }
        response = self.session.head(full_url,headers=headers,allow_redirects=False)
        # Use a default filename or generate dynamically
        sfilename = "delete_me.ext"
        # Check if the request was successful
        if response.status_code == 200:
            # Get the filename from the Content-Disposition header
            # logging.info(response.headers)
            content_disposition = response.headers.get('Content-Disposition')
            # logging.info(content_disposition)
            if content_disposition:
                # Extract the filename from the header
                sfilename = content_disposition.split('filename=')[1]
                if sfilename.startswith('"') or sfilename.startswith("'"):
                    sfilename = sfilename[1:-1]  # Strip quotes
                sfilename = unquote(sfilename)  # Decode URL-encoded characters in the filename
                # logging.info(f'filename is: {sfilename}')
                sfilename = re.sub(r'[<>:"/\\|?*]', '_', sfilename)
            
            # Save the content of the response to a file in the specified directory
        if sfilename=="delete_me.ext":
            return sfilename
        else:
            return util.save_file_with_unique_name(directory_path,sfilename)  
                                
    def start_logging(self):
        if not os.path.exists(f"{self.base_path}/{self.instance}"):
            # Create the directory
            os.makedirs(f"{self.base_path}/{self.instance}")
        logging.basicConfig(filename=f'{self.base_path}/{self.instance}/sheet.log', filemode='a', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def getfileElements(self,htmlcontent):
        input_elements = util.find_elements_containing_id_values(htmlcontent,'input',['imgHasDocuments','ibDocsGridOpen','ibDocOpen'])
        filesList=[]
        for element in input_elements:
            try:
                if element and 'onclick' in element.attrs:
                    onclick_attribute = element.attrs['onclick']
                    location_href = onclick_attribute.split("location.href='")[1].split("';")[0]
                    if len(location_href)>0:
                        filesList.append(location_href)
            except:
                continue
        return filesList
    
    def extract_info_from_html(self,html_content):
        soup = BeautifulSoup(html_content, 'html.parser')
        # Find all tables
        tables = util.find_elements_containing_id_values(soup,['docsGrid','recItemsGrid'])
        # List to store each dataframe
        dfs = []
        for table in tables:
            # Extract the table headers
            headers = []
            for th in table.find_all('th'):
                headers.append(th.text.strip().replace(" ", "_"))

            # Extract the rows
            rows = []
            for tr in table.find_all('tr'):
                row = []
                for td in tr.find_all('td'):
                    # Check for input element with specific id
                    input_element = util.find_input_elements_by_id_suffix(td,['imgHasDocuments','ibDocsGridOpen'])
                    if input_element and 'onclick' in input_element.attrs:
                        onclick_attribute = input_element.attrs['onclick']
                        location_href = onclick_attribute.split("location.href='")[1].split("';")[0]
                        row.append(location_href)
                    else:
                        row.append(td.text.strip())

                if row:
                    # Adjust rows to have the same number of columns as headers
                    while len(row) < len(headers):
                        row.append(None)  # Append None for missing data
                    rows.append(row)

            # Create a DataFrame
            df = pd.DataFrame(rows, columns=headers)
            dfs.append(df)

        # Now 'dfs' contains a dataframe for each table
        # for df in dfs:
        #     print(df)
        return dfs

    def update_cookies_from_headers(self,headers):
        set_cookie_headers = headers.get('Set-Cookie')
        if set_cookie_headers:
            # In real scenarios, Set-Cookie might be a list if multiple cookies are set
            for cookie_string in set_cookie_headers.split(', '):
                # Simple parsing - for complex scenarios, consider using a library like http.cookies.SimpleCookie
                parts = cookie_string.split(';')
                key_value = parts[0].split('=')
                if len(key_value) == 2:
                    self.session.cookies.set(key_value[0], key_value[1], path='/', secure=True, httponly=True, samesite='Lax')

    def change_period(self,period):

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            # 'Cookie': 'BLSIAPPEN=!pKMQ5kF0em2j/9NmiOBa4m4YLrxynIpKzfyVAd0DtOfeer+MbX5DspFueFUEZba2rKfq/EWPQ0sfow==; __AntiXsrfToken=9f1ed8ac955d48c287beb6e63c79adbf; __RequestVerificationToken=7yi_-s_GnXiARdXGXYlaDOYplhVcLzd8K9qm5mRbiOdXom3wku3gcIZMFkvx8_LvCNnvIYIPl2DyYFnvT3vkhWEY9P81; ASP.NET_SessionId=u23dayhekpus035ygqvkzi5d; FedAuth=77u/PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz48U2VjdXJpdHlDb250ZXh0VG9rZW4gcDE6SWQ9Il80NTg3NGI1Zi1iMGNhLTRhZTUtODFkMS0xZmZiN2ZmYTNkOTctNjU5MDFCOTJEOUREODEzRUJGMEExRThBMkI3MDRGRTYiIHhtbG5zOnAxPSJodHRwOi8vZG9jcy5vYXNpcy1vcGVuLm9yZy93c3MvMjAwNC8wMS9vYXNpcy0yMDA0MDEtd3NzLXdzc2VjdXJpdHktdXRpbGl0eS0xLjAueHNkIiB4bWxucz0iaHR0cDovL2RvY3Mub2FzaXMtb3Blbi5vcmcvd3Mtc3gvd3Mtc2VjdXJlY29udmVyc2F0aW9uLzIwMDUxMiI+PElkZW50aWZpZXI+dXJuOnV1aWQ6NDg0Y2Y4OGEtODA2Zi00NjAyLWJhMmUtMzgxODdjMzJmNWE2PC9JZGVudGlmaWVyPjxDb29raWUgeG1sbnM9Imh0dHA6Ly9zY2hlbWFzLm1pY3Jvc29mdC5jb20vd3MvMjAwNi8wNS9zZWN1cml0eSI+QUFJQUFFS3JQZDZ4bVN5NFhKWGJoUkh5c2hSNDlEdmJLVnBSM3M1aHpKZVlCaGJKRVo1RzhudHg4K3Zmc1lLNlUyaXBhdFVZZXRJZi9TZmN0WlBjWFRMdmEvT1pGSEV6cHpyZVhMR0taS1JmMFFodzVaSXA1WHRjVUM0T0d1SzJqOUUwZnpZWE9lUVhKaUg2NHVuWVBkYS9HczJOellTN1ZDaWJxaU1teTFrNU8xUzRWMFFIcDF3S2d2L2JJWVBmbnJpSjhQc1BNeVpwWVpOM1lIb2ZlWnJKQnVqQ1I5dzRHRks5eUJPeDh4dFhDZTNXclVoZ1FFWm5xK0t3aW5rQ21vRTFnc1RsSGFCYWUyQ3dkd200dzVUbi8vR1NhSzVGWmg1VGF3VytMc3Z5N25MdmJxUWZSdGlmaDFnYXBBOTlwTGs0YTBHMU82WVljeVAzbmkvZDhQL0hXZzI1QXpnanZka0Q4WWRoUVZjNGFVNTlCcTNNYXVBc1FZMStRWDFubmo1ZVVxa3dHNEJqMGN2cFdpNjB0NzJsM0J0MThybDZRdXQ4YmhFeUVhejI0VnV4UVB3T2JLOWM0YVVoMzZueW1pWm1OYTkwVVhzNzF0RmZFeW1TWG5SQ3JTUUN4aUtxWkZFZVVUb1VmN2E5UU9qTWtTeDZ5QithVWRwcUYwUXhJbDFWMGtwR2JjcWVnVHd6RnFPOXI5Y0E2KzlieHVnNG9ld3k1RjIwQ3VNekRwYmlYWTJIYzBqOEVLMkRLaUkzODlkZ3JRZXU4TVM4b09KTHdrTFFpQi81endxOVdJN0tLd3lRd3Rrb3VGMUZFUUVheGVmQThORXQxMTRUT21HcXowejA1bzN0TFRuNzh6eFNmUzFIbExTSVhGcEM0L083VkkzNnA0bU9oSmJKTEMyTkx0clpkMjNwNldXb1c5b1lMZDhjRjBGbW1KTUF6QXMvSk0rRjU5eU1ZcjRBQWdBQXNHbk1DV21YMDluaE0zaFdoYzNOMThXbEJubzVNMWY0T3NTRlZmTEhQdFNoWHJwaUhlcUpLV3hGcEVqUTloVUd2UVI2ZTNRcDJ2TXYvenNQbGJNSktOR0syMUxYZHhSWDBLaDhFdjNKNW1EWnVGaDNHbXlhVjdieURJVE16T3JxOEpuWUpKVVhSMzZWNHZhYjhBQTJUZlFRVC9jNFBKM0FIcmJnNGk5bDBWaVRrK3FGdWQvZVNLc2JXaUdoR045V0NTUzd6bWRLRHpaZ3dhNWFHSDV3cHBSUFpGcUEyQUIwTnFVakIwMUcyZ3E1S2hHMVkrZ0dlSVl2STNzTmduZmZiTnJQcGJDbGhFQ085MWZJMnZnWDNuSnBpaE0yYTY4L1BKWDJuQlpCdVZDeFpteGlRcDZUZ2Z4eGNBQ3pNdG9jNWNk; FedAuth1=VmlJc2ZOMWV4M0FpS1ZCajlwQS80Umkxb3VYa1VpbnBQSkpGSTRFV3Zibzg1YnVQcnFuQVJIazBDM1YvcStRWmo5ZjlKTDgrblIxY1BNb0padjFCcXVQbThxWlgvWFg1TE93Z3R3dG5tcnN0SHNUcERkOVBjcStRSW15V012Uk45Y2kvekZraC9yQVdBbWd4N3RjOGEwbHdNZ1hacWprdHlVeDVtRWF1L0xCT2hSckY4V29FNzRFU21Mc0lZY2Y3ZzA3ZExxaDllRnhLQmNOVDZxbHlPQ1pyeDl6SGJqRUczRnM4Ym1jRUt6Y0pSS0tZT2R0RXNueHFJZ1puYmFwNS9OV2RmRmljYXRkY1ZSNURMOFJHbC9aY3JCNyticXpRVW1Yd3l1M0Y4SDBDMWV3bnNtQStmdTM4WWZUSDRKVUpMVmlqalJUOXc0UThkREdYSHNrdHcvODN4VHcwYzNseHd1WVA3TXhMUUFnQUFEdENZejAwalZMVnpiR0FDaDZjT0ZtRzlhenJ1dU5qWjRQSkR4ay84OGE4SzlVc3ZDTE1JTTcraTJaR3NhdFo3ZDNBVlV1V0ZOOGNRSkZrZDZPVmdGVkpOaURBMzBLVFUwVFZObVMyUVFCd0J0ZTdqQ0xhYVhTaGdGcWxSbXYwVEtxOEZyYkl2WVQ3cTVocmRtNDFSam1LKzFDcHJhdVQ5WnRxd1FuZVoxWjR6R3VkSjZhbTNDVFNWamxsMUx0MHFoc1ozV1pwcnRTMFphUWt2OVhBMzNLU29UcGVBa3ZzTGxJWmJkdW85MmgyWnRtb2FoNkpWUGFNekRhTFhKSDQxS0VlZ09KQlBtSmVLc09hYkN4MTVNcmcyak14Njd1WjJrdHU3THNxbW44bDdMSHB5L01uQlFsUFZzMnI1UXVJZElQZUduaGN6Z0pTV3lwcVdFdnNtR0tiZzU4MVhVMHlQYWp2OHV0ZWRBTWFIL2FYT3ZmV25qRGhOV3E1U2prQ0d1RWFNSDEvVmpqYlVIWFpTRG1kdUZJeFFhbCtCRTVVWGhIakJXUERraXV0MndMOHpSSWxGYkNldXpzYU95cEVBcU5CVWxNMDJQSDY3QXJjMFk4UkdsNGdDa2NnWTEvb1JHb2ZQSEJCZHg2bklwc1NpZEdudEljLzQ4SjNZam5aRFZ1WHFBWjBIMGVDOTdocFFhZmF5WFB4elBLbFJnZUNNWW1BL2EraXdDZGNtaG1UTW4vYTByYXBJeS85RW9TaVN5Y1c3VE5RV2I5c0hsVUtuWkNyRnZFcVZ2cjMwN3NRV2VYeFlQV2hPN0syK09Zd0dBc3B3RWd5TE0zS0krZkJpUlVwNWcwakdMU2hjMy9tSlpQSzFXcUcySi9rMlZXYkRMNzZJR1lzNGViSHNwQ2p3Skg3bEtBSkk0RDltSWozTEZSRm5PWVcwS0h1NGFtREdhYlFsZ1c1Uml0NmJhUDlva2VYNWhxamVpc2piM3ZYQkUrZ3ZPbHZzTFdpTkJYZGxabzlqV1E2cGVrMUtER1g3VkxvMXdybURjYitqL2x3bjRTcEtjN2RmUklaVkxvanEwUnF0ckhYbDdLMW5BNW5paHZvUTZYVUJQZjVrZkRhbm43Z01qVlphRi81dm1aNkR0aitVdGdId1lONzRvV3FsTFRlSllMRE5xcmNkdTJDNVR2VFNFV0tXaTMvOHFpbWZnS2pNakFwU2k5M1dpbmpmQkFWMmsrK1hKWmNCWjVDaEtmbWgvZk1YUXVuNHVEcU48L0Nvb2tpZT48L1NlY3VyaXR5Q29udGV4dFRva2VuPg==; .BLSIAUTH_711=5CF8263A780DC79AFCE1DE9ED396B521F19E593FA178E45C673502965B372FD69E5B6B87B0B491B6272C16F5467BBD2E73B4963035BA5B938805EDD07249BE4B19802B431DF6E11D292A1FA6FAD2B8B2156A90C8FAFCDAA7C9CAA3B698D8FA304B25CCD606C6FF267DC5A3659673A8B1615DD53AB20CF81149B77714387193DEF9A8A339B73E3EDBA13FA77DE90042CD9301DAE7F8E495C80BBB1D0FC09748B6F95B57D833890BBF39AAA17A011CB9C734E1F5396D00B4F7EC05A1F3814D9EDD26AA52D62B60A7344D6540103130864A0CAB927934F6A3D33CA01152998D4396FC6810FBD470E9E147E8AC2C3FC76113D79927E05F946C665242E40044E207E543414D86B3B76D25C22A4CD504CE25B24876D39514258376810889B04EC3EB92557970A504C2BDB35689DFA225294D3033B05B83526EF798034396F60D71B7DB15D9F731AC9674BD0115A9076A9F160CE8922A0D680CF98414FBE6D1719E415E301CC378; .MIXEDAUTH=1A16D9D18506428703FED7E4BB08161AC69AB1C92FB5DC712A1CF672AF1A1006841849552115B6F898CE48A41CD57269E73CD26D9DE27CF93C0AE6FC70979F3A1D0CBB01AF9965E8334CF06EE8B6E99A31BF15BE424ED88C53E9F1937EE30731A9EF42C349300E677612256ACD2640418017CF0CED08EEB3A9FB11A17D60D3AAABDB8174D9B73F6D534F277175FFBF0304FD2F102FE10EDF2EEFBE86389523E48A8ED99648B728280E7CAB1A7C58FA22ADAFDF9C481E8A1AC5F6E1755DFC9FEEDF205A79938C92235C8D1CBEC5573165E1CA8FF78A18B09BE6EE58BA074E5D26D4697B0E46E9FF0FF26ED0D165A941FAB5F881C355CED3E555EFA5C5451D546865ECC2688EEB5ACD3B826A46707EF14B9FF20422B4B1AC98E310916865AE0CFE70C3ED8F83E88B725822D124A1902070F9749E19AE402BF85E9FBC1547FA419C2680B177',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
        }

        params = {
            'periodId': f'{period}',
        }

        response = requests.post(
            f'{self.base_url}/period/selected-period/edit',
            params=params,
            allow_redirects=False
        )
        if response.status_code == 200:
            self.config_data['source']['request']['cookies'].update(response.cookies.get_dict())
            with open(self.config_path, 'w') as file:
                json.dump(self.config_data, file, indent=4)
        else:
            print(f"Request failed with status code: {response.status_code}")

    def getPeriodList(self):

        response = self.session.get(f'{self.base_url}/api/period/list', cookies=self.cookies, headers=self.headers)

        if response.status_code == 200:
            # Convert the response content to a JSON object
            # print(response.json())
            data = response.json()
            
        # Save the JSON object to a file (e.g., response.json)
            with open(self.periodPath, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            print(response.text)

    def extract_data_to_dataframe(self,json_data):
        extracted_data = []
        if 'periods' in json_data:
            for period in json_data['periods']:
                if 'months' in period:
                    for month in period['months']:
                        period_end_date = datetime.strptime(month['periodEndDate'], '%Y-%m-%dT%H:%M:%SZ')
                        year = period_end_date.year
                        month_number = period_end_date.month
                        period=month['id']
                        extracted_data.append({'id':period,'Year': year, 'MonthNumber': month_number,'url':month['url']})
        df=pd.DataFrame(extracted_data)
        # df.set_index('id', inplace=True)
                    
        return df

    def writeProgress(self,progress,error):
        if error:
            reportName=f"{base_path}/{config_data['source']['instance']}/tracking/{period}-errors.csv"
        else:
            reportName=f"{base_path}/{config_data['source']['instance']}/tracking/{period}.csv"
        field_names = ['reconciliationId','support_files','date_downloaded']
        # print(progress)
        # Open CSV file in append mode
        # Create a file object for this file
        with open(reportName, 'a') as f_object:
        
            # Pass the file object and a list
            # of column names to DictWriter()
            # You will get a object of DictWriter
            dictwriter_object = DictWriter(f_object, fieldnames=field_names)
        
            # Pass the dictionary as an argument to the Writerow()
            dictwriter_object.writerow(progress)
        
            # Close the file object
            f_object.close()

    def getdirectory(self,row):
        path=""
        filename=""
        with open(self.periodPath, 'r') as file:
            data = json.load(file)
            df = self.extract_data_to_dataframe(data)
            row2 = df[df['id'] == self.periodId]
            year = row2.iloc[0]['Year']
            month_number = row2.iloc[0]['MonthNumber']
        match self.instance:
            case "HYATT"  : 
                # Corporate / Hotel (Defined by key6) [510 or Blank = Hotel] [!510 or !Blank = Corporate]
                
                #   Spirit Code (Defined by accountNumber)
                #       Year
                #           Month
                #               EntityCode (Defined by EntityCode – 5 digit code parsed)
                #                   Account (Defined by EntityCode - 5 digit code parsed) – key3 – key4 – key5
                #                       Filename: YYYYMM_[Account String].html
                #                       Direct filename from blackline export
                typex=""
                if row['key6'] == '510' or row['key6'] == '' or pd.isna(row['key6']):
                    typex = "Hotel"
                else:
                    typex = "Corporate"
                entiycode=row['entityCode'][:5].strip()
                spiritCode=row['accountNumber']
                account=""
                
                if typex=='Hotel':
                    account=f"{entiycode}-{row['key3']}-{row['key4']}-{row['key5']}"
                    account=re.sub(r'[<>:"/\\|?*]', '_', account)
                    path=f"{self.base_path}/HYATT/{typex}/{spiritCode}/{year}/{ util.month_to_text(int(month_number))}/{account.replace(' ', '_')}"
                else: 
                    account=f"{entiycode}-{spiritCode}-{row['key3']}-{row['key4']}-{row['key5']}-{row['key6']}"
                    account=re.sub(r'[<>:"/\\|?*]', '_', account)
                    path=f"{self.base_path}/HYATT/{typex}/{year}/{ util.month_to_text(int(month_number))}/{account.replace(' ', '_')}"
                filename=f"{year}{month_number}_{account.replace(' ', '_').replace('/', '-')}.html"

            case "ALG" : 
                # ALG
                #   Year
                #       Month
                #           EntityCode (Org Unit ID-EntityCode) (required lookup on ALG_OrgStructure table)
                #               Account (Org Unit ID-EntityCode-AccountNumber-AccountName
                #                   Filename: YYYYMM_[Account String].html
                #                   Direct filename from blackline export
                df = pd.read_csv ('org.csv')
                entiycode=f"{df.loc[df['Organizational Unit'] == row['entityCode']].iloc[0]['Org Unit ID']}-{row['entityCode'].replace(' ', '_')}"
                account=f"{entiycode[:4].strip()}-{row['accountNumber']}-{row['accountName'].replace(' ', '_')}"
                account=re.sub(r'[<>:"/\\|?*]', '_', account)
                filename=f"{year}{month_number}_{account.replace(' ', '_')}.html"
                path=f"{self.base_path}/ALG/{year}/{util.month_to_text(int(month_number))}/{entiycode}/{account.replace(' ', '_')}"

            case "BARNES":
                entiycode=row['entityCode'][:4].strip()
                reconciliationId=row['reconciliationId']
                filename="reconciliation.pdf"
                path=f"{self.base_path}/{entiycode}/{year}/{util.month_to_text(int(month_number))}/{reconciliationId}"
            
            case _  : 
                print("instance not supported. current support is for HYATT, ALG if tyring to do one of the supported then verify you have proper spelling and case")
        return path,filename

    def keepAlive(self,session):
        self.session.head('https://hyatthotels.us2.blackline.com/keep-alive')

    def saveSummarySheet(self,row):
        reconciliationId = row['reconciliationId']
        outputDir,filename = self.getdirectory(row)
        # print(outputDir)
        os.makedirs(outputDir, exist_ok=True)
        url = f'{self.base_url}/account/reconciliation/{reconciliationId}'
        html_content = ""
        response = self.session.get(url,headers=self.headers,cookies=self.cookies,allow_redirects=False,)
        if response.status_code == 200:
            html_content = response.text
        else:
            print(response.status_code)
            
        # Parse the HTML content with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        anchor_element=soup.find("a", id="ctl00_ctl00_contentBody_cphMain_SaveCertifyButtons1_PrinterFriendlyLink")
        href = anchor_element.get("href")
        # Make a request to download the page
        response = self.session.get(href ,headers=self.headers,cookies=self.cookies)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')  # You can use 'html.parser' as well
            logging.info(f" Processing Reconciliation Id {reconciliationId}. ")
            os.makedirs(outputDir, exist_ok=True)
            with open(os.path.join(outputDir, filename), 'wb') as file:
                if self.config_data['summarySheetType'] == 'pdf':
                    pisa.CreatePDF(html_content, dest=file)
                    logging.info(f"HTML page saved to file  {outputDir}/{filename}")
                else:
                    file.write(response.content)
                    logging.info(f"HTML page saved to file  {outputDir}/{filename}")
        else:
            logging.info(f"<<< Failed to download the page: Status code {response.status_code} >>>")
        return response.content

    # TODO: adjust for self
    def process_row(self,row, headers, cookies, session):
                    support_files=0
                    errors=False          
                    reconciliationId = row['reconciliationId']
                    outputDir,filename = self.getdirectory(row)
                    url = f'{self.base_url}/account/reconciliation/{reconciliationId}'
                    html_content = ""
                    response = session.get(url,headers=headers,cookies=self.cookies,allow_redirects=False,)
                    if response.status_code == 200:
                        html_content = response.text
                    else:
                        print(response.status_code)
                        errors=True
                        
                    # Parse the HTML content with BeautifulSoup
                    soup = BeautifulSoup(html_content, 'html.parser')
                    # Find the anchor element by its id
                    # logging.info(soup)

                    anchor_element=soup.find("a", id="ctl00_ctl00_contentBody_cphMain_SaveCertifyButtons1_PrinterFriendlyLink")
                    # logging.info(anchor_element)
                    # anchor_element= getMainDoc(url)
                    # print(anchor_element)
                    # Check if the anchor element was found
                    if anchor_element:
                        # Get the href attribute value
                        # logging.info('checkpoint 1')

                        href = anchor_element.get("href")
                        # Make a request to download the page
                        response = session.get(href ,headers=headers,cookies=cookies)
                        soup = BeautifulSoup(response.content, 'html.parser')  # You can use 'html.parser' as well
                        # Check if the request was successful (status code 200)
                        if response.status_code == 200:
                            logging.info(f" Processing Reconciliation Id {reconciliationId}. ")

                            # output_folder = f"Documents/{accountname}/{id}"
                            os.makedirs(outputDir, exist_ok=True)

                            # Extract the filename from the URL
                            # filename = os.path.basename(f"{reconciliationId}.html")

                            
                            # Save the HTML content to the output folder
                            with open(os.path.join(outputDir, filename), 'wb') as file:
                                file.write(response.content)
                            try:
                                location_dfs =  self.getfileElements(soup)
                                support_files=len(location_dfs)
                                directory_path = f"{outputDir}"
                                if support_files>0:
                                    for href in location_dfs:
                                        full_url = href.replace("../..", base_url)
                                    try:
                                        # Ensure the save directory exists (adjust this line if running on Windows)
                                        os.makedirs(directory_path, exist_ok=True)

                                        response = session.get(full_url, headers=headers,cookies=cookies,allow_redirects=False)

                                        # Check if the request was successful
                                        if response.status_code == 200:
                                            # Get the filename from the Content-Disposition header
                                            # logging.info(response.headers)
                                            content_disposition = response.headers.get('Content-Disposition')
                                            logging.debug(content_disposition)
                                            if content_disposition:
                                                # Extract the filename from the header
                                                sfilename = content_disposition.split('filename=')[1]
                                                if sfilename.startswith('"') or sfilename.startswith("'"):
                                                    sfilename = sfilename[1:-1]  # Strip quotes
                                                sfilename = unquote(sfilename)  # Decode URL-encoded characters in the filename
                                            else:
                                                sfilename = "default_filename.ext"  # Use a default filename or generate dynamically
                                            logging.debug("filenames: %s", sfilename)
                                            # Save the content of the response to a file in the specified directory
                                            file_path = os.path.join(directory_path, sfilename)
                                            with open(file_path, 'wb') as file:
                                                file.write(response.content)
                                            logging.debug(f"File saved to {file_path}")
                                        else:
                                            print(f"Failed to download the file. Status code: {response.status_code}")
                                    except Exception as e:
                                        # Print the exception message
                                        print(f"An error occurred: {str(e)}")
                                        # Print the stack trace
                                        traceback.print_exc()
                                        

                            except IndexError as e:
                                    logging.info(f"<<< error with reconcilition ID: {reconciliationId} >>>")
                            logging.info(f"> HTML page saved to file  {outputDir}/{filename}")
                        else:
                            logging.info(f"<<< Failed to download the page: Status code {response.status_code} >>>")
                            errors=True
                    else:
                        pass
                    progress={'reconciliationId': reconciliationId, 'support_files': support_files, 'date_downloaded': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    self.writeProgress(progress,errors)
                    return progress,errors
    # TODO: adjust for self
    def process_sheet(self,session,row,sp):
        year = row.iloc[0]['Year']
        month_number = row.iloc[0]['MonthNumber']
        month_name = util.month_to_text(int(month_number))
        sp.text=f"Processing Period: {month_name}, {year}"
        periodPath=f"{self.base_path}/{self.instance}/Reconciliation_Sheets/{self.periodId}/"
        headers = {
            'Accept': 'application/json; version=2',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            # 'Cookie': '__AntiXsrfToken=f083162b548949718c1bd194fe4609a9; BLSIAPPEN=!Uc7ijuI4fl4DiSJmiOBa4m4YLrxynGQBMy/yGJoBqSbR9232ER7BFktdzVQNo80nwuyUS4pBXIBo1w==; __RequestVerificationToken=LnOYZ5iGRTct5n39TiFrCJQF27k7dHFXDHqdKOCwT_hYjdFm6symBt0UrnIqN3bn0e9uDa6aOUz713RCjGrmSGIHYwo1; ASP.NET_SessionId=sse0q3qxa2mr5vapdk2eymoy; FedAuth=77u/PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz48U2VjdXJpdHlDb250ZXh0VG9rZW4gcDE6SWQ9Il8yNzY2ZWU3ZC0zMTM3LTQ5OTUtODg2YS0xYTkwYWJjMWExMGYtMTExRkFFQjhDNjg3MThCOTkyM0Q2Q0JENjAwRDQ5RDIiIHhtbG5zOnAxPSJodHRwOi8vZG9jcy5vYXNpcy1vcGVuLm9yZy93c3MvMjAwNC8wMS9vYXNpcy0yMDA0MDEtd3NzLXdzc2VjdXJpdHktdXRpbGl0eS0xLjAueHNkIiB4bWxucz0iaHR0cDovL2RvY3Mub2FzaXMtb3Blbi5vcmcvd3Mtc3gvd3Mtc2VjdXJlY29udmVyc2F0aW9uLzIwMDUxMiI+PElkZW50aWZpZXI+dXJuOnV1aWQ6ZDljZWMzODQtYzg2My00MGJiLWE4MWItOTIyNzJkNDE3ZTg2PC9JZGVudGlmaWVyPjxDb29raWUgeG1sbnM9Imh0dHA6Ly9zY2hlbWFzLm1pY3Jvc29mdC5jb20vd3MvMjAwNi8wNS9zZWN1cml0eSI+QUFJQUFJb3hxR2Z3anVKcWl6M1NxSmdiczJ3dnNYUG5TSEV5U0NjbFI4LzNXYVJ2SEFuTmdMTHpGanJaWlUyTDdBVHlzZFU0OG5zeC9ibnMzK2RFc1hkMnFhWGVHRmxkMTZ0VG1RQnRULzN6QXVMWFMyNVVTeDNwcmN3bWEzSUVHOG9aaWE1UG0yV3lnRURhbXpSWEIwdHRERWJDeEpXOFdXTWlRbWp1OG00bXZpN3RTTTJuQXNZejdsVDBxdHZHcHhTWUtaMmJVb1VTdjFzTXRCeTBrRjczTHl0TjVEOHBVWTJMY0szSmM2b2hwcWFQT3pwbHZYVm9EU3lHUHFMYmxQNWxQVGR3Mm9iTjBicXdqZk4rRllreDdJTzdHbVUzNUpZNVJxRXMrODI2c2NoWHBVbjJCeWE2ZFIyaWFndksyYXR3eDJZOUt1L2k5bmpGOGFHSThFQzBBZGpDY09FMVhXMTdDR0pVZFIzeVpCcnJzUDVKYWp3dysrWXhWcnZHU1FjSFVreEdGNnVGQnhwdXpuZmdFSE52bHB0UFlsNmpNbHVNYUJDOFhRSEJsdzNLYVpsWnZiUitXd3FqQk5mZWFyMlFIbmJDT1pmRDJGdElmSCtkY0grRmtFeExudmhUYzd1Y0U3UHFJOEM3YzdiekhjVmkxTlB1L0lER0I0YzNDUkRKcmFkeVcxZG1UMFF4c2haZW1NMS9PRTRmRjlTREZra3VXTHBFMzlwVGxscHVMUUdNQ0NranVGRGhGTjFYVWViOXRPTG04TFJFTEpZYVdCWld3MVZDY1djMmFMajg2Ym13TUVWZDFjbk5vcnQ1NjdUVVMzazRXdVFCem5WVE1SWjBCUkNUSUhSbzBjalBoZms0SnNoRFVTYmhPUllpR0FpMTVtWnVCQkdFSk51Mkx0clpkMjNwNldXb1c5b1lMZDhjRjBGbW1KTUF6QXMvSk0rRjU5eU1ZcjRBQWdBQWV3RXoxUFA4bUZUc1JnTVdGVWhGbVp2dFVTSGV5aS9lKzFQbkRUN1lkNnVLRE1BQTNWaVpORGQyZHdrWGVqL3lJNFFpRExjWjVtUU5CeUVjYldkemdBclg5QXhGN1F2TEY0VVZ0bU11SERGN3Q2WENrcTlleThEMXBzMzc4c1ppY04yRGJ3MUhudm9IQ01NcFJ4OTBqTGlaM1lRZnJFRHcrSWpHamFrK3lHWFhYSjNYODdlazRWR2h1M2Y4UWpnamdRVVI4M2Y4TnFVaDBQNVhBUlRTc01VQlNlZU96b3NSSnJZZUpjYWx0a3lXMVd4dEl1cmFlTEdITW5sb0F4RkF6Z3EwK2JETnJBWnAzT2gzOGdsMU1qd3dVTVkyWXBySU9NZVJucTRvWWU0NVNnMFkrNitGSFdyc2Jva3R5VktsMWZO; FedAuth1=dkY4UFBzTVVyb25oMmtFUnRGMUYvYTU5ZTFVWVlMNThBWVFacXIzNUJkTVhBYk9EWjZqTzVnVDFseWlUbmhZUWFOVmFxRERBVjRXcTUwRTJFQVVSMDMyd3prbUMyUHNWcjlIV2F0MEw0akxQQmRUKytwdWRGc0JLcGc5eG5CU0ZGSXRxU0RpVU9zN2E0NDVrTmNZTE12WFJCblNNR1BwZ0MvODZUOTZRSVBkcUEwYkk0UCtVaGh6bGhQN1U5UTUzQitmdTlzVzcrRkRlUlE3R04rbEt3VjR6Z21FK3NFUWE0Z2RrbmplM01qV01aemlaeHpaZExueVQySks3bVEwOUdqYUdHV2lQcmlCcG42TTNsdjl4aWRMblNDK01DMlJscmZ2OEVXZmN3eVdRK253WE1lZVhqV0VNRi92Mzd4R1lkZjBIRm9RODQwV1cwVTNkajFxbUpMUlI2MWtZcDJERjVXQ3lBRmtqUUFnQUFhamlVcXVFTHFocjVrSXNFemtUWGc3VzNUMlNXNlhkZXJMdGdoVE5lbys0SWtRRGFZSmplSmFONFpKQWQxRkNoK0Zydm9XajVuSmN6VVRKS3hHYThGODkrdGprMmxiRE11WFgxL2M3d01SZjRHcHpabDBlVG5nUU9qN3RNK20rclB5ZVVLZjh2bWxOa2NoNnJrU2tvbnU4aVc1S0lLaGZwQ2ZINDdxb0RuS1Z1NUloeEVlcG96UFRRQUN5dlJGRHhNTVhkUlBLWU1jVVZNSldCZ2lLUGxheUpDVDVuK0duZzJUMkFIbTdscUhHbW1vd0l1Rm54OXRDUWNIYVBrUTh1b0V6V1JYWmlLMmREbDA5Z0JwRmwvbWFFQzNiK1BhUWtiMis5YnNnRjRWL3VJRWNjblR6ajhrUXpKRHhvRzlPK2lzdGhqYk50ZmVZbjUyQXRrMGNWdkRkTGJFY2FjcVdOa1ZTN3RMcmhERlJBNitmWG53b2ZkRnZHOER1cnVaMHdYeVlCUC9uMG1VQnVCS1FQYkRmbnprZVpTWWpsRXVxcVVseklqVmttQ3R5NVdDdHZKUWlqOUhyZldrOCtpZHRESFVVRzBMbG5XMW9qb3hlS1d0b2hMdnJPY05TdDZCWjI4V01PSnpYOERYT0EzOU1KalI0c0pqWUl2cU1vWDJnVzRHTG83MWl4VFd3dW5Sbnd2b0tlT0pUM0didzJGTk14Q3lSYitoSGNuR3IzQi9sditDL0VJSFIzRGZKVnRsekJULzZ6ZlE2WkdnTzZlNUkxUTdsL2NKN2pBa3dHblpzVDNlNWdWeWdVdGRQK1ZvQUhRTEQvQlhRNTBhQk94T3R6dmtoMm1CUGR4OCtaQ0ZLYWwvaHo0SWsyNFRYaU8yY3FhUDg1dS8zN2llRllUOVg3eHhOdzFJcGJtNWFwTFFOcEVyWjIwVlJCdDU3SjlWcFJGeEtPaTFqdWQ1NG1kN3NZd2xnR1ZFWEl3cXR5VnlGUHlGVGErWVRHVi80bG5uWjN1b2IwN3g0S29WbDkrVXkyeHBWZCszS3Rlb29Ub1MxK0d0aG5kVTZaVldkTzkrSkVxR2FoNU50b3hWQkg1R2VsRGRNQVZCN2ZzY09QOEdCaDF6L0V6WE9GYWoycVlsSndsZ1JlMmIvbEhvQjJJY0V1c0FUUHg3b2pzZ2x3Vmo5L1Y0TUp5QWRSaXphaE41RXBYbkJJYnVtTnAxS3VQVUZuWUlkdGhvODBWaE5RblF1ZWUyOTgyRFRNYVRYOXIvT2o8L0Nvb2tpZT48L1NlY3VyaXR5Q29udGV4dFRva2VuPg==; .BLSIAUTH_711=10D8A2B90C0FB9F43DD3A1B21FF1E22A62F0F11F2CD0ACA90E314981156C5686D820EDC9A4C35429C7D9ACBCA4C40B9A7D4675ABAE8656CE61A24A15F7405DFF42DF90F77D5115F0B08FAAB50F8650D62A4D665F80E22C5A0C886F91496261D29A6C8499B3B2CBF31A22E498DC0E0851C2D23FAFCFDAE430961697D31B4EEE6C4D62CB7C8D027381E60050B7F5F6783420A1D3F137A895E4116D767FE66D555314541440E3EF42636BE98BCE4E174E1ADDF03C34EF0A513E12113F85A4A4C1A7DF04E826B758416F2B3F197F6E3B34B22BB391E50DD096213084A27A5237304EDB1B2C57406891662E039E35912E671FEDFD5F4F933885E0A1CA9456D89B7C92CD54491101339323DBBB6204F4BB85D191927E7828F46E3DB03E20CE84CD696E09429FDD7F6300E9FEF5EC2BD171365D5459D2FD959AA36CDD186C730E74789BD0341DFAC16846EC91E8FB4299077808009E5E4171CBA4E4C3DE8EF4EF1A9519F908641F; .MIXEDAUTH=778CC67C8B09BB76E64D0966970A287CE67F0CB10C343F3D7F939C1D8F0EF1BD12E7EA6745CA8561EFFB6CD9F89A84F1DE03F2E50F78E8AAA165A59ECEF834311BBF720FCF81D7B076F8238DB92805FEA6327CABFF9105D0D653FDA486D822B1BB89E999978F01C4EC3D3E158078B9155B758A9953637DE841B897E2975E5E357AF8CEEEF8917581F574F1ACA109A4E2AE05A3F8DF74277F3BCF03A0947B978AE611BB1385B1C17EE185D523AE5D47F49BF80E576377AAE28FDF64953720C6D6C76E784FCAA844361EE2164C1C3B338B31D3FCAF6761D3528E82EB6617E923E1B152A39D1F454A7D9804F488A42A1D7B2CDA91CBBE1D34155C847E085BB8D5A73CE3DDCB073E095FEA1D92F5A1CCA7D038AEB6CD910BB3C21ED0989D235F035F4576D416A8DAB42ACF08E804B4B7BF018B396BF2491ACDEF6C3B6879C789BB7990C3AC8D',
            'Origin': f'{base_url}',
            'Referer': f'{base_url}/Modules/Reconciliations/ExecGrid.aspx',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'X-NewRelic-ID': 'VwAAU1VTCBAFUlRVDgABVVQ=',
            'X-Request-Verification-Token': 'EUVdRv_-wyM213S3iPFjdxl0pTqQJRRzFNN_nPmECnaFMWJHBNL2jEUotDtDyKtVmVwtYLPFtGIJLkykcIU_1u8vbs01',
            'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjM2NzIzMDAiLCJhcCI6IjYwMTQzMzY1OSIsImlkIjoiYTVjZjFmZTNjMjg1NjAyZSIsInRyIjoiZmNjZTRmZmZjY2UxNDE4YTQ5NWJkYWMyYTE4ZTAwM2MiLCJ0aSI6MTcwNjEyODU3NTExNCwidGsiOiIzODkzNzkzIn19',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'traceparent': '00-fcce4fffcce1418a495bdac2a18e003c-a5cf1fe3c285602e-01',
            'tracestate': '3893793@nr=0-1-3672300-601433659-a5cf1fe3c285602e----1706128575114',
        }       

        tracking_file_path = f"{base_path}/{config_data['source']['instance']}/tracking"
        excel_file_path =f'{periodPath}/reconciliations.xlsx'
        sourceDf=util.readExcelFile(excel_file_path)
            # with open('restart.txt', 'r') as file:
        #     start_index = int(file.readline().strip())
        # for index, row in adjustedDf.iterrows():
        trackingDf = pd.read_csv(f"{tracking_file_path}/{period}.csv")
        trackingDf['reconciliationId'] = trackingDf['reconciliationId'].astype(str)

        # Perform an inner join to identify records with the same reconciliationId
        merged_df = sourceDf.merge(trackingDf, on='reconciliationId', how='inner')

        # Select records from sourceDF that are not in merged_df
        a_filtered = sourceDf[~sourceDf.index.isin(merged_df.index)]    # print(a_filtered)
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit tasks to the executor
            futures = [executor.submit(self.process_row, row2, headers, cookies,  session) for index, row2 in a_filtered.iterrows()]

            # Wait for all tasks to complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    # Retrieve result (if any) and update the data_df or handle exceptions
                    result = future.result()
                    writeProgress(result[0],result[1])
                    trackingDf = pd.read_csv(f"{tracking_file_path}/{period}.csv")
                
                    completed = (trackingDf.shape[0]/sourceDf.shape[0])*100
                    sp.text=f"Processing Period: {month_name}, {year} {round(completed, 2)}% Completed"
                    # logging.info(f"Completed {round(completed, 2)}% of current Period {periodId}" )
                except Exception as exc:
                    logging.error(f'Generated an exception: {exc}')


        pass

    def getTotalRecords(self):
        json_data = {
            # 'filters': config_data['source']['sheetView']['filters'],
            'options': {
                'grouping': 'GLAccountsOnly',
            },
        }
        headers = {
            'Accept': 'application/json; version=2',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            # 'Cookie': '__AntiXsrfToken=f083162b548949718c1bd194fe4609a9; BLSIAPPEN=!Uc7ijuI4fl4DiSJmiOBa4m4YLrxynGQBMy/yGJoBqSbR9232ER7BFktdzVQNo80nwuyUS4pBXIBo1w==; __RequestVerificationToken=LnOYZ5iGRTct5n39TiFrCJQF27k7dHFXDHqdKOCwT_hYjdFm6symBt0UrnIqN3bn0e9uDa6aOUz713RCjGrmSGIHYwo1; ASP.NET_SessionId=sse0q3qxa2mr5vapdk2eymoy; FedAuth=77u/PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0idXRmLTgiPz48U2VjdXJpdHlDb250ZXh0VG9rZW4gcDE6SWQ9Il8yNzY2ZWU3ZC0zMTM3LTQ5OTUtODg2YS0xYTkwYWJjMWExMGYtMTExRkFFQjhDNjg3MThCOTkyM0Q2Q0JENjAwRDQ5RDIiIHhtbG5zOnAxPSJodHRwOi8vZG9jcy5vYXNpcy1vcGVuLm9yZy93c3MvMjAwNC8wMS9vYXNpcy0yMDA0MDEtd3NzLXdzc2VjdXJpdHktdXRpbGl0eS0xLjAueHNkIiB4bWxucz0iaHR0cDovL2RvY3Mub2FzaXMtb3Blbi5vcmcvd3Mtc3gvd3Mtc2VjdXJlY29udmVyc2F0aW9uLzIwMDUxMiI+PElkZW50aWZpZXI+dXJuOnV1aWQ6ZDljZWMzODQtYzg2My00MGJiLWE4MWItOTIyNzJkNDE3ZTg2PC9JZGVudGlmaWVyPjxDb29raWUgeG1sbnM9Imh0dHA6Ly9zY2hlbWFzLm1pY3Jvc29mdC5jb20vd3MvMjAwNi8wNS9zZWN1cml0eSI+QUFJQUFJb3hxR2Z3anVKcWl6M1NxSmdiczJ3dnNYUG5TSEV5U0NjbFI4LzNXYVJ2SEFuTmdMTHpGanJaWlUyTDdBVHlzZFU0OG5zeC9ibnMzK2RFc1hkMnFhWGVHRmxkMTZ0VG1RQnRULzN6QXVMWFMyNVVTeDNwcmN3bWEzSUVHOG9aaWE1UG0yV3lnRURhbXpSWEIwdHRERWJDeEpXOFdXTWlRbWp1OG00bXZpN3RTTTJuQXNZejdsVDBxdHZHcHhTWUtaMmJVb1VTdjFzTXRCeTBrRjczTHl0TjVEOHBVWTJMY0szSmM2b2hwcWFQT3pwbHZYVm9EU3lHUHFMYmxQNWxQVGR3Mm9iTjBicXdqZk4rRllreDdJTzdHbVUzNUpZNVJxRXMrODI2c2NoWHBVbjJCeWE2ZFIyaWFndksyYXR3eDJZOUt1L2k5bmpGOGFHSThFQzBBZGpDY09FMVhXMTdDR0pVZFIzeVpCcnJzUDVKYWp3dysrWXhWcnZHU1FjSFVreEdGNnVGQnhwdXpuZmdFSE52bHB0UFlsNmpNbHVNYUJDOFhRSEJsdzNLYVpsWnZiUitXd3FqQk5mZWFyMlFIbmJDT1pmRDJGdElmSCtkY0grRmtFeExudmhUYzd1Y0U3UHFJOEM3YzdiekhjVmkxTlB1L0lER0I0YzNDUkRKcmFkeVcxZG1UMFF4c2haZW1NMS9PRTRmRjlTREZra3VXTHBFMzlwVGxscHVMUUdNQ0NranVGRGhGTjFYVWViOXRPTG04TFJFTEpZYVdCWld3MVZDY1djMmFMajg2Ym13TUVWZDFjbk5vcnQ1NjdUVVMzazRXdVFCem5WVE1SWjBCUkNUSUhSbzBjalBoZms0SnNoRFVTYmhPUllpR0FpMTVtWnVCQkdFSk51Mkx0clpkMjNwNldXb1c5b1lMZDhjRjBGbW1KTUF6QXMvSk0rRjU5eU1ZcjRBQWdBQWV3RXoxUFA4bUZUc1JnTVdGVWhGbVp2dFVTSGV5aS9lKzFQbkRUN1lkNnVLRE1BQTNWaVpORGQyZHdrWGVqL3lJNFFpRExjWjVtUU5CeUVjYldkemdBclg5QXhGN1F2TEY0VVZ0bU11SERGN3Q2WENrcTlleThEMXBzMzc4c1ppY04yRGJ3MUhudm9IQ01NcFJ4OTBqTGlaM1lRZnJFRHcrSWpHamFrK3lHWFhYSjNYODdlazRWR2h1M2Y4UWpnamdRVVI4M2Y4TnFVaDBQNVhBUlRTc01VQlNlZU96b3NSSnJZZUpjYWx0a3lXMVd4dEl1cmFlTEdITW5sb0F4RkF6Z3EwK2JETnJBWnAzT2gzOGdsMU1qd3dVTVkyWXBySU9NZVJucTRvWWU0NVNnMFkrNitGSFdyc2Jva3R5VktsMWZO; FedAuth1=dkY4UFBzTVVyb25oMmtFUnRGMUYvYTU5ZTFVWVlMNThBWVFacXIzNUJkTVhBYk9EWjZqTzVnVDFseWlUbmhZUWFOVmFxRERBVjRXcTUwRTJFQVVSMDMyd3prbUMyUHNWcjlIV2F0MEw0akxQQmRUKytwdWRGc0JLcGc5eG5CU0ZGSXRxU0RpVU9zN2E0NDVrTmNZTE12WFJCblNNR1BwZ0MvODZUOTZRSVBkcUEwYkk0UCtVaGh6bGhQN1U5UTUzQitmdTlzVzcrRkRlUlE3R04rbEt3VjR6Z21FK3NFUWE0Z2RrbmplM01qV01aemlaeHpaZExueVQySks3bVEwOUdqYUdHV2lQcmlCcG42TTNsdjl4aWRMblNDK01DMlJscmZ2OEVXZmN3eVdRK253WE1lZVhqV0VNRi92Mzd4R1lkZjBIRm9RODQwV1cwVTNkajFxbUpMUlI2MWtZcDJERjVXQ3lBRmtqUUFnQUFhamlVcXVFTHFocjVrSXNFemtUWGc3VzNUMlNXNlhkZXJMdGdoVE5lbys0SWtRRGFZSmplSmFONFpKQWQxRkNoK0Zydm9XajVuSmN6VVRKS3hHYThGODkrdGprMmxiRE11WFgxL2M3d01SZjRHcHpabDBlVG5nUU9qN3RNK20rclB5ZVVLZjh2bWxOa2NoNnJrU2tvbnU4aVc1S0lLaGZwQ2ZINDdxb0RuS1Z1NUloeEVlcG96UFRRQUN5dlJGRHhNTVhkUlBLWU1jVVZNSldCZ2lLUGxheUpDVDVuK0duZzJUMkFIbTdscUhHbW1vd0l1Rm54OXRDUWNIYVBrUTh1b0V6V1JYWmlLMmREbDA5Z0JwRmwvbWFFQzNiK1BhUWtiMis5YnNnRjRWL3VJRWNjblR6ajhrUXpKRHhvRzlPK2lzdGhqYk50ZmVZbjUyQXRrMGNWdkRkTGJFY2FjcVdOa1ZTN3RMcmhERlJBNitmWG53b2ZkRnZHOER1cnVaMHdYeVlCUC9uMG1VQnVCS1FQYkRmbnprZVpTWWpsRXVxcVVseklqVmttQ3R5NVdDdHZKUWlqOUhyZldrOCtpZHRESFVVRzBMbG5XMW9qb3hlS1d0b2hMdnJPY05TdDZCWjI4V01PSnpYOERYT0EzOU1KalI0c0pqWUl2cU1vWDJnVzRHTG83MWl4VFd3dW5Sbnd2b0tlT0pUM0didzJGTk14Q3lSYitoSGNuR3IzQi9sditDL0VJSFIzRGZKVnRsekJULzZ6ZlE2WkdnTzZlNUkxUTdsL2NKN2pBa3dHblpzVDNlNWdWeWdVdGRQK1ZvQUhRTEQvQlhRNTBhQk94T3R6dmtoMm1CUGR4OCtaQ0ZLYWwvaHo0SWsyNFRYaU8yY3FhUDg1dS8zN2llRllUOVg3eHhOdzFJcGJtNWFwTFFOcEVyWjIwVlJCdDU3SjlWcFJGeEtPaTFqdWQ1NG1kN3NZd2xnR1ZFWEl3cXR5VnlGUHlGVGErWVRHVi80bG5uWjN1b2IwN3g0S29WbDkrVXkyeHBWZCszS3Rlb29Ub1MxK0d0aG5kVTZaVldkTzkrSkVxR2FoNU50b3hWQkg1R2VsRGRNQVZCN2ZzY09QOEdCaDF6L0V6WE9GYWoycVlsSndsZ1JlMmIvbEhvQjJJY0V1c0FUUHg3b2pzZ2x3Vmo5L1Y0TUp5QWRSaXphaE41RXBYbkJJYnVtTnAxS3VQVUZuWUlkdGhvODBWaE5RblF1ZWUyOTgyRFRNYVRYOXIvT2o8L0Nvb2tpZT48L1NlY3VyaXR5Q29udGV4dFRva2VuPg==; .BLSIAUTH_711=10D8A2B90C0FB9F43DD3A1B21FF1E22A62F0F11F2CD0ACA90E314981156C5686D820EDC9A4C35429C7D9ACBCA4C40B9A7D4675ABAE8656CE61A24A15F7405DFF42DF90F77D5115F0B08FAAB50F8650D62A4D665F80E22C5A0C886F91496261D29A6C8499B3B2CBF31A22E498DC0E0851C2D23FAFCFDAE430961697D31B4EEE6C4D62CB7C8D027381E60050B7F5F6783420A1D3F137A895E4116D767FE66D555314541440E3EF42636BE98BCE4E174E1ADDF03C34EF0A513E12113F85A4A4C1A7DF04E826B758416F2B3F197F6E3B34B22BB391E50DD096213084A27A5237304EDB1B2C57406891662E039E35912E671FEDFD5F4F933885E0A1CA9456D89B7C92CD54491101339323DBBB6204F4BB85D191927E7828F46E3DB03E20CE84CD696E09429FDD7F6300E9FEF5EC2BD171365D5459D2FD959AA36CDD186C730E74789BD0341DFAC16846EC91E8FB4299077808009E5E4171CBA4E4C3DE8EF4EF1A9519F908641F; .MIXEDAUTH=778CC67C8B09BB76E64D0966970A287CE67F0CB10C343F3D7F939C1D8F0EF1BD12E7EA6745CA8561EFFB6CD9F89A84F1DE03F2E50F78E8AAA165A59ECEF834311BBF720FCF81D7B076F8238DB92805FEA6327CABFF9105D0D653FDA486D822B1BB89E999978F01C4EC3D3E158078B9155B758A9953637DE841B897E2975E5E357AF8CEEEF8917581F574F1ACA109A4E2AE05A3F8DF74277F3BCF03A0947B978AE611BB1385B1C17EE185D523AE5D47F49BF80E576377AAE28FDF64953720C6D6C76E784FCAA844361EE2164C1C3B338B31D3FCAF6761D3528E82EB6617E923E1B152A39D1F454A7D9804F488A42A1D7B2CDA91CBBE1D34155C847E085BB8D5A73CE3DDCB073E095FEA1D92F5A1CCA7D038AEB6CD910BB3C21ED0989D235F035F4576D416A8DAB42ACF08E804B4B7BF018B396BF2491ACDEF6C3B6879C789BB7990C3AC8D',
            'Origin': f'{self.base_url}',
            'Referer': f'{self.base_url}/Modules/Reconciliations/ExecGrid.aspx',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'X-NewRelic-ID': 'VwAAU1VTCBAFUlRVDgABVVQ=',
            'X-Request-Verification-Token': 'EUVdRv_-wyM213S3iPFjdxl0pTqQJRRzFNN_nPmECnaFMWJHBNL2jEUotDtDyKtVmVwtYLPFtGIJLkykcIU_1u8vbs01',
            'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjM2NzIzMDAiLCJhcCI6IjYwMTQzMzY1OSIsImlkIjoiYTVjZjFmZTNjMjg1NjAyZSIsInRyIjoiZmNjZTRmZmZjY2UxNDE4YTQ5NWJkYWMyYTE4ZTAwM2MiLCJ0aSI6MTcwNjEyODU3NTExNCwidGsiOiIzODkzNzkzIn19',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'traceparent': '00-fcce4fffcce1418a495bdac2a18e003c-a5cf1fe3c285602e-01',
            'tracestate': '3893793@nr=0-1-3672300-601433659-a5cf1fe3c285602e----1706128575114',
            }       
        logging.info('Getting Total')
        response = self.session.post(
            f'{self.base_url}/api/accountreconciliation/totals',
            cookies=self.cookies,
            headers=headers,
            json=json_data,
        )
        data = json.loads(response.content)
        print(data)
        total_records = data['totalRecords']
        return total_records
    # TODO: adjust for self

    def getSpreadSheets(self,session,row,sp):
        year = row.iloc[0]['Year']
        month_number = row.iloc[0]['MonthNumber']
        month_name = month_to_text(int(month_number))
        total_records=getTotalRecords(session)
        logging.info(f"Period: {month_name}, {year} has {total_records} records")
        sp.text=f"Processing Period: {month_name}, {year}"
        periodPath=f"{base_path}/{config_data['source']['instance']}/Reconciliation_Sheets/{period}/"
        # api_call_and_print_dataframe(total_records,periodPath)
            # Create the "test" directory if it doesn't exist
        if not os.path.exists(periodPath):
            os.makedirs(periodPath)
        # if (start_index==0):
        # pageSize=config_data['source']['pageSize']
        # pages=math.ceil(total_records/pageSize)+1
        pageSize=total_records
        pages=1
        rdfs=[]
        # for x in range(1, pages):
        headers = {
            'Accept': 'application/json; version=2',
            'X-Request-Verification-Token': 'S1Blyz-LsHiTtkXz0Sl60EoC-3VNxxRqC5Ag2Go3teNOUPfIyckQJSq-ZYhFez4k-2HL9DFmwo2KKnKNcvOuDnTfPSI1',
            'Referer': '',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Content-Type': 'application/json',
        }        
        json_data = {
            'columns': config_data['source']['sheetView']['columns'],
            'pageSize': pageSize,
            'pageNumber': 1,
            'sort': [
                {
                    'field': 'reconciliationId',
                    'dir': 'asc',
                },
            ],
            'pageNumberEnd': 1,
            # 'filters': config_data['source']['sheetView']['filters'],

            'options': {
                'grouping': 'GLAccountsOnly',
                'columnSet': 'GridActionStatuses',
                'includeTotalAvailableRecords': False,
                'includeTotalRecords': False,
            },
        }
        logging.info('getting spreadsheet')
        response = session.post(f'{base_url}/api/accountreconciliation', headers=headers, json=json_data,cookies=cookies)
        # print(response.status_code)

        if response.status_code == 200:
            # Flatten nested dictionaries using json_normalize
            # flattened_data = pd.json_normalize(response.json())

            # Extract the "rows" data from the response JSON
            rows_data = response.json().get("items", {}).get("rows", [])
            logging.info("processing spreadsheet information")
            # Create a DataFrame from the "rows" data
            df = pd.DataFrame(rows_data,dtype=str)
            df.to_excel(f"{periodPath}/reconciliations.xlsx", index=False,header=True)
            # rdfs.append(df)
        
        # rdf= pd.concat(rdfs)
        # rdf.to_excel(f"{periodPath}/reconciliations.xlsx", index=False,header=True)
        
       
if __name__ == "__main__":
    utility = DataProcessingUtility(max_workers=10, config_path='config/hyatt.json')
    with open('period.json', 'r') as file:
        data = json.load(file)
    df = utility.extract_data_to_dataframe(data)
    print(df)
    for index, row in df.iterrows():
        periodId=row['id']
        # get reconcilations.xlsx for period as rec_df
        # get Tracking csv for period as trac_df
        # run both DF throu DeDupper
        #  merge DFS based on ReconcilationId field
        
        
    