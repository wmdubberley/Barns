# Haytt File Downlad System
---
## Task
This Code is designed to connect with the Haytt Blackline System and treat the system like an api. and then interrogate the system and download the Reconciliation files organizing them under their Reconciliation ids. 

---
## Setup
* run pip install -r requirements.txt
* navigate to https://hyatthotels.us2.blackline.com/Modules/Reconciliations/ExecGrid.aspx 
  * inspect page and under Network find totals right-click and copy to curl
* Navigate to https://curlconverter.com/python/
  *  paste in the curl command box
  *  in the box below copy the Cookies portion of the generated code and Replace it in the script file hyattFileDownload.py line 9
* on line 191 loop_count=10
  *  set this to any value greater then 0 for poc purposes, for all files set this value to 0
  
---

## Execution
 ```
 python3 blacklineDTT.py
 ```
or you can set file perision to +x
```
chmod +x blacklineDTT.py
```
then you can run it direct like
```
./blacklineDTT.py 
```

 ---
 ## TODO:
 * Possible issue with long runs of it erroring out dude to expiration of the cookie
 * Need to build  Restartability so we don't Re Run the sam files over and over again
 * 