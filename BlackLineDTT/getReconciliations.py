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
import threading




with yaspin(text="Executing Script", color="cyan") as sp:
    if len(sys.argv) < 2:
        print("Usage: python auditfiles.py <config_name> <periodId>")
        sys.exit(1)

    config_path = f"config/{sys.argv[1]}.json"  # First argument after the script name
    periodStart=155
    periodEnd=145
    with open(config_path, 'r') as file:
        config_data = json.load(file)
    periodfile=f"config/{config_data['source']['periodfile']}"
    utility = DataProcessingUtility(config_path=config_path,periodPath=periodfile,periodId=periodStart)
    for period in range(periodStart, periodEnd, -1):
        utility.change_period(period)
        totalrecords = utility.getTotalRecords()
        print(totalrecords)