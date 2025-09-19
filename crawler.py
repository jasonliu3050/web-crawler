# crawler.py
import os
import csv
import time
import datetime as dt
from typing import List, Tuple, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ========= 基本參數 =========
BASE_URL = "https://ecs_
