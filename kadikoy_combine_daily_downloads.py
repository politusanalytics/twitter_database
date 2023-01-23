import sys
import os
import shutil
from datetime import datetime


# Get daily_downloads and combined paths
DAILY_DOWNLOADS_PATH = sys.argv[1]
COMBINED_PATH = sys.argv[2]

# Get today's date in yymmdd format
today = datetime.now().strftime("%y%m%d")

# Read all daily_downloads
daily_downloads = [f"{DAILY_DOWNLOADS_PATH}/{filename}" for filename in os.listdir(f"{DAILY_DOWNLOADS_PATH}") if filename.endswith("gz") and today not in filename]

# Combine files
with open(f"{COMBINED_PATH}/kadikoy_users-{today}_combined.txt.gz", 'wb') as wfp:
    for fn in daily_downloads:
        with open(fn, 'rb') as rfp:
            shutil.copyfileobj(rfp, wfp)

# Remove older combined files
remove_files = [f"{COMBINED_PATH}/{filename}" for filename in os.listdir(f"{COMBINED_PATH}") if filename != f"kadikoy_users-{today}_combined.txt.gz"]
for file in remove_files:
    os.remove(file)