import os
import re
import dropbox
from datetime import datetime
from dotenv import load_dotenv
import logging

# create logger
logger = logging.getLogger('dbx_upload')
logger.setLevel(logging.DEBUG)

# create file handler which logs even debug messages
fh = logging.FileHandler('uploads_status.log')
fh.setLevel(logging.DEBUG)

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

# Load Dropbox token securely from .env
load_dotenv()
DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")
print(f"Token: {DROPBOX_TOKEN[:10]}...")  # Preview the start
CHUNK_SIZE = 10 * 1024 * 1024 # 10MB

# Setup Dropbox Client
dbx = dropbox.Dropbox(DROPBOX_TOKEN)

# Constants
LOCAL_DIR = "C:/Users/Simon/My Drive/Documents/Development/dropbox_backups"
DROPBOX_DIR = "/Backups"
DATE_PATTERN = r"\d{2}-[A-Za-z]{3}-\d{2}"
DATE_FORMAT = "%d-%b-%y"

# Find latest .zip file with date
# zip_files = [f for f in os.listdir(LOCAL_DIR) if f.endswith(".zip") and re.search(DATE_PATTERN, f)]
zip_files = []

for f in os.listdir(LOCAL_DIR):
    if f.endswith(".zip"):
        match = re.search(DATE_PATTERN, f)
        if match:
            date_str = match.group() # e.g. '05-May-25'

            try:
                parsed_date = datetime.strptime(date_str, DATE_FORMAT)
                zip_files.append((parsed_date, f)) # tuple of (date, filename)
            except ValueError:
                print(f"Invalid date in filename: {{f}}")

# Sort by date descending
zip_files.sort(reverse=True)

# Get the latest file info
if zip_files:
    latest_date, latest_file = zip_files[0]
    formatted_date = latest_date.strftime("%Y-%m-%d")
    print((f"Latest backup: {latest_file} (Date: {formatted_date}) "))
    logger.info(f"Latest backup: {latest_file} (Date: {formatted_date}) ")
else:
    logger.info("No matching ZIP files found.")
    print("No matching ZIP files found.")
    exit()

filename = zip_files[0][1]
local_path = os.path.join(LOCAL_DIR, filename)
dropbox_path = f"{DROPBOX_DIR}/{filename}"
file_size = os.path.getsize(local_path)

print(filename)

logger.info(f"Uploading: {filename} ({file_size / (1024*1024):.2f} MB)")
print(f"Uploading: {filename} ({file_size / (1024*1024):.2f} MB)")

with open(local_path, "rb") as f:
    if file_size <= CHUNK_SIZE:
        dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
        logger.info("folder less than chunk size, directly uploaded")
    else:
        upload_session_start_result = dbx.files_upload_session_start(f.read(CHUNK_SIZE))
        logger.info("Upload session started")
        cursor = dropbox.files.UploadSessionCursor(
            session_id = upload_session_start_result.session_id,
            offset = f.tell()
        )
        commit = dropbox.files.CommitInfo(path=dropbox_path)
        logger.info("Session commited")

        while f.tell() < file_size:
            if (file_size - f.tell()) <= CHUNK_SIZE:
                logger.info("Finishing last chunk...")
                print("Finishing last chunk...")
                dbx.files_upload_session_finish(f.read(CHUNK_SIZE), cursor, commit)
            else:
                dbx.files_upload_session_append_v2(f.read(CHUNK_SIZE), cursor)
                cursor.offset = f.tell()
                logger.info(f"Uploaded {cursor.offset / file_size * 100:.2f}%...")
                print(f"Uploaded {cursor.offset / file_size * 100:.2f}%...")
logger.info("Upload completed.")
print("Upload completed.")


# # Paste your full token here directly (bypass .env for now)
# DROPBOX_TOKEN = "sl.u.AFwwy5qYn7r1gwYv6cmAimjmN5nSZfAT1GmG82GSIw9ZalYctpuza_ETaMzhA1rCwQIyqaTokFAwqMHYQIKtZGGz3g_cSAPK4aPb45lDo62mrT0lpbKfKgUdWlGxtSTQlSm9HwHfJ6_mbd6a_BueTE47-BMDdeMTUGwNsINuSBfBaZ_g78cFg5i0K10in_yc5PoJ1eWok0swBv6AZa4VHF2-Zyk1t5YGoPXhWc2let0wGq-ElqKNWTZ23bIBZ1_cqeHFPMCqOhKfO0rKUFl2IYL2Uapb-WRSYqLFMea-C3wV1_0dOo7NmF9LqoMYgUHPVw2ceVVb78u8wgU3Cie_nRIbYdPPPg6Eifgif2fXTgWE4obpb8ZHzA9Wv-MEBoQ_9_YezhUCbPf-GuX0KKUL66Pmfu17Wofh3xM9OPCdUUKJUYEefVBJ6dhBedglBZAa38ewJ6c9RzrjlVOcYj0PbUz90ute9pYR5h6BvU3JosKPge8-Y5wJd1DqWY0O4aCXXpldCIB--Hcr_SAQ_4jliTnPKJOQdiskosQ00LBRTf6ZnuXYqhguD5y32JOrF2w7OrqSmLCfRe5hDWk-4HtVZys6YguUnEuEsil5wdgMu4lzzxUfqn6YqjyYxK1ddhIKzyjoUhTCh1Q27npMeKL-6jEc2hYBbSoxnrZ1Re2kCBsmGSB0_z99V2oMbw3GqtPTrDtn1apwKuP8IcNn8p3_7VSwKkc38Mk5KWn9Cgs1A4035j59pLvFckG78x4uDdZWRQRQ4mJQIYhhF8fjIbciZe3_P3jw9KlYN4H64byEUzMPu-wH5lgbvJnQBE-MBAPGQn3wclDmzQNi8_8UEDZ8dyWhdNWzV7WCfctiVUSfX4-pwktxGilReldPx24TC12Loq_bPLHS0JLSGRF3tcxuJwDqErDj4Ps6o41iHfb_nx5DSJkD-hEEZ1sgnIXz7lwKNvyzuASjm1WUAgBLm1-_1wNbaY_MRC9ZWp9lwPp2KPzqmaqlJAY4I1Wf3LXDBswcG8I4Vb0nJByR-nOyLNJJPHhnENqvMTtCv6bPPlDs3GPx74WdTJg61Mnb4qDY1oFV6RJdgzBaLXtHrfxe8vLDUUVQytvVRmU0fLhXe0HkLF1WFUR1DDDPfjlHyjbWESvxi_HjoLQKrj_aXWAFuSyUikVl5IqdfopeWkTgRk-QNHOepvAen80rhC1D4ykeMekeCGEkHL9cfrEvjqRoQIe6wfMbzt7v5LYsVyCrscWEs2Nd3oeL8Ha-pGF7uwZf_WKe1qA43cEm6QhgVEz3kAQNCzTc-8EaNLlkusS3uM2phI-Kahgz4uee03DiFtPhAnwas6vGeaNfbY1PuscTHiQNQV2oqhsqrIyH7FpPbZ6e8amj7qPEBLTUwWrgF44E4yViDgF8BYknIjbhUPStsNFPHFvOihBLhOOrgOzKLVfYYs_39A"
# DROPBOX_DIR = "/Backups"  # <- Must match your app name if using App Folder access
# LOCAL_FILE = "C:/Users/Simon/Desktop/uploads_status.log"
# REMOTE_FILE = f"{DROPBOX_DIR}/uploads_status.log"

# with open(LOCAL_FILE, "rb") as f:
#     dbx = dropbox.Dropbox(DROPBOX_TOKEN)
#     dbx.files_upload(f.read(), REMOTE_FILE, mode=dropbox.files.WriteMode.overwrite)
#     print("Upload succeeded.")