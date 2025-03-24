import pandas as pd
import shutil

staging_path = "/content/test_db/staging_sch/original.csv"
cdc_path = "/content/test_db/cdc_sch/original.csv"

if not os.path.exists(cdc_path):
    shutil.copy(staging_path, cdc_path)
    print("✅ CDC file created from Staging!")
else:
    print("🚀 CDC file already exists!")
