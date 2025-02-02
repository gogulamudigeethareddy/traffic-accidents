import os
import unzip
from kaggle.api.kaggle_api_extended import KaggleApi

# Initialize Kaggle API
api = KaggleApi()
api.authenticate()

# Construct the path dynamically
base_path = os.path.dirname(os.path.abspath(__file__))
new_path = os.path.join(base_path, 'data')

# Download dataset to the specified path
api.dataset_download_files('oktayrdeki/traffic-accidents', path=new_path, unzip=True)

print(f"Dataset downloaded to: {new_path}")
