# Traffic Accidents Project

This project aims to analyze traffic accident data to identify patterns and insights that can help improve road safety. The dataset is sourced from Kaggle and processed using AWS Glue for data cataloging and analysis.

## Project Structure

- `dataset_generator.py`: Script to download the dataset from Kaggle and upload it to an S3 bucket.
- `glue_crawlers.py`: Script to create and manage AWS Glue crawlers for data cataloging.
- `configs/`: Directory containing configuration files.

## Setup

### Prerequisites

- Python 3.6+
- AWS CLI configured with appropriate permissions
- Kaggle API credentials

### Installation

Create a virtual environment and activate it
 - pipenv shell 
 - pipenv install boto3 requests unzip 
  
Set up Kaggle API credentials
 - Place your kaggle.json file in the ~/.kaggle/ directory.
  
### Configuration

Update the configuration files in the configs directory as needed
- `data_config.py`: Contains dataset identifier, S3 bucket name, S3 directory, crawler name, database name, and role ARN.

### License
This project is licensed under the MIT License. See the LICENSE file for details.