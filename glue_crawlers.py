import boto3
import time
from configs.dataconfig import (
    DATASET_IDENTIFIER,
    bucket_name,
    s3_directory,
    crawler_name,
    database_name,
    role_arn,
)

def create_glue_crawler(crawler_name, database_name, s3_target_path):
    glue_client = boto3.client("glue")

    try:
        response = glue_client.create_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=database_name,
            Targets={"S3Targets": [{"Path": s3_target_path}]},
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG",
            },
            TablePrefix="traffic_",  # This will prefix your table names
            Configuration='{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
        )
        print(f"Crawler {crawler_name} created successfully")
        return response
    except glue_client.exceptions.AlreadyExistsException:
        print(f"Crawler {crawler_name} already exists")
        try:
            # Update the existing crawler
            response = glue_client.update_crawler(
                Name=crawler_name,
                Role=role_arn,
                DatabaseName=database_name,
                Targets={"S3Targets": [{"Path": s3_target_path}]},
                SchemaChangePolicy={
                    "UpdateBehavior": "UPDATE_IN_DATABASE",
                    "DeleteBehavior": "LOG",
                },
                TablePrefix="traffic_",
                Configuration='{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
            )
            print(f"Crawler {crawler_name} updated successfully")
            return response
        except Exception as e:
            print(f"Error updating crawler: {str(e)}")
            raise


def start_glue_crawler(crawler_name):
    glue_client = boto3.client("glue")

    response = glue_client.start_crawler(Name=crawler_name)
    print(f"Glue Crawler {crawler_name} started successfully.")
    return response


def check_crawler_status(crawler_name):
    glue_client = boto3.client("glue")

    while True:
        response = glue_client.get_crawler(Name=crawler_name)
        status = response["Crawler"]["State"]
        print(f"Crawler status: {status}")
        if status == "READY":
            print(f"Glue Crawler {crawler_name} completed successfully.")
            break
        elif status == "FAILED":
            print(f"Glue Crawler {crawler_name} failed.")
            break
        time.sleep(30)

def check_crawler_results(crawler_name):
    glue_client = boto3.client("glue")
    response = glue_client.get_crawler(Name=crawler_name)

    if "LastCrawl" in response["Crawler"]:
        last_crawl = response["Crawler"]["LastCrawl"]
        print(f"Tables Created: {last_crawl.get('TablesCreated', 0)}")
        print(f"Tables Updated: {last_crawl.get('TablesUpdated', 0)}")
        print(f"Tables Deleted: {last_crawl.get('TablesDeleted', 0)}")


# Parameters for the Glue Crawler
s3_target_path = f"s3://{bucket_name}/{s3_directory}"

# Create the Glue Crawler
create_glue_crawler(crawler_name, database_name, s3_target_path)

# Start the Glue Crawler
start_glue_crawler(crawler_name)

# Check the status of the Glue Crawler
check_crawler_status(crawler_name)

# After running the crawler, check results
check_crawler_results(crawler_name)