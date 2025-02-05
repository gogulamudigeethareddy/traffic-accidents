import boto3
import time
from botocore.exceptions import ClientError
from configs.dataconfig import (
    DATASET_IDENTIFIER,
    bucket_name,
    s3_directory,
    crawler_name,
    database_name,
    role_arn,
)


def diagnose_crawler_issues(crawler_name, database_name, bucket_name, s3_directory):
    glue_client = boto3.client("glue")
    s3_client = boto3.client("s3")
    iam_client = boto3.client("iam")

    print("\n=== Starting Diagnostic Check ===")

    # 1. Verify S3 Data
    print("\n1. Checking S3 Data...")
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_directory)
        if "Contents" in response:
            print(f"✓ Found {len(response['Contents'])} files in S3")
            # Show sample of file types
            extensions = set()
            for obj in response["Contents"][:10]:
                ext = (
                    obj["Key"].split(".")[-1].lower()
                    if "." in obj["Key"]
                    else "no extension"
                )
                extensions.add(ext)
            print(f"File types found: {', '.join(extensions)}")
        else:
            print("✗ No files found in S3 location")
    except Exception as e:
        print(f"✗ Error accessing S3: {str(e)}")

    # 2. Check IAM Role Permissions
    print("\n2. Checking IAM Role...")
    try:
        crawler_info = glue_client.get_crawler(Name=crawler_name)
        role_name = crawler_info["Crawler"]["Role"].split("/")[-1]
        try:
            role_policies = iam_client.list_attached_role_policies(RoleName=role_name)
            print("Attached policies:")
            for policy in role_policies["AttachedPolicies"]:
                print(f"- {policy['PolicyName']}")
        except Exception as e:
            print(f"✗ Error checking role policies: {str(e)}")
    except Exception as e:
        print(f"✗ Error checking crawler role: {str(e)}")

    # 3. Check Crawler Configuration
    print("\n3. Checking Crawler Configuration...")
    try:
        crawler_info = glue_client.get_crawler(Name=crawler_name)
        targets = crawler_info["Crawler"]["Targets"]
        print("Crawler targets:", targets)
        print("Database name:", crawler_info["Crawler"]["DatabaseName"])
    except Exception as e:
        print(f"✗ Error checking crawler config: {str(e)}")

    # 4. Check Last Crawl Results
    print("\n4. Checking Last Crawl Results...")
    try:
        crawler_info = glue_client.get_crawler(Name=crawler_name)
        if "LastCrawl" in crawler_info["Crawler"]:
            last_crawl = crawler_info["Crawler"]["LastCrawl"]
            print("Status:", last_crawl.get("Status", "N/A"))
            print("Error message:", last_crawl.get("ErrorMessage", "None"))
            print("Tables created:", last_crawl.get("TablesCreated", 0))
            print("Tables updated:", last_crawl.get("TablesUpdated", 0))
            print("Tables deleted:", last_crawl.get("TablesDeleted", 0))
        else:
            print("No previous crawl information found")
    except Exception as e:
        print(f"✗ Error checking last crawl: {str(e)}")

    # 5. Check Database Status
    print("\n5. Checking Database Status...")
    try:
        database = glue_client.get_database(Name=database_name)
        print(f"✓ Database '{database}' exists")

        # List tables
        try:
            tables = glue_client.get_tables(DatabaseName=database_name)
            print(f"Number of tables: {len(tables['TableList'])}")
            if len(tables["TableList"]) > 0:
                print("Table names:")
                for table in tables["TableList"]:
                    print(f"- {table['Name']}")
        except Exception as e:
            print(f"✗ Error listing tables: {str(e)}")
    except Exception as e:
        print(f"✗ Error checking database: {str(e)}")


# Run the diagnostic
diagnose_crawler_issues(crawler_name, database_name, bucket_name, s3_directory)
