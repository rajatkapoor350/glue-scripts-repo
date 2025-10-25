import sys
import json
import os
import boto3
from datetime import timedelta
import requests
import pandas as pd
from io import BytesIO
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Function to fetch data with pagination
def fetch_complete_data(activity_event, columns):
    # API details
    url = "https://api-in21.leadsquared.com/v2/ProspectActivity.svc/Activity/Retrieve/BySearchParameter?accessKey=u%24r919ef51c7a4caf3a92a096ad032f58ec&secretKey=74d61685e0cb386b9d3244b08ed4ec2e98e60ec3"

    all_records = []  # List to store all records
    page_index = 1    # Start with the first page
    page_size = 1000  # Number of records per page

    while True:
        # Payload for the current page
        payload = json.dumps({
            "ActivityEvent": activity_event,
            "AdvancedSearch": "{\"GrpConOp\":\"And\",\"Conditions\":[{\"Type\":\"Activity\",\"ConOp\":\"or\",\"RowCondition\":[{\"SubConOp\":\"And\",\"LSO\":\"ActivityEvent\",\"LSO_Type\":\"PAEvent\",\"Operator\":\"eq\",\"RSO\":\"" + str(activity_event) + "\"},{\"RSO\":\"\"},{\"SubConOp\":\"And\",\"LSO_Type\":\"DateTime\",\"LSO\":\"ActivityTime\",\"Operator\":\"eq\",\"RSO\":\"opt-all-time\"}]}],\"QueryTimeZone\":\"India Standard Time\"}",
            "Paging": {
                "PageIndex": page_index,
                "PageSize": page_size
            },
            "Sorting": {
                "ColumnName": "CreatedOn",
                "Direction": 1
            },
            "Columns": {
                "Include_CSV": columns
            }
        })

        headers = {
            'Content-Type': 'application/json'
        }

        # API request
        response = requests.post(url, headers=headers, data=payload)

        # Check if the response is successful
        if response.status_code == 200:
            response_json = response.json()

            # Extract the list of records
            records = response_json.get('List', [])
            all_records.extend(records)

            # If the number of records fetched is less than the page size, stop pagination
            if len(records) < page_size:
                break

            # Increment page index for the next page
            page_index += 1
        else:
            print(f"Error: API request failed with status code {response.status_code}")
            print(f"Response content: {response.text}")
            break

    # Return all records as a DataFrame
    if all_records:
        return pd.DataFrame(all_records)
    else:
        print("No records found.")
        return None

# Function to save DataFrame to S3 as Excel
def save_to_s3(dataframe, bucket_name, file_key):
    s3_client = boto3.client("s3")
    buffer = BytesIO()
    dataframe.to_excel(buffer, index=False, engine='openpyxl')
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, bucket_name, file_key)
    print(f"File saved successfully to s3://{bucket_name}/{file_key}")

# Main execution
if __name__ == "__main__":
    # Set bucket name for S3
    bucket_name = "streamlit-app-server-bucket"

    print("Processing Phone Call Activity...")
    phone_df = fetch_complete_data(
        activity_event=200,
        columns="ProspectActivityId,Status,mx_Custom_1,RelatedActivityId,mx_Custom_2,mx_Custom_13,mx_Custom_4,CreatedBy"
    )

    if phone_df is not None:
        file_key = "AllActivity/PhoneCallActivity.xlsx"
        save_to_s3(phone_df, bucket_name, file_key)
        
    #2
    print("Processing Retail Phone Call Activity...")
    retail_df = fetch_complete_data(
        activity_event=215,
        columns="ProspectActivityId,Status,mx_Custom_17,RelatedActivityId,mx_Custom_6,mx_Custom_8,CreatedBy"
    )

    if retail_df is not None:
        file_key = "AllActivity/RetailPhoneCallActivity.xlsx"
        save_to_s3(retail_df, bucket_name, file_key)
        
    #3    
    print("Processing Delivery Phone Call Activity...")
    delivery_df = fetch_complete_data(
        activity_event=216,
        columns="ProspectActivityId,Status,mx_Custom_17,RelatedActivityId,mx_Custom_9,mx_Custom_10,CreatedBy"
    )

    if delivery_df is not None:
        file_key = "AllActivity/DeliveryPhoneCallActivity.xlsx"
        save_to_s3(delivery_df, bucket_name, file_key)
        
    #4    
    print("Processing Cogent Phone Call Activity...")
    cogent_df = fetch_complete_data(
        activity_event=226,
        columns="ProspectActivityId,Status,mx_Custom_17,RelatedActivityId,mx_Custom_3,CreatedBy"
    )

    if cogent_df is not None:
        file_key = "AllActivity/CogentPhoneCallActivity.xlsx"
        save_to_s3(cogent_df, bucket_name, file_key)
        
        
    #5    
    print("Processing Fin Phone Call Activity...")
    fin_df = fetch_complete_data(
        activity_event=222,
        columns="ProspectActivityId,Status,mx_Custom_17,RelatedActivityId,CreatedBy"
    )

    if fin_df is not None:
        file_key = "AllActivity/FinPhoneCallActivity.xlsx"
        save_to_s3(fin_df, bucket_name, file_key)
