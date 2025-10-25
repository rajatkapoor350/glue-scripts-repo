import boto3, os, hashlib
from pathlib import Path

# connect to AWS S3
s3 = boto3.client('s3')
bucket = os.environ['BUCKET']

# path to your Glue scripts folder
jobs_dir = Path('glue-jobs')

# loop through all job folders
for job_folder in jobs_dir.iterdir():
    if not job_folder.is_dir():
        continue
    script = job_folder / 'job.py'
    if not script.exists():
        continue

    # upload file to S3 (same name)
    s3_key = f"jobs/{job_folder.name}/job.py"
    s3.upload_file(str(script), bucket, s3_key)
    print(f"✅ Uploaded {job_folder.name} to s3://{bucket}/{s3_key}")
