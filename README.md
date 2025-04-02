# Car Rental Marketplace

<p align="center">
    <img src="images/architecture_diagram (1).jpg" alt="The architecture diagram" width="100%" />
</p>



### Load Data 

The `load_data.py` provides functionality to upload files from a local folder to an S3 bucket.

#### Usage

1. Ensure your `.env` file contains the following variables:
   ```
   ACCESS_KEYS=your_aws_access_key
   SECRET_KEYS=your_aws_secret_key
   REGION=your_aws_region
   BUCKET_NAME=your_s3_bucket_name
   ```

2. Import and use the module in your code:
   ```python

   LOCAL_FOLDER = "../data"
   S3_BUCKET = "your_s3_bucket_name"

   upload_files_to_s3(LOCAL_FOLDER, S3_BUCKET)
   ```
