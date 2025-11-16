import pandas as pd
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
import boto3
import io

# AWS clients
ssm = boto3.client('ssm', region_name='ap-south-2')
s3 = boto3.client('s3', region_name='ap-south-2')

# Get Snowflake credentials from AWS SSM
sf_username = ssm.get_parameter(Name='/snowflake/username', WithDecryption=True)['Parameter']['Value']
sf_password = ssm.get_parameter(Name='/snowflake/password', WithDecryption=True)['Parameter']['Value']
sf_account = ssm.get_parameter(Name='/snowflake/accountname', WithDecryption=True)['Parameter']['Value']

# Main function
def run_script():

    # Create Snowflake connection
    def create_connection():
        conn = snow.connect(
            user=sf_username,
            password=sf_password,
            account=sf_account,
            warehouse="COMPUTE_WH",
            database="PROD",
            schema="DBT_RAW"
        )
        cursor = conn.cursor()
        print('SQL Connection Created')
        return cursor, conn

    # Truncate tables before load
    def truncate_table():
        cur, conn = create_connection()
        sql_titles = "TRUNCATE TABLE IF EXISTS TITLES_RAW"
        sql_credits = "TRUNCATE TABLE IF EXISTS CREDITS_RAW"
        cur.execute(sql_titles)
        cur.execute(sql_credits)
        print('Tables truncated')
        cur.close()
        conn.close()

    # Load data from S3 into Snowflake
    def load_data():
        # Fetch files from S3
        titles_obj = s3.get_object(Bucket='nelix-analycs-data', Key='raw_files/titles.csv')
        credits_obj = s3.get_object(Bucket='nelix-analycs-data', Key='raw_files/credits.csv')

        # Read CSVs into DataFrames
        titles_df = pd.read_csv(io.BytesIO(titles_obj['Body'].read()))
        print("Titles file read")
        credits_df = pd.read_csv(io.BytesIO(credits_obj['Body'].read()))
        print("Credits file read")

        # Create Snowflake connection
        cur, conn = create_connection()

        # Load data into Snowflake
        write_pandas(conn, titles_df, "TITLES_RAW", auto_create_table=True)
        print('Titles file loaded')
        write_pandas(conn, credits_df, "CREDITS_RAW", auto_create_table=True)
        print('Credits file loaded')

        cur.close()
        conn.close()

    # Run the modules
    print("Starting Script")
    truncate_table()
    load_data()

