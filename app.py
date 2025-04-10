import pymysql
import boto3
import json


def get_secret():
    secret_name = "lambda-dbsecrets"
    region_name = "ap-south-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        return json.loads(get_secret_value_response['SecretString'])
    except e:
        print("Error retrieving secret:", e.response['Error']['Message'])
        return None


# Establish a database connection
def connect_to_rds():
    secret = get_secret()
    print(secret)
    if not secret:
        raise ValueError("Failed to retrieve database secrets error-message.")

    db_host = secret['host']
    db_user = secret['username']
    db_pass = secret['password']
    new_db_name = "test"  # Change as needed
    table_name = "mytable"  # Change as needed

    connection = pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection


# Lambda function handler
def lambda_handler(event, context):
    connection = None
    try:
        connection = connect_to_rds()
        with connection.cursor() as cursor:
            # Create a new database
            new_db_name = "test"
            table_name = "mytable"

            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {new_db_name};")
            cursor.execute(f"USE {new_db_name};")

            # Create a new table
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(create_table_sql)

            print(f"Database '{new_db_name}' and table '{table_name}' created successfully.")
            return {
                'statusCode': 200,
                'body': f"Database '{new_db_name}' and table '{table_name}' created successfully."
            }
    except Exception as e:
        print("Error:", str(e))
        return {
            'statusCode': 500,
            'body': str(e)
        }
    finally:
        if connection:
            connection.close()
