import psycopg2
import streamlit as st
import boto3

class DBConnection:

    # Function to create a database connection
    def create_connection(self, dbname, user, password, host, port):
        try:
            conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            return conn
        except psycopg2.Error as e:
            st.error(f"Failed to connect to the database: {e}")
            return None
        
    # Function to get the list of tables
    def get_tables(self, conn):
        if conn is None:
            st.error("No connection available")
            return []

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                """)
                tables = cur.fetchall()
            return [table[0] for table in tables]
        except psycopg2.Error as e:
            st.error(f"Failed to retrieve tables: {e}")
            return []

    # Function to get the relationships between tables
    def get_table_relationships(self, conn):
        if conn is None:
            st.error("No connection available")
            return []

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        tc.table_name, kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM 
                        information_schema.table_constraints AS tc 
                        JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                        JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                        AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY';
                """)
                relationships = cur.fetchall()
            return relationships
        except psycopg2.Error as e:
            st.error(f"Failed to retrieve table relationships: {e}")
            return []


    def assume_role(self, aws_access_key_id, aws_secret_access_key, region):
        # Provide your user's AWS Access Key and Secret Key
        sts_client = boto3.client(
            'sts',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        
        # Assume the role
        assumed_role_object = sts_client.assume_role(
            RoleArn="arn:aws:iam::463470974852:role/StreamlitGlueRole",  # Replace with the Role ARN
            RoleSessionName="AssumeRoleSession1"
        )

        # Get temporary credentials
        credentials = assumed_role_object['Credentials']

        # Use temporary credentials to create a new Boto3 client for Glue
        athena_client = boto3.client(
            'athena',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
            region_name=region
        )
        
        return athena_client

    def get_athena_client(self, aws_access_key_id, aws_secret_access_key, region):

        athena_client = self.assume_role(aws_access_key_id, aws_secret_access_key, region)
        
        return athena_client
    
    # def get_schema(self, table):

    #     athena_client = self.assume_role(aws_access_key_id, aws_secret_access_key, region)
        
    #     response = athena_client.list_table_metadata(CatalogName='AwsDataCatalog',
    #                 DatabaseName=database
    #                 )
        
    #     tables = response['TableMetadataList']
        
    #     return tables
