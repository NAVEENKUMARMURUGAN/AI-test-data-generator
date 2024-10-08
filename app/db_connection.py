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
                            tc.table_name AS foreign_table, 
                            kcu.column_name AS foreign_column, 
                            ccu.table_name AS referenced_table, 
                            ccu.column_name AS referenced_column
                        FROM 
                            information_schema.table_constraints AS tc 
                            JOIN information_schema.key_column_usage AS kcu
                            ON tc.constraint_name = kcu.constraint_name
                            JOIN information_schema.constraint_column_usage AS ccu
                            ON ccu.constraint_name = tc.constraint_name
                        WHERE tc.constraint_type = 'FOREIGN KEY';
                """)
                relationships = cur.fetchall()

            # Convert to list of dictionaries
            relationship_dicts = []
            for rel in relationships:
                relationship_dicts.append({
                    'child_table': rel[0],
                    'child_column': rel[1],
                    'parent_table': rel[2],
                    'parent_column': rel[3]
                })
            return relationship_dicts

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
