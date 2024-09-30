from email.policy import default
import streamlit as st
from io import BytesIO, StringIO
import zipfile
import os
import pandas as pd
import psycopg2
import boto3
from openai import OpenAI
import csv
import json
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple

class DatabaseConnector(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def get_tables(self):
        pass

    @abstractmethod
    def get_table_relationships(self):
        pass

    @abstractmethod
    def get_table_schema(self, table_name):
        pass

class PostgreSQLConnector(DatabaseConnector):
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            return True
        except psycopg2.Error as e:
            st.error(f"Failed to connect to the database: {e}")
            return False

    def get_tables(self):
        if self.conn is None:
            st.error("No connection available")
            return []

        try:
            with self.conn.cursor() as cur:
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

    def get_table_relationships(self):
        if self.conn is None:
            st.error("No connection available")
            return []

        try:
            with self.conn.cursor() as cur:
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

    def get_table_schema(self, table_name):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        col.column_name,
                        col.data_type, 
                        col.is_nullable, 
                        col.column_default, 
                        tc.constraint_type
                    FROM information_schema.columns col
                    LEFT JOIN information_schema.key_column_usage kcu 
                        ON col.table_name = kcu.table_name 
                        AND col.column_name = kcu.column_name 
                        AND col.table_schema = kcu.table_schema
                    LEFT JOIN information_schema.table_constraints tc 
                        ON kcu.constraint_name = tc.constraint_name
                        AND tc.constraint_type = 'PRIMARY KEY'
                    WHERE col.table_name = %s 
                    AND col.table_schema = 'public'
                """, (table_name,))
                schema = cur.fetchall()
            return schema
        except psycopg2.Error as e:
            st.error(f"Failed to retrieve schema for table {table_name}: {e}")
            return None

class AWSGlueConnector(DatabaseConnector):
    def __init__(self, access_key, secret_key, region, database):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.database = database
        self.client = None

    def connect(self):
        try:
            self.client = self.assume_role(self.access_key, self.secret_key, self.region)
            return True
        except Exception as e:
            st.error(f"Failed to connect to AWS Glue: {e}")
            return False

    def assume_role(self, aws_access_key_id, aws_secret_access_key, region):
        sts_client = boto3.client(
            'sts',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        
        assumed_role_object = sts_client.assume_role(
            RoleArn="arn:aws:iam::463470974852:role/StreamlitGlueRole",
            RoleSessionName="AssumeRoleSession1"
        )

        credentials = assumed_role_object['Credentials']

        athena_client = boto3.client(
            'athena',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
            region_name=region
        )
        
        return athena_client

    def get_tables(self):
        if self.client is None:
            st.error("No connection available")
            return []

        try:
            response = self.client.list_table_metadata(
                CatalogName='AwsDataCatalog',
                DatabaseName=self.database
            )
            return [item['Name'] for item in response['TableMetadataList']]
        except Exception as e:
            st.error(f"Failed to retrieve tables: {e}")
            return []

    def get_table_relationships(self):
        # AWS Glue doesn't provide a straightforward way to get table relationships
        # This method would need to be implemented based on specific requirements
        return []

    def get_table_schema(self, table_name):
        if self.client is None:
            st.error("No connection available")
            return None

        try:
            response = self.client.get_table(
                CatalogName='AwsDataCatalog',
                DatabaseName=self.database,
                TableName=table_name
            )
            return response['Table']['StorageDescriptor']['Columns']
        except Exception as e:
            st.error(f"Failed to retrieve schema for table {table_name}: {e}")
            return None

class DataGenerator:
    def __init__(self, api_key):
        self.client = OpenAI(api_key=api_key)

    def generate_data(self, tables: List[str], schemas: Dict[str, List], relationships: List[Tuple], no_of_records: int) -> Dict[str, str]:
        generated_data = {}
        foreign_key_data = {}

        # Sort tables based on dependencies
        sorted_tables = self.topological_sort(tables, relationships)

        for table in sorted_tables:
            schema = schemas[table]
            prompt = self.create_prompt(table, schema, no_of_records, relationships, foreign_key_data)

            try:
                response = self.client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are an AI test data generator."},
                        {"role": "user", "content": prompt}
                    ]
                )
                generated_data[table] = response.choices[0].message.content

                # Extract foreign key data for use in related tables
                self.extract_foreign_key_data(table, generated_data[table], relationships, foreign_key_data)

            except Exception as e:
                st.error(f"Failed to generate data for {table}: {e}")
                generated_data[table] = None

        return generated_data

    def create_prompt(self, table: str, schema: List, no_of_records: int, relationships: List[Tuple], foreign_key_data: Dict[str, List]) -> str:
        prompt = f"Generate sample data for the table '{table}' with the following schema:\n"
        prompt += f"Generate exactly {no_of_records} records.\n"
        prompt += "Rules:\n"
        prompt += "1. Generate new, realistic data following the patterns and constraints.\n"
        prompt += "2. Produce only CSV content (header row followed by data rows).\n"
        prompt += "3. Don't use ``` in the output.\n"

        for column in schema:
            column_name, data_type, is_nullable, column_default, constraint_type = column
            constraints = []
            if constraint_type == 'PRIMARY KEY':
                constraints.append("PRIMARY KEY")
            if is_nullable == 'NO':
                constraints.append("NOT NULL")
            else:
                constraints.append("NULLABLE")
            if column_default:
                constraints.append(f"DEFAULT {column_default}")
            prompt += f"- {column_name} ({data_type}) {' '.join(constraints)}\n"

        # Add foreign key constraints
        for rel in relationships:
            if rel[0] == table:
                fk_column, ref_table, ref_column = rel[1], rel[2], rel[3]
                prompt += f"4. Ensure referential integrity: {fk_column} references {ref_table}.{ref_column}\n"
                if ref_table in foreign_key_data:
                    prompt += f"   Use only these values for {fk_column}: {foreign_key_data[ref_table]}\n"

        return prompt

    def extract_foreign_key_data(self, table: str, data: str, relationships: List[Tuple], foreign_key_data: Dict[str, List]):
        lines = data.strip().split('\n')
        if len(lines) < 2:  # Ensure we have at least a header and one data row
            return

        header = lines[0].split(',')
        for rel in relationships:
            if rel[0] == table:
                column_index = header.index(rel[1])
                foreign_key_data[table] = [line.split(',')[column_index] for line in lines[1:]]

    def topological_sort(self, tables: List[str], relationships: List[Tuple]) -> List[str]:
        graph = {table: set() for table in tables}
        for rel in relationships:
            if rel[0] in graph and rel[2] in graph:
                graph[rel[2]].add(rel[0])

        result = []
        visited = set()

        def dfs(node):
            visited.add(node)
            for neighbor in graph[node]:
                if neighbor not in visited:
                    dfs(neighbor)
            result.append(node)

        for table in tables:
            if table not in visited:
                dfs(table)

        return result[::-1]

class DataConverter:
    @staticmethod
    def convert_data(data, format, table_name):
        if not data:
            raise ValueError("No data provided for conversion")
        
        reader = csv.DictReader(StringIO(data))
        records = list(reader)

        if not records:
            raise ValueError("No records found in the provided data")
        
        if format == 'CSV':
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(records)
            return output.getvalue().encode('utf-8'), f"{table_name}.csv"
        
        elif format == 'JSON':
            return json.dumps(records, indent=2).encode('utf-8'), f"{table_name}.json"
        
        elif format == 'EXCEL':
            output = BytesIO()
            df = pd.DataFrame(records)
            df.to_excel(output, index=False, engine='openpyxl')
            return output.getvalue(), f"{table_name}.xlsx"
        
        elif format == 'PARQUET':
            output = BytesIO()
            df = pd.DataFrame(records)
            df.to_parquet(output, index=False)
            return output.getvalue(), f"{table_name}.parquet"
        
        else:
            raise ValueError(f"Unsupported format: {format}")

class TestDataGenerator:
    def __init__(self):
        self.db_connector = None
        self.data_generator = None
        self.data_converter = DataConverter()

    def setup_database_connection(self, db_type, **kwargs):
        if db_type == "postgres":
            self.db_connector = PostgreSQLConnector(**kwargs)
        elif db_type == "glue":
            self.db_connector = AWSGlueConnector(**kwargs)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

        return self.db_connector.connect()

    def setup_data_generator(self, api_key):
        self.data_generator = DataGenerator(api_key)

    def generate_data(self, selected_tables, no_of_records, output_format):
        if not self.db_connector or not self.data_generator:
            raise ValueError("Database connection and data generator must be set up first")

        schemas = {table: self.db_connector.get_table_schema(table) for table in selected_tables}
        relationships = self.db_connector.get_table_relationships()
        
        # Filter relationships to include only selected tables
        filtered_relationships = [rel for rel in relationships if rel[0] in selected_tables and rel[2] in selected_tables]

        generated_data = self.data_generator.generate_data(selected_tables, schemas, filtered_relationships, no_of_records)

        all_data_files = []
        for table, data in generated_data.items():
            if data:
                converted_data, filename = self.data_converter.convert_data(data, output_format, table)
                all_data_files.append((converted_data, filename))

        return all_data_files

def main():
    st.title("AI-Powered Test Data Generator")
    st.subheader("with Constraints & Referential Integrity")

    # Initialize the API key in session state
    if 'api_key' not in st.session_state:
        st.session_state.api_key = ""

    api_key = st.text_input("Provide OpenAI Key", type='password', value=st.session_state.api_key)
    st.session_state.api_key = api_key

    option = st.selectbox(
        "Choose input method to generate data:",
        ("PostgreSQL Database", "AWS Glue Catalog")
    )

    test_data_generator = TestDataGenerator()

    # Check if API key is provided
    if api_key:
        if option == "PostgreSQL Database":
            st.divider()
            st.header("Enter PostgreSQL Database Connection Details")
            dbname = st.text_input("Database Name")
            user = st.text_input("Username")
            password = st.text_input("Password", type="password")
            host = st.text_input("Host")
            port = st.text_input("Port", value="5432")

            if st.button("Connect to Database"):
                if test_data_generator.setup_database_connection("postgres", dbname=dbname, user=user, password=password, host=host, port=port):
                    st.success("Connected to the database successfully!")
                    st.session_state.tables = test_data_generator.db_connector.get_tables()
                    st.session_state.relationships = test_data_generator.db_connector.get_table_relationships()
                    # Set up the data generator here
                    test_data_generator.setup_data_generator(api_key)
                else:
                    st.error("Failed to connect to the database. Please check your credentials.")

        elif option == "AWS Glue Catalog":
            st.divider()
            st.header("Enter AWS Glue Catalog Details")
            access_key = st.text_input("Access Key")
            secret_key = st.text_input("Secret Key", type="password")
            region = st.text_input("AWS Region")
            glue_database = st.text_input("Glue Database Name")

            if st.button("Connect to AWS Glue"):
                if test_data_generator.setup_database_connection("glue", access_key=access_key, secret_key=secret_key, region=region, database=glue_database):
                    st.success("Connected to AWS Glue Catalog!")
                    st.session_state.tables = test_data_generator.db_connector.get_tables()
                    # Set up the data generator here
                    test_data_generator.setup_data_generator(api_key)
                else:
                    st.error("Failed to connect to AWS Glue. Please check your credentials.")
    else:
        st.warning("Please enter your OpenAI API key.")

    # Check if tables are loaded in session state
    if 'tables' in st.session_state:
        selected_tables = st.multiselect("Select Tables", st.session_state.tables)
        if selected_tables:
            format_options = ['CSV', 'JSON', 'EXCEL', 'PARQUET']
            selected_format = st.selectbox("Select Export Format", format_options)

            no_of_records_options = [5, 50, 500, 1000]
            selected_no_of_records = st.selectbox("Enter number of records (max 1000):", no_of_records_options)

            if st.button("Generate Data"):
                if test_data_generator.db_connector and test_data_generator.data_generator:
                    all_data_files = test_data_generator.generate_data(selected_tables, selected_no_of_records, selected_format)

                    if all_data_files:
                        zip_buffer = BytesIO()
                        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                            for file_content, file_name in all_data_files:
                                zip_file.writestr(file_name, file_content)

                        st.download_button(
                            label=f"Download All Generated Data as {selected_format}",
                            data=zip_buffer.getvalue(),
                            file_name=f"generated_data_{selected_format.lower()}.zip",
                            mime="application/zip"
                        )
                else:
                    st.error("Please connect to a database and provide an OpenAI API key before generating data.")

if __name__ == "__main__":
    main()
