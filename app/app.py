from importlib import metadata
import streamlit as st
from io import BytesIO
import zipfile
import os
import pandas as pd 
from data_generator import DataGenerator
from db_connection import DBConnection
from table_schema import TableSchema
from data_converter import DataConverter


def main():
    st.title("AI-Powered Test Data Generator")
    st.subheader("with Constraints & Referential Integrity")

    api_key = st.text_input("provide OpenAI Key", type='password')

    st.session_state.api_key = api_key

    option = st.selectbox(
        "Choose input method to generate data:",
        ("PostgreSQL Database", "AWS Glue Catalog", "Upload a Sample File")
    )

    if option == "PostgreSQL Database" and api_key:
        gen_type = "postgres"
        st.divider()
        st.header("Enter PostgreSQL Database Connection Details")
        dbname = st.text_input("Database Name")
        user = st.text_input("Username")
        password = st.text_input("Password", type="password")
        host = st.text_input("Host")
        port = st.text_input("Port", value="5432")

        if st.button("Connect to Database"):
            dbobj = DBConnection()
            conn = dbobj.create_connection(dbname, user, password, host, port)
            if conn:
                st.success("Connected to the database successfully!")
                st.session_state.conn = conn
                st.session_state.tables = dbobj.get_tables(conn)
                st.session_state.relationships = dbobj.get_table_relationships(conn)
            else:
                st.error("Failed to connect to the database. Please check your credentials.")
        
        if 'conn' in st.session_state and 'tables' in st.session_state:
            selected_tables = st.multiselect("Select Tables", st.session_state.tables)
            if selected_tables:
                generate_data_flow(gen_type, selected_tables)

    elif option == "AWS Glue Catalog" and api_key:
        gen_type = "glue"
        st.divider()
        st.header("Enter AWS Glue Catalog Details")
        access_key = st.text_input("Access Key")
        secret_key = st.text_input("Secret Key", type="password")
        region = st.text_input("AWS Region")
        glue_database = st.text_input("Glue Database Name")

        if st.button("Connect to AWS Glue"):
            dbobj = DBConnection()
            athena_client = dbobj.get_athena_client(access_key, secret_key, region)

            st.session_state.client = athena_client
            response = athena_client.list_table_metadata(CatalogName='AwsDataCatalog',
            DatabaseName=glue_database)
        
            metadata = response['TableMetadataList']

            st.session_state.tables = [item['Name'] for item in metadata]
            
            st.success("Connected to AWS Glue Catalog!")    
            
        if 'tables' in st.session_state and 'client' in st.session_state:
            selected_tables = st.multiselect("Select Tables", st.session_state.tables)
            if selected_tables:
                generate_data_flow(gen_type, selected_tables, database=glue_database, client=st.session_state.client)

    elif option == "Upload a Sample File" and api_key:
        gen_type = "file"
        st.divider()
        st.header("Upload a Sample File to Generate Data")
        uploaded_file = st.file_uploader("Upload your file", type=["csv", "json", "xlsx", "parquet"])

        if uploaded_file:
            st.success(f"File {uploaded_file.name} uploaded successfully!")
            # Read the uploaded file and generate data based on its schema
            if uploaded_file.name.endswith(".csv"):
                data = pd.read_csv(uploaded_file)
            elif uploaded_file.name.endswith(".json"):
                data = pd.read_json(uploaded_file)
            elif uploaded_file.name.endswith(".xlsx"):
                data = pd.read_excel(uploaded_file)
            elif uploaded_file.name.endswith(".parquet"):
                data = pd.read_parquet(uploaded_file)
            
            st.write("Sample of uploaded data:")
            st.dataframe(data.head())

            generate_data_flow(gen_type, [uploaded_file.name], data=data)


def generate_data_flow(gen_type, selected_tables, database=None, client=None, data=None):
    format_options = ['CSV', 'JSON', 'EXCEL', 'PARQUET']
    selected_format = st.selectbox("Select Export Format", format_options)

    no_of_records_options = [5, 50, 500, 1000]
    selected_no_of_records = st.selectbox("Enter number of records (max 1000):", no_of_records_options)

    if st.button("Generate Data"):
        all_data_files = []
        dcobj = DataConverter()
        dgobj = DataGenerator()

        if gen_type=='file' and data is not None:
            # If data comes from file, use the data directly
            st.write("Generating data based on the uploaded file...")
            
            generated_data = dgobj.generate_data_for_files(selected_tables[0], data, selected_no_of_records)
            for table, data in generated_data.items():
                if data:
                    st.write(f"Generated Data for {table}:")
                    st.code(data[:1000] + "..." if len(data) > 1000 else data, language='sql')
                    file_name_without_ext = os.path.splitext(table)[0]
                    converted_data, filename = dcobj.convert_data_to_format(data, selected_format, file_name_without_ext)
                    all_data_files.append((converted_data, filename))
                else:
                    st.error(f"No data generated for {table}.")

        elif gen_type=='postgres':
            # If data comes from the database or AWS Glue
            tsobj = TableSchema()
            schemas = {table: tsobj.get_table_schema(table, st.session_state.conn) for table in selected_tables}
            generated_data = dgobj.generate_data_for_tables(st.session_state.conn, selected_tables, schemas, st.session_state.relationships, selected_no_of_records)

            for table, data in generated_data.items():
                if data:
                    st.write(f"Generated Data for {table}:")
                    st.code(data[:1000] + "..." if len(data) > 1000 else data, language='sql')

                    converted_data, filename = dcobj.convert_data_to_format(data, selected_format, table)
                    all_data_files.append((converted_data, filename))
                else:
                    st.error(f"No data generated for {table}.")

        elif gen_type=='glue':
            # If data comes from the database or AWS Glue
            response = client.list_table_metadata(CatalogName='AwsDataCatalog',
            DatabaseName=database)
        
            metadata = response['TableMetadataList']

            schemas  = {item['Name']:item['Columns'] for item in metadata if item['Name'] in selected_tables}

            generated_data = dgobj.generate_data_for_athena_tables(client, selected_tables, schemas, selected_no_of_records, database)

            for table, data in generated_data.items():
                if data:
                    st.write(f"Generated Data for {table}:")
                    st.code(data[:1000] + "..." if len(data) > 1000 else data, language='sql')

                    converted_data, filename = dcobj.convert_data_to_format(data, selected_format, table)
                    all_data_files.append((converted_data, filename))
                else:
                    st.error(f"No data generated for {table}.")
        # Provide download link
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


if __name__ == "__main__":
    main()
