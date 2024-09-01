import streamlit as st
import psycopg2
from openai import OpenAI
from io import StringIO, BytesIO
import csv
import json
import pandas as pd

client = OpenAI()

# Function to create a database connection
def create_connection(dbname, user, password, host, port):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to the database: {e}")
        return None

# Function to get the list of tables
def get_tables(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
            """)
            tables = cur.fetchall()
        return [table[0] for table in tables]
    except Exception as e:
        st.write(e)
        with conn.cursor() as cur:
            cur.execute("""ROLLBACK""")



# Function to get the schema of the selected table
def get_table_schema(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT 
                col.column_name,
                data_type, 
                is_nullable, 
                column_default, 
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

def understand_data(conn, table_name):
    # Ensure the table_name is properly sanitized to avoid SQL injection
    if not table_name.isidentifier():
        raise ValueError("Invalid table name")
    
    with conn.cursor() as cur:
        # Use f-string or .format() to include the table name in the query
        query = f"SELECT * FROM {table_name} LIMIT 10"
        cur.execute(query)
        data = cur.fetchall()
    return data

def convert_data_to_format(data, format):
    # Convert the string data to a list of dictionaries
    reader = csv.DictReader(StringIO(data))
    records = list(reader)
    
    if format == 'CSV':
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(records)
        return output.getvalue().encode('utf-8'), 'text/csv'
    
    elif format == 'JSON':
        return json.dumps(records, indent=2).encode('utf-8'), 'application/json'
    
    elif format == 'EXCEL':
        output = BytesIO()
        df = pd.DataFrame(records)
        df.to_excel(output, index=False, engine='openpyxl')
        return output.getvalue(), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    
    elif format == 'PARQUET':
        output = BytesIO()
        df = pd.DataFrame(records)
        df.to_parquet(output, index=False)
        return output.getvalue(), 'application/octet-stream'

# Function to generate data for the selected table
def generate_data_for_table(conn, table_name, schema):
    data = understand_data(conn, table_name)
    prompt = f"Generate sample data for the table '{table_name}' with the following schema:\n"
    rule_1 = f"Here is the sample records retrived from the table '{table_name}':\n {data} STRICTLY UNDERSTAND THE PATTERN AND GENERATE BUT DON't USE THE SAME DATA DURING GENERATION PRODUCE NEW"
    rule_2 = f"And, here are the rules, you just need to ONLY  respond data, NO EXTRA Comments\n"
    rule_3 = f"Generate ONLY max 100 records"
    rule_4 = f"ONLY PROVIDE DATA, NOT INSERT QUERY I DONT NEED TO PARSE BEFORE WRITING TO A FILE"
    rule_5 = f"STRICTLY MAKE SURE YOU PRODUCE ONLY CSV CONTENT WHICH CAN BE WRIITEN TO A FILE NOT PYTHON OBJECTS, DON'T USE ``` in OUTPUT"

    prompt = prompt + rule_1 + rule_2 + rule_3 + rule_4 + rule_5
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


    try:

        response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a test data generator."},
            {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        st.error(f"Failed to generate data: {e}")
        return None

# Streamlit UI
def main():
    st.title("AI-Powered Test Data Generator with Constraints")
    st.subheader("By Understanding Production data pattern")

    # Input fields for database connection
    st.header("Enter PostgreSQL Database Connection Details")
    dbname = st.text_input("Database Name")
    user = st.text_input("Username")
    password = st.text_input("Password", type="password")
    host = st.text_input("Host")
    port = st.text_input("Port", value="5432")

    # Button to connect to the database
    if st.button("Connect to Database"):
        conn = create_connection(dbname, user, password, host, port)
        if conn:
            st.success("Connected to the database successfully!")

            # Store the connection in session state
            st.session_state.conn = conn

            # Get list of tables
            tables = get_tables(conn)
            st.session_state.tables = tables  # Save the tables in session state

            # Debugging: Show found tables
            st.write(f"Tables found: {tables}")

    # Check if connection exists in session state
    if 'conn' in st.session_state and 'tables' in st.session_state:
        # Dropdown for table selection
        table_name = st.selectbox("Select a Table", st.session_state.tables)

         # Add format selection
        format_options = ['CSV', 'JSON', 'EXCEL', 'PARQUET']
        selected_format = st.selectbox("Select Export Format", format_options)

        if st.button("Generate Data"):
                # Fetch the schema of the selected table
                schema = get_table_schema(st.session_state.conn, table_name)

                if schema:
                    st.code(f"Schema for {table_name}: {schema}", language='sql')
                    data = generate_data_for_table(st.session_state.conn, table_name, schema)
                    if data:
                        st.write(f"Generated Data for {table_name}:")
                        st.code(data, language='sql')

                        # Convert generated data to the selected format
                        converted_data, mime_type = convert_data_to_format(data, selected_format)

                        # Prepare the file extension based on the selected format
                        file_extensions = {
                            'CSV': 'csv',
                            'JSON': 'json',
                            'EXCEL': 'xlsx',
                            'PARQUET': 'parquet'
                        }
                        file_extension = file_extensions[selected_format]

                        st.download_button(
                            label=f"Download Data as {selected_format}",
                            data=converted_data,
                            file_name=f"{table_name}_generated_data.{file_extension}",
                            mime=mime_type
                        )
                    else:
                        st.error("No data generated.")
                else:
                    st.error("Failed to retrieve table schema.")

if __name__ == "__main__":
    main()