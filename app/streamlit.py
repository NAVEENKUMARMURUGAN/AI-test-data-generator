import streamlit as st
import psycopg2
from anthropic import Anthropic
from io import StringIO

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
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = cur.fetchall()
    return [table[0] for table in tables]

# Function to get the schema of the selected table
def get_table_schema(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT 
                column_name, 
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

# Function to generate data for the selected table using Claude API
def generate_data_for_table(table_name, schema):
    prompt = f"Generate sample data for the table '{table_name}' with the following schema:\n"
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

    prompt += "\nPlease generate 5 rows of sample data for this table in SQL INSERT statement format."

    anthropic = Anthropic(api_key=st.secrets["ANTHROPIC_API_KEY"])

    try:
        response = anthropic.completions.create(
            model="claude-2.1",
            max_tokens_to_sample=300,
            prompt=f"{anthropic.HUMAN_PROMPT} {prompt}{anthropic.AI_PROMPT}",
        )
        return response.completion
    except Exception as e:
        st.error(f"Failed to generate data: {e}")
        return None

# Streamlit UI
def main():
    st.title("AI-Powered Data Generator with Constraints (Using Claude API)")

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
        
        # Generate Data button
        if st.button("Generate Data"):
            st.write("Generate Data button clicked!")  # Debugging: Confirm button click

            # Fetch the schema of the selected table
            schema = get_table_schema(st.session_state.conn, table_name)

            if schema:
                st.write(f"Schema for {table_name}: {schema}")  # Debugging: Show schema
                data = generate_data_for_table(table_name, schema)
                if data:
                    st.write(f"Generated Data for {table_name}:")
                    st.code(data, language='sql')

                    # Convert generated data to a downloadable format
                    data_as_file = StringIO(data)
                    data_as_file.seek(0)
                    st.download_button(
                        label="Download Data as Text File",
                        data=data_as_file,
                        file_name=f"{table_name}_generated_data.txt",
                        mime="text/plain"
                    )
                else:
                    st.error("No data generated.")
            else:
                st.error("Failed to retrieve table schema.")

if __name__ == "__main__":
    main()