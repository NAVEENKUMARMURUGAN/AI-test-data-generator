import streamlit as st
from io import BytesIO
import zipfile
from data_generator import DataGenerator
from db_connection import DBConnection
from table_schema import TableSchema
from data_converter import DataConverter
from PIL import Image

def main():
    st.title("AI-Powered Test Data Generator")
    st.subheader("with Constraints & Referential Integrity")
    logo = Image.open("images/logo.png")
    st.image(logo, width=200)

    # Input fields for database connection
    st.divider()
    st.header("Enter PostgreSQL Database Connection Details")
    dbname = st.text_input("Database Name")
    user = st.text_input("Username")
    password = st.text_input("Password", type="password")
    host = st.text_input("Host")
    port = st.text_input("Port", value="5432")

    if st.button("Connect to Database"):
        dbobj = DBConnection(dbname, user, password, host, port)
        conn = dbobj.create_connection()
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
            format_options = ['CSV', 'JSON', 'EXCEL', 'PARQUET']
            selected_format = st.selectbox("Select Export Format", format_options)

            if st.button("Generate Data"):
                tsobj = TableSchema(st.session_state.conn)
                schemas = {table: tsobj.get_table_schema(table) for table in selected_tables}
                dgobj = DataGenerator(st.session_state.conn, st.session_state.relationships)
                generated_data = dgobj.generate_data_for_tables(selected_tables, schemas)

                all_data_files = []
                dcobj = DataConverter()
                for table, data in generated_data.items():
                    if data:
                        st.write(f"Generated Data for {table}:")
                        st.code(data[:1000] + "..." if len(data) > 1000 else data, language='sql')

                        converted_data, filename = dcobj.convert_data_to_format(data, selected_format, table)
                        all_data_files.append((converted_data, filename))
                    else:
                        st.error(f"No data generated for {table}.")


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
