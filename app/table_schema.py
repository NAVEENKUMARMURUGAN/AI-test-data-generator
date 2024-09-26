import psycopg2
import streamlit as st

class TableSchema:

    # Function to get the schema of the selected table
    def get_table_schema(self, table_name, conn):
        try:
            with conn.cursor() as cur:
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
    