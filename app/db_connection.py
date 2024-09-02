import psycopg2
import streamlit as st

class DBConnection:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    # Function to create a database connection
    def create_connection(self):
        try:
            conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
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
