from openai import OpenAI
import streamlit as st
import pandas as pd
import time

class DataGenerator:
    def __init__(self):
        self.client = OpenAI()

    def understand_data(self, conn, table_name):
        if not table_name.isidentifier():
            raise ValueError("Invalid table name")
        
        with conn.cursor() as cur:
            query = f"SELECT * FROM {table_name} LIMIT 100"
            cur.execute(query)
            data = cur.fetchall()
        return data

    def generate_data_for_tables(self, conn, selected_tables, schemas, relationships, no_of_records):
        data = {}
        for table in selected_tables:
            sample_data = self.understand_data(conn, table)
            prompt = f"Generate sample data for the table '{table}' with the following schema:\n"
            prompt += f"Here are sample records retrieved from the table '{table}':\n {sample_data}\n"
            prompt += "And here are the rules:\n"
            prompt += "1. STRICTLY UNDERSTAND THE PATTERN AND GENERATE BUT DON'T USE THE SAME DATA DURING GENERATION PRODUCE NEW\n"
            prompt += f"2. STRICTLY GENERATE '{no_of_records}' of records in the output\n"
            prompt += "3. ONLY PROVIDE DATA, NOT INSERT QUERY\n"
            prompt += "4. STRICTLY PRODUCE ONLY CSV CONTENT WHICH CAN BE WRITTEN TO A FILE, NOT PYTHON OBJECTS\n"
            prompt += "5. DON'T USE ``` in OUTPUT\n"

            for column in schemas[table]:
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
            
            # Add information about foreign key relationships
            for rel in relationships:
                if rel[0] == table:
                    prompt += f"Ensure referential integrity with {rel[2]} table for column {rel[1]}\n"

            try:
                response = self.client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are a AI test data generator."},
                        {"role": "user", "content": prompt}
                    ]
                )

                data[table] = response.choices[0].message.content
            except Exception as e:
                st.error(f"Failed to generate data for {table}: {e}")
                data[table] = None

        return data
    
    def generate_data_for_files(self, file_name, data_content, no_of_records):
        data = {}
        sample_data = data_content
        prompt = f"Generate sample data for the table '{file_name}' with the following schema:\n"
        prompt += f"Here are sample records retrieved from the table '{file_name}':\n {sample_data}\n"
        prompt += "And here are the rules:\n"
        prompt += "1. STRICTLY UNDERSTAND THE PATTERN AND GENERATE BUT DON'T USE THE SAME DATA DURING GENERATION PRODUCE NEW\n"
        prompt += f"2. STRICTLY GENERATE '{no_of_records}' of records in the output\n"
        prompt += "3. ONLY PROVIDE DATA, NOT INSERT QUERY\n"
        prompt += "4. STRICTLY PRODUCE ONLY CSV CONTENT WHICH CAN BE WRITTEN TO A FILE, NOT PYTHON OBJECTS\n"
        prompt += "5. DON'T USE ``` in OUTPUT\n"


        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are an AI test data generator."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            data[file_name] = response.choices[0].message.content
           
        except Exception as e:
            st.error(f"Failed to generate data for {file_name}: {e}")
            data[file_name] = None

        return data
    
    def run_athena_query(self, client, database, table):

        query = f"select * from {table} limit 10"
        # Start query execution
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': 's3://test-data-gen-sample-athena/'}
        )
        query_execution_id = response['QueryExecutionId']

        # Wait for query to finish
        while True:
            status = client.get_query_execution(QueryExecutionId=query_execution_id)
            state = status['QueryExecution']['Status']['State']
            if state == 'SUCCEEDED':
                print("Query succeeded")
                break
            elif state in ['FAILED', 'CANCELLED']:
                print(f"Query {state}: {status['QueryExecution']['Status']['StateChangeReason']}")
                return None
            time.sleep(2)

        # Get query results
        results = client.get_query_results(QueryExecutionId=query_execution_id)
        return results

    def generate_data_for_athena_tables(self, client, selected_tables, schemas, no_of_records, database):
        data = {}
        for table in selected_tables:
            sample_data = self.run_athena_query(client, database, table)
            prompt = f"Generate sample data for the table '{table}' with the following schema:\n"
            prompt = f"{schemas}"
            prompt += f"Here are sample records retrieved from the table '{table}':\n {sample_data}\n"
            prompt += "And here are the rules:\n"
            prompt += "1. STRICTLY UNDERSTAND THE PATTERN AND GENERATE BUT DON'T USE THE SAME DATA DURING GENERATION PRODUCE NEW\n"
            prompt += f"2. STRICTLY GENERATE '{no_of_records}' of records in the output\n"
            prompt += "3. ONLY PROVIDE DATA, NOT INSERT QUERY\n"
            prompt += "4. STRICTLY PRODUCE ONLY CSV CONTENT WHICH CAN BE WRITTEN TO A FILE, NOT PYTHON OBJECTS\n"
            prompt += "5. DON'T USE ``` in OUTPUT\n"

            try:
                response = self.client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are a AI test data generator."},
                        {"role": "user", "content": prompt}
                    ]
                )

                data[table] = response.choices[0].message.content
            except Exception as e:
                st.error(f"Failed to generate data for {table}: {e}")
                data[table] = None

        return data
