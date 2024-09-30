from ast import Str
from openai import OpenAI
import streamlit as st
import pandas as pd
import time
from typing import List, Dict, Tuple
from io import StringIO

from openai import OpenAI
import streamlit as st
import pandas as pd
from typing import List, Dict

class DataGenerator:
    def __init__(self):
        self.client = OpenAI(api_key=st.session_state.api_key)

    def sort_tables_by_dependency(self, selected_tables: List[str], relationships: List[Dict]):
        """Sort tables based on foreign key dependencies."""
        dependency_graph = {table: set() for table in selected_tables}
        
        for rel in relationships:
            child_table = rel['child_table']
            parent_table = rel['parent_table']
            
            if child_table in selected_tables and parent_table in selected_tables:
                dependency_graph[child_table].add(parent_table)

        sorted_tables = []
        while dependency_graph:
            independent_tables = [t for t, deps in dependency_graph.items() if not deps]
            
            if not independent_tables:
                raise ValueError("Circular dependency detected!")
            
            for table in independent_tables:
                sorted_tables.append(table)
                del dependency_graph[table]
            
            for deps in dependency_graph.values():
                deps.difference_update(independent_tables)

        return sorted_tables

    def understand_data(self, conn, table_name):
        """Retrieve sample data from the specified table."""
        if not table_name.isidentifier():
            raise ValueError("Invalid table name")
        
        with conn.cursor() as cur:
            query = f"SELECT * FROM {table_name} LIMIT 100"
            cur.execute(query)
            data = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]

        df = pd.DataFrame(data, columns=column_names)
        return df

    def generate_data_for_tables(self, conn, selected_tables: List[str], schemas, relationships, no_of_records):
        data = {}
        foreign_key_values = {table: {} for table in selected_tables}

        sorted_tables = self.sort_tables_by_dependency(selected_tables, relationships)

        for table in sorted_tables:
            sample_data = self.understand_data(conn, table)
            prompt = f"Generate sample data for the table '{table}' with the following schema:\n"

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

            prompt += f"\nHere are sample records retrieved from the table '{table}':\n ```{sample_data}``` \n"
            prompt += "\nAnd here are the rules:\n"
            prompt += f"1. STRICTLY UNDERSTAND THE PATTERN AND GENERATE BUT DON'T USE THE SAME DATA DURING GENERATION. PRODUCE NEW.\n"
            prompt += f"2. STRICTLY GENERATE '{no_of_records}' records in the output.\n"
            prompt += "3. ONLY PROVIDE DATA, NOT INSERT QUERIES.\n"
            prompt += "4. STRICTLY PRODUCE ONLY CSV CONTENT WHICH CAN BE WRITTEN TO A FILE, NOT PYTHON OBJECTS.\n"
            prompt += "5. DON'T USE ``` IN OUTPUT.\n"

            # Add foreign key constraints
            for rel in relationships:
                if rel['child_table'] == table:
                    foreign_column = rel['child_column']
                    referenced_table = rel['parent_table']
                    referenced_column = rel['parent_column']
                    if referenced_table in foreign_key_values and referenced_column in foreign_key_values[referenced_table]:
                        prompt += f"6. For column {foreign_column}, use values from this list to ensure referential integrity: {foreign_key_values[referenced_table][referenced_column]}\n"

            try:
                response = self.client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are an AI test data generator."},
                        {"role": "user", "content": prompt}
                    ]
                )

                generated_data = response.choices[0].message.content
                data[table] = generated_data

                # Extract generated values for potential foreign key references
                df = pd.read_csv(StringIO(generated_data))  # Use StringIO from io module
                for column in df.columns:
                    foreign_key_values[table][column] = df[column].tolist()

            except Exception as e:
                st.error(f"Failed to generate data for {table}: {e}")
                data[table] = None

        return data


    def run_athena_query(self, client, database, table):
        """Run a query on Athena to retrieve data."""
        query = f"SELECT * FROM {table} LIMIT 10"
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': 's3://your-output-bucket/'}
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

        results = client.get_query_results(QueryExecutionId=query_execution_id)
        return results

    def generate_data_for_athena_tables(self, client, selected_tables: List[str], schemas: Dict, no_of_records: int, database: str):
        """Generate data for Athena tables."""
        data = {}
        for table in selected_tables:
            sample_data = self.run_athena_query(client, database, table)
            prompt = f"Generate sample data for the table '{table}' with the following schema:\n"
            prompt += f"{schemas}"
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
                        {"role": "system", "content": "You are an AI test data generator."},
                        {"role": "user", "content": prompt}
                    ]
                )
                data[table] = response.choices[0].message.content
            except Exception as e:
                st.error(f"Failed to generate data for {table}: {e}")
                data[table] = None

        return data
