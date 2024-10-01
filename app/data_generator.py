import boto3
import streamlit as st
import pandas as pd
from io import StringIO
from typing import List, Dict

class DataGenerator:
    def __init__(self):
        # Initialize the boto3 client for Bedrock
        self.client = boto3.client('bedrock-agent-runtime', region_name="ap-southeast-2")  # Update the region as necessary
        self.agent_id = 'LGWJLKSFOV'  # Replace with your actual Bedrock agent ID
        self.agent_alias_id='ZEIHOS2QG0'

    def invoke_bedrock_model(self, prompt: str, session_id: str):
        """Invoke Amazon Bedrock agent with the given prompt."""
        try:
            # Invoking the Bedrock agent
            response = self.client.invoke_agent(
                agentId=self.agent_id,
                agentAliasId=self.agent_alias_id,
                sessionId=session_id,
                inputText=prompt
            )
            
            completion = ""

            for event in response.get("completion"):
                chunk = event["chunk"]
                completion = completion + chunk["bytes"].decode()

            return completion
        except Exception as e:
            st.error(f"Error invoking Bedrock agent: {e}")
            return None

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
    
    def generate_data_for_files(self, file_name, data_content, no_of_records, session_id):
        data = {}
        sample_data = data_content
        prompt = f"Generate sample data for the table '{file_name}' with the following schema:\n"
        prompt += f"Here are sample records retrieved from the table '{file_name}':\n {sample_data}\n"
        prompt += "And here are the rules:\n"
        prompt += "1. STRICTLY UNDERSTAND THE PATTERN AND GENERATE BUT DON'T USE THE SAME DATA DURING GENERATION PRODUCE NEW\n"
        prompt += f"2. STRICTLY GENERATE '{no_of_records}' records in the output\n"
        prompt += "3. ONLY PROVIDE DATA, NOT INSERT QUERY\n"
        prompt += "4. STRICTLY PRODUCE ONLY CSV CONTENT WHICH CAN BE WRITTEN TO A FILE, NOT PYTHON OBJECTS\n"
        prompt += "5. DON'T USE ``` IN OUTPUT\n"
        
        try:
            # Invoke the Bedrock agent
            response = self.invoke_bedrock_model(prompt, 'testdata1234567')
            
            if response:
                data[file_name] = response
            else:
                data[file_name] = None
        except Exception as e:
            st.error(f"Failed to generate data for {file_name}: {e}")
            data[file_name] = None
        return data

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
                # Invoke Bedrock agent
                response = self.invoke_bedrock_model(prompt, 'testdata1234567')

                if response:
                    generated_data = response
                    data[table] = generated_data

                    # Extract generated values for potential foreign key references
                    df = pd.read_csv(StringIO(generated_data))  # Use StringIO from io module
                    for column in df.columns:
                        foreign_key_values[table][column] = df[column].tolist()

                else:
                    data[table] = None

            except Exception as e:
                st.error(f"Failed to generate data for {table}: {e}")
                data[table] = None

        return data
