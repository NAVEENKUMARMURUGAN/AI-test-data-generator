from openai import OpenAI
import streamlit as st

class DataGenerator:
    def __init__(self, conn, relationships):
        self.conn = conn
        self.relationships = relationships
        self.client = OpenAI()

    def understand_data(self, table_name):
        if not table_name.isidentifier():
            raise ValueError("Invalid table name")
        
        with self.conn.cursor() as cur:
            query = f"SELECT * FROM {table_name} LIMIT 100"
            cur.execute(query)
            data = cur.fetchall()
        return data

    def generate_data_for_tables(self, selected_tables, schemas, no_of_records):
        data = {}
        for table in selected_tables:
            sample_data = self.understand_data(table)
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
            for rel in self.relationships:
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
