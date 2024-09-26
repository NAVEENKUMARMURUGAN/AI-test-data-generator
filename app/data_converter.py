from io import StringIO, BytesIO
import csv
import json
import pandas as pd
import streamlit as st

class DataConverter:
    def convert_data_to_format(self, data, format, table_name):

        if not data:
            raise ValueError("No data provided for conversion")
        
        reader = csv.DictReader(StringIO(data))
        records = list(reader)

        if not records:
            raise ValueError("No records found in the provided data")
        
        if format == 'CSV':
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(records)
            return output.getvalue().encode('utf-8'), f"{table_name}.csv"
        
        elif format == 'JSON':
            return json.dumps(records, indent=2).encode('utf-8'), f"{table_name}.json"
        
        elif format == 'EXCEL':
            output = BytesIO()
            df = pd.DataFrame(records)
            try:
                df.to_excel(output, index=False, engine='openpyxl')
            except Exception as e:
                raise ValueError(f"Failed to convert data to Excel format: {e}")
            return output.getvalue(), f"{table_name}.xlsx"
        
        elif format == 'PARQUET':
            output = BytesIO()
            df = pd.DataFrame(records)
            try:
                df.to_parquet(output, index=False)
            except Exception as e:
                raise ValueError(f"Failed to convert data to Parquet format: {e}")
            return output.getvalue(), f"{table_name}.parquet"
        
        else:
            raise ValueError(f"Unsupported format: {format}")