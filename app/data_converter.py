from io import StringIO, BytesIO
import csv
import json
import pandas as pd
import streamlit as st

class DataConverter:
    def convert_data_to_format(self, data, format, table_name):
        if not data:
            raise ValueError("No data provided for conversion")
        
        # Read data from StringIO
        reader = csv.DictReader(StringIO(data))
        records = list(reader)

        if not records:
            raise ValueError("No records found in the provided data")

        # Ensure fieldnames are not None
        if reader.fieldnames is None or any(field is None for field in reader.fieldnames):
            raise ValueError("Fieldnames cannot be None")

        # Clean records
        cleaned_records = []
        for record in records:
            # Filter out None values and ensure all keys are present
            cleaned_record = {key: record.get(key, None) for key in reader.fieldnames}
            if any(value is not None for value in cleaned_record.values()):  # Only keep if at least one value is present
                cleaned_records.append(cleaned_record)

        if format == 'CSV':
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(cleaned_records)  # Write cleaned records
            return output.getvalue().encode('utf-8'), f"{table_name}.csv"
        
        elif format == 'JSON':
            return json.dumps(cleaned_records, indent=2).encode('utf-8'), f"{table_name}.json"
        
        elif format == 'EXCEL':
            output = BytesIO()
            df = pd.DataFrame(cleaned_records)
            try:
                df.to_excel(output, index=False, engine='openpyxl')
            except Exception as e:
                raise ValueError(f"Failed to convert data to Excel format: {e}")
            return output.getvalue(), f"{table_name}.xlsx"
        
        elif format == 'PARQUET':
            output = BytesIO()
            df = pd.DataFrame(cleaned_records)
            try:
                df.to_parquet(output, index=False)
            except Exception as e:
                raise ValueError(f"Failed to convert data to Parquet format: {e}")
            return output.getvalue(), f"{table_name}.parquet"
        
        else:
            raise ValueError(f"Unsupported format: {format}")