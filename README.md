# Multi-Table Test Data Generator

> Generate realistic test data for multiple database tables while maintaining referential integrity.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Customization](#customization)
- [Contributing](#contributing)
- [License](#license)

## Introduction

The Multi-Table Test Data Generator is a Streamlit-based application that allows you to generate realistic test data for multiple database tables, while ensuring referential integrity between related tables. This tool is particularly useful for software developers, data engineers, and anyone who needs to create test data for their applications.

By leveraging the power of the OpenAI API, this application can generate sample data that closely mimics the patterns and characteristics of your actual production data, making it a valuable asset for testing and development.

## Features

- **Multi-Table Selection**: Choose the tables you want to generate data for, without the need to worry about creating data for each table individually.
- **Referential Integrity**: The generated data respects the relationships between tables, ensuring that foreign key constraints are met.
- **Multiple Export Formats**: Download the generated data in various formats, including CSV, JSON, Excel, and Parquet.
- **User-Friendly Interface**: The Streamlit-based UI provides an intuitive and easy-to-use experience for generating and exporting test data.
- **Customizable Prompt**: The application allows you to customize the prompt sent to the OpenAI API, giving you control over the generated data.
- **Modular and Extensible Design**: The codebase follows OOP principles and the SOLID design pattern, making it easy to maintain, extend, and contribute to the project.

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/multi-table-data-generator.git
   ```

2. Navigate to the project directory:

   ```bash
   cd multi-table-data-generator
   ```

3. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Set your OpenAI API key as an environment variable:

   ```bash
   export OPENAI_API_KEY=your_api_key
   ```

   Alternatively, you can update the `app.py` file and hardcode the API key.

5. Run the Streamlit application:

   ```bash
   streamlit run app.py
   ```

The application should now be accessible at `http://localhost:8501`.

## Usage

1. Enter your PostgreSQL database connection details in the Streamlit UI.
2. Click the "Connect to Database" button to establish the connection.
3. Select the tables you want to generate data for using the multiselect dropdown.
4. Choose the desired export format (CSV, JSON, Excel, or Parquet) from the dropdown.
5. Click the "Generate Data" button to create the test data.
6. Once the data is generated, use the download button to save the files.

## Customization

To customize the generated data or the application's behavior, you can modify the following components:

1. **Data Generation**: Update the `generate_data_for_tables()` method in the `DataGenerator` class to modify the prompts sent to the OpenAI API or the data generation logic.
2. **Database Connectivity**: Modify the `DBConnection` class to support different database engines or customize the connection parameters.
3. **Table Schema Retrieval**: Enhance the `TableSchema` class to handle more complex schema structures or additional metadata.
4. **Data Conversion**: Extend the `DataConverter` class to support new export formats or customize the conversion logic.
5. **Streamlit UI**: Modify the `app.py` file to change the layout, add new features, or integrate the application into a larger ecosystem.

## Contributing

We welcome contributions to this project! If you encounter any issues, have suggestions for improvements, or would like to add new features, please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Implement your changes and ensure they follow the project's coding style and conventions.
4. Write tests (if applicable) to ensure the reliability and correctness of your changes.
5. Submit a pull request, describing the changes you've made and the problem they solve.

## License

This project is licensed under the [MIT License](LICENSE).