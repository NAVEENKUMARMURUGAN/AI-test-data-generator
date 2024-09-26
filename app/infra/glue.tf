resource "aws_glue_catalog_database" "my_database" {
  name = "my_database"
}

resource "aws_glue_catalog_table" "employees" {
  database_name = aws_glue_catalog_database.my_database.name
  name          = "employees"

  table_type = "EXTERNAL_TABLE"
  parameters = {
    "classification" = "csv"  
  }

  storage_descriptor {
    location      = "s3://test-data-gen-sample/employees/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed    = false
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      parameters = {
        "field.delim" = ","
        "skip.header.line.count" = "1"
      }
    }

    columns {
      name = "employee_id"
      type = "int"
    }

    columns {
      name = "full_name"
      type = "string"
    }

    columns {
      name = "email"
      type = "string"
    }

    columns {
      name = "hire_date"
      type = "date"
    }

    columns {
      name = "salary"
      type = "int"
    }
  }
}

resource "aws_glue_catalog_table" "customers" {
  database_name = aws_glue_catalog_database.my_database.name
  name          = "customers"

  table_type = "EXTERNAL_TABLE"
  parameters = {
    "classification" = "csv"  
  }

  storage_descriptor {
    location      = "s3://test-data-gen-sample/customers/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed    = false
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      parameters = {
        "field.delim" = ","
        "skip.header.line.count" = "1"
      }
    }

    columns {
      name = "customer_id"
      type = "int"
    }

    columns {
      name = "first_name"
      type = "string"
    }

    columns {
      name = "last_name"
      type = "string"
    }

    columns {
      name = "email"
      type = "string"
    }

    columns {
      name = "signup_date"
      type = "date"
    }
  }
}
