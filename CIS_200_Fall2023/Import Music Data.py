#!/usr/bin/env python
# coding: utf-8

# ## Import Music Data
# 
# 
# 

# # Import Music Data
# 
# Sources: 
# - [Multiple CSV files to build a relational database](https://www.kaggle.com/datasets/mcfurland/10-m-beatport-tracks-spotify-audio-features/data)
# - [Single CSV file (music.csv) for simplified analysis](https://github.com/likeawednesday/TechCamp_DataViz/blob/main/data/music.csv)

# In[ ]:


import os
import zipfile
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, StringType, BooleanType, DateType

# Specify zip file name
zip_file = 'multi_table.zip'

# Specify schemas to import
import_schemas = (

    # beatport
    'bp'

    # spotify
    , 'sp'
)

# Specify base path
base_path = os.path.join('/', 'lakehouse', 'default')

# Specify spark path
spark_path = 'Files'

# Get folder name from zip file
dataset_folder = zip_file.rstrip('.zip')

# Specify folder path
folder_path = os.path.join(base_path, spark_path, dataset_folder)

# Specify zip file path
zip_file_path = os.path.join(base_path, spark_path, dataset_folder + '.zip')

# Get list of schemas from folder_path
schemas = [s for s in os.listdir(folder_path) if s.startswith(import_schemas)]


# In[ ]:


# Extract zip file
with zipfile.ZipFile(zip_file_path, 'r') as archive:
  
  # Loop through all files in archive
  for file in archive.namelist():

    # Get schema from characters before the first underscore
    schema = file.split('_', 1)[0]

    # Set destination for extracted files
    destination = os.path.join(folder_path, schema)

    # Only extract files matching import_schemas
    if file.startswith(import_schemas):

      # Create a new directory for the destination if it doesn't exist
      if not os.path.exists(destination):
        os.makedirs(destination)

      # Extract file into the destination directory
      print(f"Extracting: {file}")
      archive.extract(file, destination)

archive.close()


# In[ ]:


# Loop through schemas and import their tables
for schema in schemas:

    # Set datetime mode by schema to avoid import issues with very old tracks from spotify
    datetime_mode = 'LEGACY' if schema == 'sp' else 'CORRECTED'
    spark.conf.set('spark.sql.parquet.datetimeRebaseModeInWrite', datetime_mode)

    # Construct schema_path from folder_path and schema
    schema_path = os.path.join(folder_path, schema)

    # Get list of CSV files starting with 'bp_' (beatport) in folder_path
    files = [f for f in os.listdir(schema_path) if f.endswith('.csv')]

    # Loop through each CSV file
    for file in files:
        
        # Get table name from CSV file, adding extra underscore to schema prefix
        prefix = schema + '_'
        table_name = file.rstrip('.csv').replace(prefix, prefix + '_')

        # Construct spark_schema_path
        spark_schema_path = os.path.join(spark_path, dataset_folder, schema, file)
        
        # Read CSV file into DataFrame
        df = spark.read.format('csv') \
            .option('header', 'true') \
            .option('mode', 'DROPMALFORMED') \
            .option('inferschema', 'true') \
            .option('dateFormat', 'yyyy-mm-dd') \
            .load(spark_schema_path)

        # Use column names as clues to set their datatypes
        for column in df.columns:

            # ID columns
            id_names = ('_id')
            if column.endswith(id_names) and schema == 'bp':
                df = df.withColumn(column, col(column).cast(IntegerType()))

            # Integer columns
            integer_names = ('_count', '_num', '_number', '_ms', 'popularity', '_tracks', 'bpm')
            if column.endswith(integer_names):
                df = df.withColumn(column, col(column).cast(IntegerType()))
            
            # Boolean columns
            boolean_names = ('explicit', 'is_')
            if column.startswith(boolean_names):
                df = df.withColumn(column, when(col(column) == 't', True).otherwise(False))
                df = df.withColumn(column, col(column).cast(BooleanType()))

            # String columns
            string_names = ('_name', '_letter', '_url', '_uuid')
            if column.endswith(string_names):
                df = df.withColumn(column, col(column).cast(StringType()))

            # Date columns
            date_names = ('_on', '_date')
            if column.endswith(date_names):
                df = df.withColumn(column, col(column).cast(DateType()))

        # Write DataFrame into Delta table
        print(f"Importing table: {table_name}")
        df.write.format('delta') \
            .mode('overwrite') \
            .saveAsTable(table_name)


# In[ ]:


# To delete all CSV files, drop all tables, and start over...
# Un-freeze and run this cell

# Delete the folder containing the CSV files
# print(f"Deleting folder: {folder_path}")
os.rmdir(folder_path)

# Get name of current database
database_name = spark.catalog.currentDatabase()

# Get a list of all tables
tables = spark.catalog.listTables()

# Drop all delta tables
for table in tables:
    print(f"Dropping table: {table.name}")
    spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table.name}")

