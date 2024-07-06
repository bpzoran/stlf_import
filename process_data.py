import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text

import config

# Create a connection string for SQLAlchemy
connection_string = f"mssql+pyodbc://{config.server}/{config.database}?driver={config.driver}"

# Create an SQLAlchemy engine
engine = create_engine(connection_string)

# Establish connection to the database
conn = pyodbc.connect(
    f'DRIVER={config.driver};SERVER={config.server};DATABASE={config.database};Trusted_Connection=yes')
cursor = conn.cursor()

# Drop and create tables
cursor.execute(
    f"IF OBJECT_ID('{config.table_name_power_data_sumarized}', 'U') IS NOT NULL DROP TABLE {config.table_name_power_data_sumarized}")
cursor.execute(
    f"CREATE TABLE {config.table_name_power_data_sumarized} ([POWER_DATA_ID] INT IDENTITY(1,1) NOT NULL, [TIMEST] DATETIME NULL, [AKTIVNA_SNAGA] FLOAT NOT NULL)")
cursor.execute(
    f"IF OBJECT_ID('{config.table_name_power_data_skipped}', 'U') IS NOT NULL DROP TABLE {config.table_name_power_data_skipped}")
cursor.execute(f"CREATE TABLE {config.table_name_power_data_skipped} ([TIMEST] DATETIME NULL, [TS] VARCHAR(MAX) NULL)")
cursor.execute(
    f"IF OBJECT_ID('{config.table_name_power_data_simulated}', 'U') IS NOT NULL DROP TABLE {config.table_name_power_data_simulated}")
cursor.execute(f"CREATE TABLE {config.table_name_power_data_simulated} ([TIMEST] DATETIME NULL, [TS] VARCHAR(MAX) NULL, [FILE_NAME] VARCHAR(MAX) NULL, \
                [AKTIVNA_SNAGA_1] VARCHAR(MAX) NULL, [AKTIVNA_SNAGA_2] VARCHAR(MAX) NULL, [AKTIVNA_SNAGA_3] VARCHAR(MAX) NULL, [AKTIVNA_SNAGA_4] VARCHAR(MAX) NULL, \
                [AKTIVNA_SNAGA_5] VARCHAR(MAX) NULL, [AKTIVNA_SNAGA_6] VARCHAR(MAX) NULL, [AKTIVNA_SNAGA_7] VARCHAR(MAX) NULL, [AKTIVNA_SNAGA_8] VARCHAR(MAX) NULL, \
                [AKTIVNA_SNAGA_9] VARCHAR(MAX) NULL, [AKTIVNA_SNAGA_10] VARCHAR(MAX) NULL, [TIMEST_COPIED_FROM] DATETIME NULL)")

# Fetch data from PowerData table using SQLAlchemy engine
query = f"SELECT * FROM {config.table_name_power_data} order by TIMEST"
df = pd.read_sql(query, engine)

# Process data
unique_timestamps = list(df['TIMEST'].unique())
unique_timestamps.sort()
summarized_data = []
skipped_rows = []
simulated_data = []
j = 0
for timestamp in unique_timestamps:
    if j % 240 == 0:
        print(f"processing timestamp {timestamp}")
    j += 1
    timestamp_data = df[df['TIMEST'] == timestamp]
    substations_missing_all = 0
    total_power = 0
    substations_with_data = 0

    for _, row in timestamp_data.iterrows():
        substations_data_valid = False
        substation_power = 0

        for i in range(1, 11):
            power_value = row[f'AKTIVNA_SNAGA_{i}']
            try:
                power_value = float(power_value) if power_value is not None else 0
                substation_power += power_value
                substations_data_valid = True
            except ValueError:
                power_value = 0

        if substations_data_valid:
            total_power += substation_power
            substations_with_data += 1
        else:
            skipped_rows.append((timestamp, row['TS']))
            substations_missing_all += 1

    if substations_missing_all <= 3:
        summarized_data.append((timestamp, total_power))
    else:
        skipped_rows.extend([(timestamp, row['TS']) for _, row in timestamp_data.iterrows()])

# Insert summarized data into PowerDataSumarized table
print("Insert summarized data into PowerDataSumarized table")
j = 0
for timest, akt_snaga in summarized_data:
    if j % 240 == 0:
        print(f"summarized rows - insert timestamp {timest}")
    j += 1
    cursor.execute(f"INSERT INTO {config.table_name_power_data_sumarized} (TIMEST, AKTIVNA_SNAGA) VALUES (?, ?)",
                   timest, akt_snaga)

# Insert skipped rows into PowerDataSkipped table
print("Insert skipped rows into PowerDataSkipped table")
j = 0
for timest, ts in skipped_rows:
    if j % 240 == 0:
        print(f"skipped rows - insert timestamp {timest}")
    j += 1
    cursor.execute(f"INSERT INTO {config.table_name_power_data_skipped} (TIMEST, TS) VALUES (?, ?)", timest, ts)

# Commit transactions and close the connection
conn.commit()
cursor.close()
conn.close()
print("Process data finished")
