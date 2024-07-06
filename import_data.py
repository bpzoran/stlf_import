import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import config


def import_ts_data():
    """
    The import_ts_data function reads time series data from CSV files in a specified directory
    structure, processes the data, and imports it into a SQL database table.
    It ensures the table is created if it doesn't exist and handles specific data
    formatting requirements.

    Flow
    Establish a connection to the SQL database using the connection string from config.
    Iterate through folders and CSV files in the specified directory.
    Process each CSV file to extract and format the required data.
    Create the SQL table if it doesn't exist.
    Insert the processed data into the SQL table.

    """
    connection_string = f'mssql+pyodbc://@{config.server}/{config.database}?driver={config.driver}'
    engine = create_engine(connection_string)
    # Create table if not created
    # Generate SQL to create table
    create_table_sql = f"""
                        CREATE TABLE {config.table_name_power_data} (
                            {config.timestamp_field} DATETIME,
                            {config.ts_field} VARCHAR(MAX),
                            {config.file_name_field} VARCHAR(MAX),
                            {config.active_power_column_name_template}_1 VARCHAR(MAX),
                            {config.active_power_column_name_template}_2 VARCHAR(MAX),
                            {config.active_power_column_name_template}_3 VARCHAR(MAX),
                            {config.active_power_column_name_template}_4 VARCHAR(MAX),
                            {config.active_power_column_name_template}_5 VARCHAR(MAX),
                            {config.active_power_column_name_template}_6 VARCHAR(MAX),
                            {config.active_power_column_name_template}_7 VARCHAR(MAX),
                            {config.active_power_column_name_template}_8 VARCHAR(MAX),
                            {config.active_power_column_name_template}_9 VARCHAR(MAX),
                            {config.active_power_column_name_template}_10 VARCHAR(MAX)
                        );
                        """

    with engine.connect() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {config.table_name_power_data}"))
        connection.execute(text(create_table_sql))
    for ts_folder in os.listdir(config.main_folder):
        print(ts_folder)
        ts_folder_path = os.path.join(config.main_folder, ts_folder)
        trafo_polja_folder_path = os.path.join(ts_folder_path, config.transformer_fields_str)

        if os.path.exists(trafo_polja_folder_path):
            for csv_file in os.listdir(trafo_polja_folder_path):
                if csv_file.endswith('.csv'):
                    csv_file_path = os.path.join(trafo_polja_folder_path, csv_file)
                    data = pd.read_csv(csv_file_path, skiprows=0)

                    aktivna_snaga_columns = [col for col in data.columns if config.active_power_str in col]

                    if len(aktivna_snaga_columns) > 10:
                        print(
                            f"Warning: More than 10 '{config.active_power_str}' columns found in {csv_file_path}. Only the first 10 will be used.")

                    aktivna_snaga_columns = aktivna_snaga_columns[:10]

                    processed_data = []

                    current_date = None
                    for index, row in data.iterrows():
                        if pd.isna(row.iloc[0]) or row.iloc[0] == config.max_str:
                            continue

                        if len(row.iloc[0].split(':')) == 1:
                            current_date = datetime.strptime(row.iloc[0], "%d.%m.%Y").date()
                            continue

                        if current_date is not None:
                            time = row.iloc[0]
                            if time == '24:00':
                                time = '23:00'
                                datetime_obj = datetime.combine(current_date, datetime.min.time()) + timedelta(
                                    days=1) - timedelta(hours=1)
                            else:
                                datetime_obj = datetime.strptime(f"{current_date} {time}",
                                                                 "%Y-%m-%d %H:%M") - timedelta(
                                    hours=1)

                            row_data = [datetime_obj, ts_folder, csv_file]
                            for col in aktivna_snaga_columns:
                                row_data.append(row[col] if col in row else None)

                            row_data.extend([None] * (10 - len(aktivna_snaga_columns)))
                            processed_data.append(row_data)

                    columns = [config.timestamp_field, config.ts_field, config.file_name_field,
                               f'{config.active_power_column_name_template}_1',
                               f'{config.active_power_column_name_template}_2',
                               f'{config.active_power_column_name_template}_3',
                               f'{config.active_power_column_name_template}_4',
                               f'{config.active_power_column_name_template}_5',
                               f'{config.active_power_column_name_template}_6',
                               f'{config.active_power_column_name_template}_7',
                               f'{config.active_power_column_name_template}_8',
                               f'{config.active_power_column_name_template}_9',
                               f'{config.active_power_column_name_template}_10']
                    processed_df = pd.DataFrame(processed_data, columns=columns)

                    processed_df.to_sql(config.table_name_power_data, engine, if_exists='append', index=False)

    print("Data imported successfully.")


if __name__ == "__main__":
    import_ts_data()
