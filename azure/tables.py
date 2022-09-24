

from config import *
from azure.data.tables import TableServiceClient, TableClient
from azure.core.exceptions import ResourceExistsError
import pandas as pd




def create_table(tablename):
    """this will help you make a new table in azure"""
    with TableServiceClient.from_connection_string(conn_str=connection_string) as table_service:
        
        try:
            table_service.create_table(
                table_name=tablename)
        except:
            print("Table already exists")



def insert_entity(tablename, location, PartitionKey=None, RowKey=None):
    """ upload data from a csv to an azure table, you can specify a partitionkey and rowkey if you'd like """
    with TableClient.from_connection_string(conn_str=connection_string, table_name=tablename) as table_client:

        try:
            table = pd.read_csv(location)
            df = pd.DataFrame(table, columns=table.columns)
            
            if PartitionKey:
                print(PartitionKey)
                df.rename(
                    columns = {f'{PartitionKey}': 'PartitionKey'}, inplace=True)
            else:
                df.insert(0, "PartitionKey", [
                    f'P{i}' for i in range(len(df.index))])
            if RowKey:
                df.rename(columns = {f'{RowKey}': 'RowKey'}, inplace=True)
            else:
                df.insert(1, "RowKey", [
                    f'R{i+1}' for i in range(len(df.values))])

            task2 = dict.fromkeys([name for name in df.columns])

            for i in range(0, len(df.index)):
                # print(i)
                for key in task2:
                    task2[key] = str(df[key].iloc[i])
                table_client.create_entity(task2)
            return True
        except ResourceExistsError:
            print("Entity already exists")




def lists_tables():
    """ prints the names of all the tables that exist """
    with TableServiceClient.from_connection_string(conn_str= connection_string) as table_service:

        try:
            # List all the tables in the service
            list_tables = table_service.list_tables()
            print("Listing tables:")
            for table in list_tables:
                print("\t{}".format(table.name))

        except:
            print("Beep Boop. This wont happen.")



def list_table_keys(tablename):
    """ prints all the column names in a specified table"""
    with TableClient.from_connection_string(connection_string, tablename) as table_client:

        try:
            tasks = table_client.query_entities(query_filter=None)

            for key in tasks.next():
                print(key)
        except:
            print("Something went wrong. Either a spelling error or the table is empty")



def query_entities(tablename, key, value, save=None, print=False):
    """allows you to query a specific key and value in a table, saving and printing optional"""
    from azure.data.tables import TableClient
    from azure.core.exceptions import HttpResponseError

    with TableClient.from_connection_string(connection_string, tablename) as table_client:
        try:
            parameters = {key: value}
            name_filter = f"{key} eq @{key}"
            queried_entities = table_client.query_entities(
                query_filter=name_filter, parameters=parameters)
            answers = []
            for entity_chosen in queried_entities:
                answers.append(entity_chosen)
            if print:
                print(answers)
            if save:
                df = pd.DataFrame.from_dict(answers)
                df.to_csv(f'{tablename}.csv')
        except HttpResponseError as e:
            key
            print(e.message)


