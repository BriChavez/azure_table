from tables import *
import typer

table_app = typer.Typer()


@table_app.command("new_table")
def new_table(tablename: str):
    create_table(tablename)


@table_app.command("load_table")
def load_table(tablename: str, location: str):
    insert_entity(tablename, location, PartitionKey=None, RowKey=None)


@table_app.command("list_tables")
def list_tables():
    lists_tables()


@table_app.command("list_keys")
def list_keys(tablename):
    list_table_keys(tablename)


@table_app.command("query_key")
def query_key(tablename: str, key: str, value):
    query_entities(tablename, key, value, save=None)


if __name__ == "__main__":
    table_app()
