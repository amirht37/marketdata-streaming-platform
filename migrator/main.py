

import os 
from pathlib import Path
import clickhouse_connect


def get_config():
    return {
        "host":os.getenv("CLICKHOUSE_HOST","crypto-clickhouse"),
        "username":os.getenv("CLICKHOUSE_USER","clickhouse_admin"),
        "password":os.getenv("CLICKHOUSE_PASSWORD","clickhouse_secure_pass"),
        "database":os.getenv("CLICKHOUSE_DB","crypto_db")
    }



def get_migrations():
    path=Path("./migrations")
    migrations=sorted(path.glob("*.sql"))

    return migrations




def create_client(config):
    client=clickhouse_connect.get_client(host=config["host"],username=config["username"],password=config["password"],database=config["database"])
    return client


def run_migrations(client, migrations):
    for migration in migrations:
        sql= migration.read_text()
        statements=sql.split(";")
        for statement in statements:
            statement= statement.strip()
            if statement:
                print("Executing statement...")
                client.command(statement)

def main():
    config=get_config()

    migrations=get_migrations()


    client=create_client(config)

    run_migrations(client, migrations)

if __name__ == "__main__":
    main()