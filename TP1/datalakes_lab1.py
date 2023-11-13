import os
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
load_dotenv()

def get_service_client_account_key(account_name, account_key) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.core.windows.net"
    service_client = DataLakeServiceClient(account_url, credential=account_key)

    return service_client

account_name = os.environ["STORAGE_ACCOUNT_NAME"]
account_key = os.environ["STORAGE_ACCOUNT_KEY"]

service_client = get_service_client_account_key(account_name, account_key)

file_system = service_client.create_file_system(file_system="filesystem9")

directory_client = file_system.create_directory("Data")

for path, subdirs, files in os.walk(r".\TP1\data"):
    for name in files:
        with open(os.path.join(path, name), "rb") as data:
            FILENAME = os.path.join(path, name)
            file_client = file_system.get_file_client(FILENAME)
            file_client.upload_data(data, overwrite=True)
            print(f"Uploaded {FILENAME}")
