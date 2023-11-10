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

def create_file_system(service_client: DataLakeServiceClient, file_system_name: str) -> FileSystemClient:
    file_system_client = service_client.create_file_system(file_system=file_system_name)

    return file_system_client

file_system = create_file_system(service_client, "filesystem8")

def create_directory(file_system_client: FileSystemClient, directory_name: str) -> DataLakeDirectoryClient:
    directory_client = file_system_client.create_directory(directory_name)

    return directory_client

directory_client = create_directory(file_system, "Data")

def upload_file_to_directory(directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
    file_client = directory_client.get_file_client(file_name)

    with open(file=os.path.join(local_path, file_name), mode="rb") as data:
        file_client.upload_data(data, overwrite=True)


for path, subdirs, files in os.walk(r".\data"):
    for name in files:
        with open(os.path.join(path, name), "rb") as data:
            upload_file_to_directory(directory_client, path, name)
            #print(f"Uploaded {os.path.join(path, name)} to {directory_client.getDirectoryName()}")


