#!/usr/bin/python3

# Global
import os
from abc import (
    ABC, 
    abstractmethod
)

# External
import polars as pl
from airflow.providers.sftp.hooks.sftp import SFTPHook


class StorageOperator(ABC):

    @abstractmethod
    def describe_path(self, path: str) -> dict:
        """
            Retrieve metadata details about the given directory path.

            Args:
                path (str): The path to the directory in the storage system.

            Returns:
                dict: A dictionary containing details {
                    "modify": "20241013173425",
                    "name": "/upload",
                    "size": 512,
                    "type": "dir",
                }.
        """
        pass

    @abstractmethod
    def create_directory(self, path: str) -> None:
        """
            Create directory with given directory path.

            Args:
                path (str): The path to the directory in the storage system.
        """
        pass

    @abstractmethod
    def upload_file(self, remote_path: str, local_path: str) -> None:
        """
            Upload file with given file path.

            Args:
                remote_path (str): The path to the directory in the storage system.
                local_path (str): The path to the directory in local storage.
        """
        pass

    @abstractmethod
    def download_file(self, remote_path: str, local_path: str) -> None:
        """
            Download file with given file path.

            Args:
                remote_path (str): The path to the directory in the storage system.
                local_path (str): The path to the directory in local storage.
        """
        pass
        
    @staticmethod
    def _detect_change(self, source_describe: dict, destination_describe: dict, type: str) -> dict:
        """
            The static method help detect change file or directory

            Args:
                source_describe (dict): The metadata details on source (return of self.describe_path).
                destination_describe (dict): The metadata details on source (return of self.describe_path).
                type (str): dir or file.
        """
        pass

    @abstractmethod
    def close(self):
        """
            The close method help close hook connection
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class SFTPStorageOperator(StorageOperator):

    def __init__(self, conn_id: str):
        self.connection = SFTPHook(ssh_conn_id=conn_id)

    def describe_path(self, path: str) -> list[dict]:

        result = []

        _, directories, _ = self.connection.get_tree_map(path)

        directories = [path] + directories

        for directory in directories:
            for path, metadata_path in self.connection.describe_directory(directory).items():
                result.append(metadata_path | {'name': os.path.join(directory, path)})

        return result

    def create_directory(self, path):
        self.connection.create_directory(path)

    def upload_file(self, remote_path: str, local_path: str):
        return self.connection.store_file(
            remote_full_path=remote_path,
            local_full_path=local_path            
        )

    def download_file(self, remote_path: str, local_path: str):
        return self.connection.retrieve_file(
            remote_full_path=remote_path,
            local_full_path=local_path            
        )

    def _detect_change(
            self,
            source_describe: list[dict],
            destination_describe: list[dict],
            type: str = 'dir',
        ) -> list:

        source = pl.DataFrame(source_describe)
        dest = pl.DataFrame(destination_describe)

        df = source.join(
            dest,
            on=['name', 'type'],
            how='left',
            suffix='_dest'
        ).filter(pl.col('type') == type)

        changes = df.filter(
            (pl.col('size_dest').is_null())
            | (pl.col('size') != pl.col('size_dest'))
        )

        return changes.select(pl.col('name')).to_series(0).to_list()

    def detect_change_path(
            self,
            source_describe: list[dict],
            destination_describe: list[dict],
        ) -> list:

        _TYPE = 'file'
        return self._detect_change(source_describe, destination_describe, _TYPE)

    def detect_change_file(
            self,
            source_describe: list[dict],
            destination_describe: list[dict],
        ) -> list:

        _TYPE = 'dir'
        return self._detect_change(source_describe, destination_describe, _TYPE)

    def close(self):
        if self.connection:
            self.connection.close_conn()
            print("SFTP hook closed.")



#!/usr/bin/python3

# Global
import os
import tempfile
from datetime import (
    datetime, 
    timedelta
)

# Internal
from common.common import SFTPStorageOperator

# External
from airflow.models.param import Param
from airflow.decorators import (
    dag, 
    task
)


@task()
def describe_path_on_source(
    params: dict,
) -> list[dict]:
    
    result = []

    source_conn_id = params.get('source_conn_id')
    path = params.get('path')

    with SFTPStorageOperator(conn_id=source_conn_id) as storage_client:
        try:
            result = storage_client.describe_path(path)
        except Exception as e:
            print(e)
    
    return result


@task()
def describe_path_on_destination(
    params: dict,
) -> list[dict]:
    
    result = []

    destination_conn_id = params.get('destination_conn_id')
    path = params.get('path')

    with SFTPStorageOperator(conn_id=destination_conn_id) as storage_client:
        try:
            result = storage_client.describe_path(path)
        except Exception as e:
            print(e)
    
    return result


@task(multiple_outputs=True)
def get_change_path(
    source_describe: list[dict],
    dest_describe: list[dict],
):

    result = {
        'change_directories': None,
        'change_files': None
    }

    try:
        change_directories = SFTPStorageOperator.detect_change_path(source_describe, dest_describe)
        change_files = SFTPStorageOperator.detect_change_file(source_describe, dest_describe)
    except Exception as e:
        print(e)
        change_directories = []
        change_files = []
    finally:
        result['change_directories'] = change_directories
        result['change_files'] = change_files

    return result


@task()
def sync_directory(
    change_path: dict[str:list[str]], 
    params: dict,
) -> dict[str:list[str]]:

    result = {
        'success': None,
        'failure': None
    }

    destination_conn_id = params.get('destination_conn_id')

    success_path = []
    failure_path = []

    directories = change_path.get('change_directories')
    
    with SFTPStorageOperator(conn_id=destination_conn_id) as storage_client:
        
        for directory in directories:
            try:
                storage_client.create_directory(directory)
            except Exception as e:
                print(e)
                failure_path.append(directory)
            else:
                success_path.append(directory)
    
    result['success'] = success_path
    result['failure'] = failure_path

    return result


@task()
def sync_file(
    change_path, 
    params: dict,
):

    result = {
        'success': None,
        'failure': None
    }

    source_conn_id = params.get('source_conn_id')
    destination_conn_id = params.get('destination_conn_id')

    success_path = []
    failure_path = []

    files = change_path.get('change_files')

    with SFTPStorageOperator(conn_id=source_conn_id) as source_storage_client:

        with SFTPStorageOperator(conn_id=destination_conn_id) as destination_storage_client:

            with tempfile.TemporaryDirectory() as tmpdirname:

                for file in files:

                    try:

                        file_name = file.split("/")[-1]
                        tmp_path = os.path.join(tmpdirname, file_name)

                        source_storage_client.retrieve_file(
                            remote_full_path=file,
                            local_full_path=tmp_path
                        )

                        destination_storage_client.store_file(
                            remote_full_path=file,
                            local_full_path=tmp_path                        
                        )

                    except Exception as e:
                        print(e)
                        failure_path.append(file)
                    else:
                        success_path.append(file)

    result['success'] = success_path
    result['failure'] = failure_path

    return result


@dag(
    schedule='@daily',
    dag_id='syncs',
    start_date=datetime(2024, 10, 12),
    description='Sync files from source SFTP to target SFTP',
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        'source_conn_id': Param("sftp1", type="string"),
        'destination_conn_id': Param("sftp2", type="string"),
        'path': Param("/", type="string"),
    }
)
def sync():

    # source_conn_id = "sftp1"
    # destination_conn_id = "sftp2"

    source_describe = describe_path_on_source()
    dest_describe = describe_path_on_destination()

    change_path = get_change_path(
        source_describe,
        dest_describe
    )

    sync_directory(change_path)

    sync_file(change_path)


# if __name__ == "__main__":
sync()