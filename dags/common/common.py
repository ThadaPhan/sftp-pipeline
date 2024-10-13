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
