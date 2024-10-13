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
