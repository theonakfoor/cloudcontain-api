"""
Utility functions for container and file management and interaction with AWS services.
"""

import boto3
from bson import ObjectId
from flask import current_app as app

from cloudcontain_api.utils.constants import S3_BUCKET_NAME


def get_path(folder, container, include_all=True):
    path = []
    if folder != "~":
        cur_folder = container["folders"][folder]
        if not cur_folder:
            return -1
        else:
            while True:
                if include_all:
                    path.insert(0,
                        {
                            "folderId": cur_folder["folderId"], 
                            "name": cur_folder["name"]
                        },
                    )
                else:
                    path.insert(0, cur_folder["name"])
                if cur_folder["parent"] == "~":
                    break
                cur_folder = container["folders"][cur_folder["parent"]]
    return path


def get_key_string(container_id, path, name=None):
    return f"{container_id}/project/{'/'.join(path)}{'/' if len(path) > 0 else ''}{name if name else ''}"


def get_folder_id(folderId):
    return folderId if folderId == "~" else ObjectId(folderId)


def rename_s3_object(old_key, new_key):
    bucket = app.s3.Bucket(S3_BUCKET_NAME)
    source_object = bucket.Object(old_key)
    bucket.copy({
        "Bucket": S3_BUCKET_NAME, "Key": old_key
    }, new_key)
    source_object.delete()

def stream_s3_object(key):
    s3_object = app.s3.Object(S3_BUCKET_NAME, key)
    with s3_object.get()["Body"] as body:
        for chunk in iter(lambda: body.read(4096), b""):
            yield chunk


def get_all_keys(folder_id, folders, files, seen_folders=None, seen_files=None):
    if seen_folders is None:
        seen_folders = set()  
    if seen_files is None:
        seen_files = set()  

    if folder_id in seen_folders:
        return [], []
    seen_folders.add(folder_id)

    current_folder = list(filter(lambda x: x["folderId"] == folder_id, folders))
    current_files = list(filter(lambda x: x["folder"] == folder_id and x["fileId"] not in seen_files, files))

    for file in current_files:
        seen_files.add(file["fileId"])

    child_folders = list(filter(lambda x: x["parent"] == folder_id, folders))
    for child in child_folders:
        subfolders, subfiles = get_all_keys(child["folderId"], folders, files, seen_folders, seen_files)
        current_folder.extend(subfolders)
        current_files.extend(subfiles)

    return current_folder, current_files


def get_container_contents(containerId, files_col, folders_col):
    all_files = files_col.find({
        "containerId": ObjectId(containerId)
    })
    all_files = [
        {
            "fileId": str(file["_id"]),
            "containerId": str(file["containerId"]),
            "createdBy": file["createdBy"],
            "folder": str(file["folder"]),
            "key": file["key"],
            "name": file["name"],
            "created": str(file["created"]),
            "lastModified": str(file["lastModified"]),
        }
        for file in all_files
    ]

    all_folders = folders_col.find({
        "containerId": ObjectId(containerId)
    })
    all_folders = [
        {
            "folderId": str(directory["_id"]),
            "containerId": str(directory["containerId"]),
            "parent": str(directory["parent"]),
            "name": directory["name"],
            "created": str(directory["created"]),
            "lastModified": str(directory["lastModified"]),
        }
        for directory in all_folders
    ]

    return all_files, all_folders