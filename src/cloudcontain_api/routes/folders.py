import json
import re
from datetime import datetime, timezone

import boto3
from bson import ObjectId
from flask import Blueprint, jsonify, request
from pymongo import MongoClient

from src.cloudcontain_api.utils.auth import require_auth
from src.cloudcontain_api.utils.constants import (
    MONGO_CONN_STRING,
    MONGO_DB_NAME,
    S3_BUCKET_NAME,
)
from src.cloudcontain_api.utils.utils import (
    get_all_keys,
    get_container_contents,
    get_folder_id,
    get_key_string,
    get_path,
    rename_s3_object,
)

folders_bp = Blueprint("folders", __name__)

db_client = MongoClient(MONGO_CONN_STRING)
db = db_client[MONGO_DB_NAME]

s3 = boto3.resource("s3")


@folders_bp.route("/containers/<container_id>/folders/<folder_id>", methods=["POST"])
@require_auth
def create_folder(container_id, folder_id):
    data = request.get_json()
    containers = db["containers"]
    folders = db["folders"]

    timestamp = datetime.now(timezone.utc)
    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        if get_path(folder_id, container, include_all=False) == -1:
            return jsonify(
                {"message": "Parent folder not found within this container."}
            ), 404
        
        if "name" not in data or not re.match(r"[\w\-.]+$", data["name"]):
            return jsonify(
                {"message": "Please provide a valid folder name."}
            ), 400
        
        name_duplicate_count = folders.count_documents(
            {
                "containerId": ObjectId(container_id),
                "parent": get_folder_id(folder_id),
                "name": re.compile(f"^{data["name"]}$", re.IGNORECASE),
            },
            limit=1,
        )
        if name_duplicate_count != 0:
            return jsonify(
                {"message": "A folder with this name already exists in this location."}
            ), 409
        
        insert_response = folders.insert_one(
            {
                "containerId": ObjectId(container_id),
                "createdBy": request.user["sub"],
                "parent": get_folder_id(folder_id),
                "name": data["name"].strip(),
                "created": timestamp,
                "lastModified": timestamp,
            }
        )

        if insert_response.inserted_id:
            created_folder_id = str(insert_response.inserted_id)
            containers.update_one(
                {"_id": ObjectId(container_id)},
                {
                    "$set": {
                        f"folders.{created_folder_id}": {
                            "folderId": created_folder_id,
                            "parent": folder_id,
                            "name": data["name"].strip(),
                        },
                        "lastModified": timestamp,
                    }
                },
            )
            return jsonify({"folderId": created_folder_id}), 201
        else:
            return jsonify({"message": "Error creating folder."}), 500
    else:
        if containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
            return jsonify({
                "message": "User is not authorized to modify this container."
            }), 401
        else:
            return jsonify({"message": "Container not found."}), 404
        

@folders_bp.route("/containers/<container_id>/folders/<folder_id>", methods=["GET"])
@require_auth
def get_folder(container_id, folder_id):
    containers = db["containers"]
    folders = db["folders"]
    files = db["files"]

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        folder_path = get_path(folder_id, container)
        if folder_path == -1:
            return jsonify(
                {"message": "Folder not found within this container."}
            ), 404
        
        sub_directories_response = folders.find(
            {
                "containerId": ObjectId(container_id),
                "parent": get_folder_id(folder_id),
            }
        )

        sub_directories = [
            {
                "folderId": str(directory["_id"]),
                "containerId": str(directory["containerId"]),
                "parent": str(directory["parent"]),
                "name": directory["name"],
                "created": str(directory["created"]),
                "lastModified": str(directory["lastModified"]),
            }
            for directory in sub_directories_response
        ]

        for dir in sub_directories:
            directory_path = get_path(dir["folderId"], container, include_all=False)
            prefix = get_key_string(container_id, directory_path)
            directory_size = sum(
                obj.size for
                obj in s3.Bucket(S3_BUCKET_NAME).objects.filter(Prefix=prefix)
            )
            dir["size"] = directory_size

        sub_files_response = files.find(
            {
                "containerId": ObjectId(container_id),
                "folder": get_folder_id(folder_id),
            }
        )

        sub_files = [
            {
                "fileId": str(file["_id"]),
                "containerId": str(file["containerId"]),
                "createdBy": file["createdBy"],
                "folder": str(file["folder"]),
                "size": file["size"],
                "key": file["key"],
                "name": file["name"],
                "created": str(file["created"]),
                "lastModified": str(file["lastModified"]),
            }
            for file in sub_files_response
        ]

        total_file_size = sum(file["size"] for file in sub_files)
        total_directory_size = sum(dir["size"] for dir in sub_directories)
        total_size = total_file_size + total_directory_size

        metadata = None
        # Only fetch metadata if not root folder
        if folder_id != "~":
            metadata = folders.find_one(
                {
                    "_id": ObjectId(folder_id),
                    "containerId": ObjectId(container_id),
                }
            )

        return jsonify(
            {
                "folderId": folder_id,
                "parent": str(metadata["parent"]) if metadata else None,
                "path": folder_path,
                "directories": sub_directories,
                "files": sub_files,
                "size": total_size,
                "created": str(metadata["created"]) if metadata else str(container["created"]),
                "lastModified": str(metadata["lastModified"]) if metadata else None,
            }
        ), 200
    else:
        if containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
            return jsonify({
                "message": "User is not authorized to access this container's folders."
            }), 401
        else:
            return jsonify({"message": "Container not found."}), 404
        

@folders_bp.route("/containers/<container_id>/folders/<folder_id>", methods=["PUT"])
@require_auth
def update_folder(container_id, folder_id):

    def get_latest_value(original_obj, updated_obj, key):
        return updated_obj[key] if key in updated_obj else original_obj[key] 

    data = request.get_json()
    containers = db["containers"]
    folders = db["folders"]
    files = db["files"]

    timestamp = datetime.now(timezone.utc)

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        if folder_id == "~":
            return jsonify({"message": "Cannot modify root folder."}), 403
        
        folder = folders.find_one(
            {"_id": ObjectId(folder_id), "containerId": ObjectId(container_id)}
        )
        
        if folder:

            all_files, all_folders = get_container_contents(container_id, files, folders)
            _, file_keys = get_all_keys(folder_id, all_folders, all_files)

            updates = dict()

            if "parent" in data and data["parent"]:
                if get_path(data["parent"], container, include_all=False) == -1:
                    return jsonify({"message": "Parent folder not found within this container."}), 404
                
                name_duplicate_count = folders.count_documents(
                    {
                        "containerId": ObjectId(container_id),
                        "parent": get_folder_id(data["parent"]),
                        "name": re.compile(f"^{get_latest_value(folder, data, "name")}$", re.IGNORECASE),
                    },
                    limit=1,
                )

                if name_duplicate_count != 0:
                    return jsonify({"message": "Folder with this name already exists within parent folder."}), 409
                
                # Update local container folder reference
                container["folders"][folder_id]["parent"] = data["parent"]
                # Update remote container folder reference
                containers.update_one(
                    {"_id": ObjectId(container_id)},
                    {
                        "$set": {
                            f"folders.{folder_id}.parent": data["parent"],
                            "lastModified": timestamp,
                        }
                    },
                )

                updates["parent"] = get_folder_id(data["parent"])
                
            if "name" in data and data["name"]:
                data["name"] = data["name"].strip()

                if not re.match(r"[\w\-.]+$", data["name"]):
                    return jsonify({"message": "Please provide a valid folder name."}), 400
                
                name_duplicate_count = folders.count_documents(
                    {
                        "containerId": ObjectId(container_id),
                        "parent": get_latest_value(folder, updates, "parent"),
                        "name": re.compile(f"^{data["name"]}$", re.IGNORECASE),
                    },
                    limit=1,
                )

                if name_duplicate_count != 0:
                    return jsonify({"message": "Folder with this name already exists."}), 409
                
                # Update local container folder reference
                container["folders"][folder_id]["name"] = data["name"]
                # Update remote container folder reference
                containers.update_one(
                    {"_id": ObjectId(container_id)},
                    {
                        "$set": {
                            f"folders.{folder_id}.name": data["name"],
                            "lastModified": timestamp,
                        }
                    },
                )

                updates["name"] = data["name"]

            if updates:

                for file in file_keys:
                    file_path = get_path(file["folder"], container, include_all=False)
                    new_key = get_key_string(container_id, file_path, file["name"])

                    rename_s3_object(file["key"], new_key)

                    files.update_one(
                        {"_id": ObjectId(file["fileId"])},
                        {
                            "$set": {
                                "key": new_key,
                            }
                        },
                    )

                folders.update_one(
                    {"_id": ObjectId(folder_id)},
                    {
                        "$set": {
                            "lastModified": timestamp,
                            **updates
                        }
                    },
                )

                return jsonify({
                    "folderId": folder_id,
                    "path": get_path(folder_id, container),
                    "lastModified": str(timestamp),
                }), 200

            else:
                return jsonify({"message": "No valid updates provided."}), 400
            
        else:
            return jsonify({"message": "Folder not found within this container."}), 404

    else:
        if containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
            return jsonify({
                "message": "User is not authorized to modify this container's folders."
            }), 401
        else:
            return jsonify({"message": "Container not found."}), 404
        
    
@folders_bp.route("/containers/<container_id>/folders/<folder_id>", methods=["DELETE"])
@require_auth
def delete_folder(container_id, folder_id):
    containers = db["containers"]
    files = db["files"]
    folders = db["folders"]

    timestamp = datetime.now(timezone.utc)

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        if folder_id == "~":
            return jsonify({"message": "Cannot delete root folder."}), 403
        
        if get_path(folder_id, container, include_all=False) == -1:
            return jsonify({"message": "Folder not found within this container."}), 404
        
        all_files, all_folders = get_container_contents(container_id, files, folders)
        folder_keys, file_keys = get_all_keys(folder_id, all_folders, all_files)

        contains_entrypoint = any(file["fileId"] == str(container["entryPoint"]) for file in file_keys)
        if contains_entrypoint:
            return jsonify({
                "message": (
                    "Cannot delete folder containing the Entry Point of this container. "
                    "Set a different file as the Entry Point before re-attempting."
                )
            }), 409

        delete_keys = [
            {"Key": file["key"]} 
            for file in file_keys
        ]
        if delete_keys:
            s3.Bucket(S3_BUCKET_NAME).delete_objects(Delete={"Objects": delete_keys})

        folders.delete_many({
            "_id": {
                "$in": [ObjectId(folder["folderId"]) for folder in folder_keys]
            }
        })

        files.delete_many({
            "_id": {
                "$in": [ObjectId(file["fileId"]) for file in file_keys]
            }
        }) 

        container_updates = { 
            "lastModified": timestamp 
        }
        if contains_entrypoint:
            container_updates["entryPoint"] = None  

        containers.update_one(
            {"_id": ObjectId(container_id)}, 
            {
                "$set": container_updates,
                "$unset": {
                    f"folders.{folder['folderId']}": "" for folder in folder_keys
                },
            },
        )

        return jsonify({
            "message": "Folder deleted."
        }), 200
    else:
        if containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
            return jsonify({
                "message": "User is not authorized to delete this container's folders."
            }), 401
        else:
            return jsonify({"message": "Container not found."}), 404