from datetime import datetime, timezone
import boto3
from bson import ObjectId
from flask import Blueprint, request, jsonify
from pymongo import MongoClient

from src.cloudcontain_api.utils.auth import require_auth
from src.cloudcontain_api.utils.constants import (
    MONGO_CONN_STRING, MONGO_DB_NAME, S3_BUCKET_NAME
)

containers = Blueprint("containers", __name__)

db_client = MongoClient(MONGO_CONN_STRING)
db = db_client[MONGO_DB_NAME]

s3 = boto3.client("s3")


@containers.route("/containers", methods=["POST"])
@require_auth
def create_container():
    data = request.get_json()
    col = db["containers"]

    timestamp = datetime.now(timezone.utc)

    insert = col.insert_one(
        {
            "owner": request.user["sub"],
            "name": data["name"],
            "created": timestamp,
            "lastModified": timestamp,
            "public": False,
            "folders": {},
            "entryPoint": None,
        }
    )

    if insert.inserted_id:
        return jsonify({"containerId": str(insert.inserted_id)}), 201
    else:
        return jsonify({"message": "Error creating container."}), 500


@containers.route("/containers", methods=["GET"])
@require_auth
def list_containers():
    offset = int(request.args.get("offset")) if request.args.get("offset") else 0
    col = db["containers"]

    containers = col.find(
        {"owner": request.user["sub"]}, limit=offset + 10, skip=offset
    ).sort("created", -1)

    return jsonify(
        [
            {
                "containerId": str(container["_id"]),
                "name": container["name"],
                "created": str(container["created"]),
                "lastModified": str(container["lastModified"]),
                "public": container["public"],
                "entryPoint": str(container["entryPoint"])
            }
            for container in containers
        ]
    ), 200


@containers.route("/containers/<container_id>", methods=["GET"])
@require_auth
def get_container(container_id):
    col = db["containers"]

    container = col.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        """
        TODO: Create new collection called access_logs to log access to containers
        by users. Saving old snippet as reference.

        timestamp = datetime.now(timezone.utc)
        col.update_one(
            {"_id": ObjectId(container_id)}, {"$set": {"lastAccessed": timestamp}}
        )
        """
        return jsonify(
            {
                "containerId": str(container["_id"]),
                "name": container["name"],
                "created": container["created"],
                "lastModified": str(container["lastModified"]),
                "public": container["public"],
                "entryPoint": str(container["entryPoint"]),
            }
        ), 200
    else:
        if col.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
            return jsonify(
                {"message": "User is not authorized to access this container."}
            ), 401
        else:
            return jsonify({"message": "Container not found."}), 404
        

@containers.route("/containers/<container_id>", methods=["PUT"])
@require_auth
def update_container(container_id):
    data = request.get_json()
    col = db["containers"]

    timestamp = datetime.now(timezone.utc)
    container = col.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        updates = {}

        if "entryPoint" in data:
            updates["entryPoint"] = ObjectId(data["entryPoint"])

        if "name" in data:
            updates["name"] = data["name"].strip()

        if updates:
            col.update_one(
                {"_id": ObjectId(container_id)},
                {
                    "$set": {
                        "lastModified": timestamp,
                        **updates,
                    }
                },
            )
            return jsonify({"message": "Container updated."}), 204
        else:
            return jsonify({"message": "No valid updates provided."}), 400

    else:
        if col.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
            return jsonify(
                {"message": "User is not authorized to modify this container."}
            ), 401
        else:
            return jsonify({"message": "Container not found."}), 404
        

@containers.route("/containers/<container_id>", methods=["DELETE"])
@require_auth
def delete_container(container_id):
    containers = db["containers"]
    folders = db["folders"]
    files = db["files"]
    jobs = db["jobs"]
    logs = db["logs"]

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        # Delete s3 objects
        to_delete = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=f"{container_id}/")
        delete_keys = [{"Key": obj["Key"]} for obj in to_delete.get("Contents", [])]

        if delete_keys and len(delete_keys) > 0:
            s3.delete_objects(
                Bucket=S3_BUCKET_NAME,
                Delete={"Objects": delete_keys},
            )
        # Delete file metadata
        files.delete_many({"containerId": ObjectId(container_id)})
        # Delete folder metadata
        folders.delete_many({"containerId": ObjectId(container_id)})
        # Delete container job history
        jobs.delete_many({"containerId": ObjectId(container_id)})
        # Delete container log history
        logs.delete_many({"containerId": ObjectId(container_id)})
        # Delete container metadata
        containers.delete_one({"_id": ObjectId(container_id)})
        return jsonify({"message": "Container deleted."}), 200
    else:
        if containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
            return jsonify(
                {"message": "User is not authorized to delete this container."}
            ), 401
        else:
            return jsonify({"message": "Container not found."}), 404