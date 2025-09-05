import json
from datetime import datetime, timezone

import boto3
from bson import ObjectId
from flask import Blueprint, jsonify, request
from pusher import Pusher
from pymongo import MongoClient

from src.cloudcontain_api.utils.auth import require_auth
from src.cloudcontain_api.utils.constants import (
    JOB_NODE_AMI_ID,
    MONGO_CONN_STRING,
    MONGO_DB_NAME,
    PUSHER_APP_ID,
    PUSHER_CLUSTER,
    PUSHER_KEY,
    PUSHER_SECRET,
    S3_BUCKET_NAME,
    SQS_URL,
)

containers_bp = Blueprint("containers", __name__)

db_client = MongoClient(MONGO_CONN_STRING)
db = db_client[MONGO_DB_NAME]

s3 = boto3.resource("s3")
sqs = boto3.client("sqs", region_name="us-west-1")
ec2 = boto3.client("ec2", region_name="us-west-1")

pusher = Pusher(
    app_id=PUSHER_APP_ID,
    key=PUSHER_KEY,
    secret=PUSHER_SECRET,
    cluster=PUSHER_CLUSTER,
)


@containers_bp.route("/containers", methods=["POST"])
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
            "sharedWith": [],
        }
    )

    if insert.inserted_id:
        return jsonify({"containerId": str(insert.inserted_id)}), 201
    else:
        return jsonify({"message": "Error creating container."}), 500


@containers_bp.route("/containers", methods=["GET"])
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


@containers_bp.route("/containers/<container_id>", methods=["GET"])
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
            return jsonify({
                "message": "User is not authorized to access this container."
            }), 401
        else:
            return jsonify({"message": "Container not found."}), 404
        

@containers_bp.route("/containers/<container_id>", methods=["PUT"])
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
            return jsonify({
                "message": "User is not authorized to modify this container."
            }), 401
        else:
            return jsonify({"message": "Container not found."}), 404
        

@containers_bp.route("/containers/<container_id>", methods=["DELETE"])
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
        # Delete S3 objects
        bucket = s3.Bucket(S3_BUCKET_NAME)
        to_delete = bucket.objects.filter(Prefix=f"{container_id}/")
        delete_keys = [{"Key": obj.key} for obj in to_delete]
        if delete_keys:
            bucket.delete_objects(Delete={"Objects": delete_keys})

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
            return jsonify({
                "message": "User is not authorized to delete this container."
            }), 401
        else:
            return jsonify({"message": "Container not found."}), 404
        

@containers_bp.route("/containers/<container_id>/execute", methods=["POST"])
@require_auth
def execute_container(container_id):
    containers = db["containers"]
    jobs = db["jobs"]
    nodes = db["nodes"]

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        active_jobs = jobs.count_documents({
            "containerId": ObjectId(container_id), 
            "status": {"$nin": ["COMPLETED", "FAILED", "BUILD_FAILED"]}, 
        })
        if active_jobs > 0:
            return jsonify(
                {"message": "Container already has an active job running or queued."}
            ), 400
        
        job_status = "PENDING"
        node = nodes.find_one({"$or": [{"alive": True}, {"pending": True}]})

        if node:
            if node["alive"] == False:
                job_status = "STARTING_NODE"
        else:
            insert_node_response = nodes.insert_one(
                {
                    "pending": True,
                    "alive": False,
                    "launched": datetime.now(timezone.utc),
                    "started": None,
                    "instanceId": None,
                    "instanceType": None,
                    "instanceRegion": None,
                }
            )

            if insert_node_response.inserted_id:
                node_tag = str(insert_node_response.inserted_id)[-5:]
                ec2.run_instances(
                    ImageId=JOB_NODE_AMI_ID,
                    InstanceType="t3.small",
                    KeyName="cloudcontain",
                    MinCount=1,
                    MaxCount=1,
                    IamInstanceProfile={"Name": "EC2_CC_Node"},
                    TagSpecifications=[
                        {
                            "ResourceType": "instance",
                            "Tags": [
                                {
                                    "Key": "Name",
                                    "Value": f"CC-APP-NODE-{node_tag}",
                                }
                            ],
                        }
                    ],
                )

                job_status = "STARTING_NODE"
            else:
                return jsonify(
                    {"message": "Error starting node. Please try again later."}
                ), 500
            
        queued_time = datetime.now(timezone.utc)
        insert_job_response = jobs.insert_one(
            {
                "containerId": ObjectId(container_id),
                "status": job_status,
                "queued": queued_time,
                "started": None,
                "ended": None,
                "requestedBy": request.user["sub"],
                "node": None,
            }
        )

        if insert_job_response.inserted_id:
            # Notify Pusher than job has been queued for containerId
            job_id = str(insert_job_response.inserted_id)
            pusher.trigger(
                container_id,
                "job-queued",
                {
                    "jobId": job_id,
                    "status": job_status,
                    "queued": str(queued_time),
                    "started": None,
                    "ended": None,
                    "node": str(node),
                    "output": [],
                },
            )

            # Insert job into SQS queue
            sqs.send_message(
                QueueUrl=SQS_URL,
                MessageBody=json.dumps(
                    {
                        "jobId": job_id,
                        "containerId": container_id,
                        "queued": str(queued_time),
                    }
                ),
                MessageGroupId=container_id,
                MessageDeduplicationId=job_id,
            )

            return jsonify(
                {
                    "jobId": job_id,
                    "status": job_status,
                    "queued": str(queued_time),
                    "started": None,
                    "ended": None,
                    "node": str(node),
                    "logCount": 0,
                    "output": [],
                }
            ), 201
        else:
            return jsonify(
                {"message": "Error queuing job. Please try again later."}
            ), 500

    else:
        if containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
            return jsonify({
                "message": "User is not authorized to execute this container."
            }), 401
        else:
            return jsonify({"message": "Container not found."}), 404