import json
from datetime import datetime, timedelta, timezone

from bson import ObjectId
from flask import Blueprint, jsonify, request
from flask import current_app as app

from cloudcontain_api.utils.auth import require_auth
from cloudcontain_api.utils.constants import (
    JOB_NODE_AMI_ID,
    S3_BUCKET_NAME,
    SQS_URL,
)

containers_bp = Blueprint("containers", __name__)


@containers_bp.route("/containers", methods=["POST"])
@require_auth
def create_container():
    data = request.get_json()
    col = app.db["containers"]

    timestamp = datetime.now(timezone.utc)

    if "name" not in data or not data["name"].strip():
        return jsonify({"message": "Please provide a valid container name."}), 400
    
    owned_containers = col.count_documents({"owner": request.user["sub"]})
    if owned_containers >= 3:
        return jsonify(
            {"message": "You have reached the limit of 3 free containers."}
        ), 403

    insert = col.insert_one(
        {
            "owner": request.user["sub"],
            "name": data["name"],
            "description": None,
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
    offset = int(request.args.get("offset", 0))
    col = app.db["containers"]

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
    col = app.db["containers"]

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
    
    elif col.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
        return jsonify({
            "message": "User is not authorized to access this container."
        }), 401
    else:
        return jsonify({"message": "Container not found."}), 404
        

@containers_bp.route("/containers/<container_id>", methods=["PUT"])
@require_auth
def update_container(container_id):
    data = request.get_json()
    col = app.db["containers"]

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

        if "description" in data:
            updates["description"] = data["description"].strip()
        
        if "public" in data:
            try:
                updates["public"] = bool(data["public"])
            except ValueError:
                return jsonify({"message": "Public must be specified as a boolean value."}), 400

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
        
    elif col.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
        return jsonify({
            "message": "User is not authorized to modify this container."
        }), 401
    else:
        return jsonify({"message": "Container not found."}), 404
        

@containers_bp.route("/containers/<container_id>", methods=["DELETE"])
@require_auth
def delete_container(container_id):
    containers = app.db["containers"]
    folders = app.db["folders"]
    files = app.db["files"]
    jobs = app.db["jobs"]
    logs = app.db["logs"]

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        
        try:
            bucket = app.s3.Bucket(S3_BUCKET_NAME)
            to_delete = bucket.objects.filter(Prefix=f"{container_id}/")
            delete_keys = [{"Key": obj.key} for obj in to_delete]
            if delete_keys:
                bucket.delete_objects(Delete={"Objects": delete_keys})
        except Exception as e:
            return jsonify({"message": f"Error deleting container files from S3. {e}"}), 500

        files.delete_many({"containerId": ObjectId(container_id)})
        folders.delete_many({"containerId": ObjectId(container_id)})
        jobs.delete_many({"containerId": ObjectId(container_id)})
        logs.delete_many({"containerId": ObjectId(container_id)})

        containers.delete_one({"_id": ObjectId(container_id)})
        
        return '', 204
    
    elif containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
        return jsonify({
            "message": "User is not authorized to delete this container."
        }), 401
    else:
        return jsonify({"message": "Container not found."}), 404
        

@containers_bp.route("/containers/<container_id>/execute", methods=["POST"])
@require_auth
def execute_container(container_id):
    containers = app.db["containers"]
    jobs = app.db["jobs"]
    nodes = app.db["nodes"]

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
        
        jobs_last_month = jobs.count_documents({
            "requestedBy": request.user["sub"],
            "queued": {"$gte": datetime.now(timezone.utc) - timedelta(days=30)}
        })
        if jobs_last_month >= 50:
            return jsonify(
                {"message": "You have reached the limit of 50 jobs in the last 30 days."}
            ), 429
        
        job_status = "PENDING"
        node_count = nodes.count_documents({"$or": [{"alive": True}, {"pending": True}]})
        queued_jobs = jobs.count_documents({
            "status": {"$in": ["STARTING_NODE", "PENDING"]}
        })

        if (node_count == 0 or queued_jobs >= 20) and node_count < 3:
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
                app.ec2.run_instances(
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
        else:
            node = nodes.find_one({"$or": [{"alive": False}, {"pending": True}]})
            if node:
                job_status = "STARTING_NODE"
            
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
            app.pusher.trigger(
                container_id,
                "job-queued",
                {
                    "jobId": job_id,
                    "status": job_status,
                    "queued": str(queued_time),
                    "started": None,
                    "ended": None,
                    "node": None,
                    "output": [],
                },
            )

            # Insert job into SQS queue
            app.sqs.send_message(
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
                    "node": None,
                    "logCount": 0,
                    "output": [],
                }
            ), 201
        
        else:
            return jsonify(
                {"message": "Error queuing job. Please try again later."}
            ), 500
        
    elif containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
        return jsonify({
            "message": "User is not authorized to execute this container."
        }), 401
    else:
        return jsonify({"message": "Container not found."}), 404