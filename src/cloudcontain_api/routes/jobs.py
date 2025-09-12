from bson import ObjectId
from flask import Blueprint, jsonify, request
from flask import current_app as app

from cloudcontain_api.utils.auth import require_auth

jobs_bp = Blueprint("jobs", __name__)


@jobs_bp.route("/containers/<container_id>/jobs/<job_id>/logs", methods=["GET"])
@require_auth
def get_job_logs(container_id, job_id):
    offset = int(request.args.get("offset", 0))
    containers = app.db["containers"]
    jobs = app.db["jobs"]
    logs = app.db["logs"]

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        job = jobs.find_one(
            {"_id": ObjectId(job_id), "containerId": ObjectId(container_id)}
        )

        if job:
            # BUG: Daemon is not publising the NS timestamp correctly, so we cannot sort by it.
            # Need to fix this, it should be publishing so that it can accurately sort by chronological order,
            # still needs to use timestamp to convert from UTC to local time on the client side.
            query_result = logs.aggregate(
                [
                    {"$match": {"jobId": ObjectId(job_id)}},
                    {"$sort": {"ns": -1}},
                    {"$limit": offset + 10},
                    {"$skip": offset},
                ]
            )

            results = [
                {
                    "content": log["content"],
                    "timestamp": str(log["timestamp"]),
                    "ns": log["ns"],
                    "level": log["level"],
                }
                for log in query_result
            ]

            results = sorted(results, key=lambda log: log["ns"])

            return jsonify(results), 200

        else:
            return jsonify({"message": "Job not found for this container."}), 404
    elif containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
        return jsonify({
            "message": "User is not authorized to access this container's job logs."
        }), 401
    else:
        return jsonify({"message": "Container not found."}), 404

@jobs_bp.route("/containers/<container_id>/jobs", methods=["GET"])
@require_auth
def list_jobs(container_id):
    offset = int(request.args.get("offset", 0))
    containers = app.db["containers"]
    jobs = app.db["jobs"]
    logs = app.db["logs"]

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        query_result = jobs.find(
            {"containerId": ObjectId(container_id)}, limit=offset + 10, skip=offset
        ).sort("queued", -1)

        results = [
            {
                "jobId": str(job["_id"]),
                "status": job["status"],
                "queued": str(job["queued"]) if job["queued"] else None,
                "started": str(job["started"]) if job["started"] else None,
                "ended": str(job["ended"]) if job["ended"] else None,
                "requestedBy": job["requestedBy"],
                "node": str(job["node"]),
                "logCount": logs.count_documents({"jobId": job["_id"]}),
                "output": [],
            }
            for job in query_result
        ]

        return jsonify(results), 200

    elif containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
        return jsonify({
            "message": "User is not authorized to access this container's job history."
        }), 401
    else:
        return jsonify({"message": "Container not found."}), 404