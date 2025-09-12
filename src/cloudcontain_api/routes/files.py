import re
from datetime import datetime, timezone

from bson import ObjectId
from flask import Blueprint, Response, jsonify, request, stream_with_context
from flask import current_app as app

from cloudcontain_api.utils.auth import require_auth
from cloudcontain_api.utils.constants import (
    S3_BUCKET_NAME,
)
from cloudcontain_api.utils.utils import (
    get_folder_id,
    get_key_string,
    get_path,
    rename_s3_object,
    stream_s3_object,
)

files_bp = Blueprint("files", __name__)


@files_bp.route("/containers/<container_id>/folders/<folder_id>/files", methods=["POST"])
@require_auth
def create_file(container_id, folder_id):
    data = request.get_json()
    containers = app.db["containers"]
    files = app.db["files"]

    timestamp = datetime.now(timezone.utc)

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        folder_path = get_path(folder_id, container, include_all=False)
        if folder_path == -1:
            return jsonify({"message": "Folder not found within this container."}), 404
        
        if ("name" not in data or 
            not data["name"].strip() or
            not re.match(r"[\w\-.]+\.[\w]+$", data["name"])):
            return jsonify({"message": "Please provide a valid filename."}), 400
        
        if not data["name"].lower().endswith((".java", ".py", ".c", ".h")):
            return jsonify(
                {"message": "Can only save Java, Python or C (.c or .h) files."}
            ), 403
        
        name_duplicate_count = files.count_documents(
            {"containerId": ObjectId(container_id), 
            "folder": get_folder_id(folder_id), 
            "name": re.compile(f"^{data["name"]}$", re.IGNORECASE)},
            limit=1,
        ) 

        if name_duplicate_count != 0:
            return jsonify({"message": "File with this name already exists."}), 409
        
        s3_key = get_key_string(container_id, folder_path, data["name"])
        s3_response =  app.s3.Object(S3_BUCKET_NAME, s3_key).put(Body="")

        if s3_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            return jsonify({"message": "Error creating file in storage."}), 500
        
        insert_response = files.insert_one(
            {
                "containerId": ObjectId(container_id),
                "createdBy": request.user["sub"],
                "folder": get_folder_id(folder_id),
                "key": s3_key,
                "size": 0,
                "name": data["name"].strip(),
                "created": timestamp,
                "lastModified": timestamp,
            }
        )

        if insert_response.inserted_id:
            containers.update_one(
                {"_id": ObjectId(container_id)}, {"$set": {"lastModified": timestamp}}
            )

            is_entry = False
            if container["entryPoint"] is None:
                containers.update_one(
                    {"_id": ObjectId(container_id)},
                    {"$set": {"entryPoint": insert_response.inserted_id}},
                )
                is_entry = True

            return jsonify(
                {"fileId": str(insert_response.inserted_id), "isEntry": is_entry}
            ), 201
        
        else:
            return jsonify({"message": "Error creating file."}), 500
    elif containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
        return jsonify(
            {"message": "User is not authorized to modify this container."}
        ), 401
    else:
        return jsonify({"message": "Container not found."}), 404


@files_bp.route("/containers/<container_id>/files/<file_id>", methods=["GET"])
@require_auth
def get_file(container_id, file_id):
    containers = app.db["containers"]
    files = app.db["files"]

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        file = files.find_one(
            {"_id": ObjectId(file_id), "containerId": ObjectId(container_id)}
        )

        if file:
            return jsonify(
                {
                    "fileId": str(file["_id"]),
                    "containerId": str(file["containerId"]),
                    "createdBy": file["createdBy"],
                    "folderId": str(file["folder"]),
                    "key": file["key"],
                    "size": file["size"],
                    "path": get_path(str(file["folder"]), container),
                    "name": file["name"],
                    "created": str(file["created"]),
                    "lastModified": str(file["lastModified"]),
                }
            ), 200
        
        else:
            return jsonify({"message": "File not found within this container."}), 404
    elif containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
        return jsonify(
            {"message": "User is not authorized to access this container's files."}
        ), 401
    else:
        return jsonify({"message": "Container not found."}), 404
        

@files_bp.route("/containers/<container_id>/files/<file_id>/content", methods=["GET"])
@require_auth
def get_file_content(container_id, file_id):
    containers = app.db["containers"]
    files = app.db["files"]

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        file = files.find_one(
            {"_id": ObjectId(file_id), "containerId": ObjectId(container_id)}
        )

        if file:
            return Response(
                stream_with_context(stream_s3_object(file["key"])), 
                content_type="application/octet-stream"
            ), 200
        
        else:
            return jsonify({"message": "File not found within this container."}), 404
    elif containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
        return jsonify(
            {"message": "User is not authorized to access this container's files."}
        ), 401
    else:
        return jsonify({"message": "Container not found."}), 404


@files_bp.route("/containers/<container_id>/files/<file_id>", methods=["PUT"])
@require_auth
def update_file(container_id, file_id):
    data = request.get_json()
    containers = app.db["containers"]
    files = app.db["files"]

    timestamp = datetime.now(timezone.utc)

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        file = files.find_one(
            {"_id": ObjectId(file_id), "containerId": ObjectId(container_id)}
        )

        if file:
            new_name = data.get("name", file["name"]).strip()
            new_folder = get_folder_id(data.get("folder", file["folder"]))

            name_duplicate_count = files.count_documents(
                {"containerId": ObjectId(container_id), 
                 "folder": new_folder, 
                 "name": re.compile(f"^{new_name}$", re.IGNORECASE),
                 "_id": {"$ne": ObjectId(file_id)}},
                limit=1,
            )

            if name_duplicate_count != 0:
                return jsonify({"message": "File with this name already exists."}), 409

            updates = dict()

            if "name" in data and data["name"]:
                data["name"] = data["name"].strip()

                if not re.match(r"[\w\-.]+\.[\w]+$", data["name"]):
                    return jsonify({"message": "Please provide a valid filename."}), 400
                
                if not data["name"].lower().endswith((".java", ".py", ".c", ".h")):
                    return jsonify(
                        {"message": "Can only save Java, Python or C (.c or .h) files."}
                    ), 403
                
                updates["name"] = data["name"]

            if "folder" in data and data["folder"]:
                folder_path = get_path(data["folder"], container, include_all=False)
                if folder_path == -1:
                    return jsonify({"message": "Folder not found within this container."}), 404

                updates["folder"] = get_folder_id(data["folder"])

            if updates:
                new_path = get_path(new_folder, container, include_all=False)
                new_key = get_key_string(container_id, new_path, new_name)
                updates["key"] = new_key    
                rename_s3_object(file["key"], new_key)

                files.update_one(
                    {"_id": ObjectId(file_id)},
                    {
                        "$set": {
                            "lastModified": timestamp,
                            **updates
                        }
                    },
                )

                return jsonify({
                    "fileId": str(file["_id"]),
                    "folderId": new_folder,
                    "key": new_key,
                    "name": new_name,
                    "path": get_path(new_folder, container),
                    "lastModified": str(timestamp)
                }), 200

            else:
                return jsonify({"message": "No valid updates provided."}), 400
        else:
            return jsonify({"message": "File not found within this container."}), 404
    elif containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
        return jsonify(
            {"message": "User is not authorized to modify this container's files."}
        ), 401
    else:
        return jsonify({"message": "Container not found."}), 404
    

@files_bp.route("/containers/<container_id>/files/<file_id>/content", methods=["PUT"])
@require_auth
def update_file_content(container_id, file_id):
    containers = app.db["containers"]
    files = app.db["files"]

    timestamp = datetime.now(timezone.utc)

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        file = files.find_one(
            {"_id": ObjectId(file_id), "containerId": ObjectId(container_id)}
        )

        if file:
            file_size = request.content_length

            try:
                s3_object = app.s3.Object(S3_BUCKET_NAME, file["key"])
                s3_object.upload_fileobj(request.stream)
            except Exception as e:
                return jsonify({"message": f"Error updating file content in S3. {e}"}), 500
            
            files.update_one(
                {"_id": ObjectId(file_id)}, {"$set": 
                    {
                        "lastModified": timestamp,
                        "size": file_size,
                    }
                }
            )

            containers.update_one(
                {"_id": ObjectId(container_id)}, {"$set": {"lastModified": timestamp}}
            )

            return jsonify(
                {
                    "fileId": str(file["_id"]),
                    "lastModified": str(timestamp),
                    "size": file_size,
                }
            ), 200

        else:
            return jsonify({"message": "File not found within this container."}), 404
    elif containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
        return jsonify(
            {"message": "User is not authorized to modify this container's files."}
        ), 401
    else:
        return jsonify({"message": "Container not found."}), 404


@files_bp.route("/containers/<container_id>/files/<file_id>", methods=["DELETE"])
@require_auth
def delete_file(container_id, file_id):
    containers = app.db["containers"]
    files = app.db["files"]

    timestamp = datetime.now(timezone.utc)

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:
        if str(container["entryPoint"]) == file_id:
            return jsonify({
                "message": (
                    "Cannot delete the Entry Point of this container. "
                    "Set a different file as the Entry Point before re-attempting."
                )
            }), 409
        
        file = files.find_one(
            {"_id": ObjectId(file_id), "containerId": ObjectId(container_id)}
        )

        if file:
            try:
                s3_object = app.s3.Object(S3_BUCKET_NAME, file["key"])
                response = s3_object.delete()
                if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
                    return jsonify({"message": "Error deleting file from S3."}), 500
            except Exception as e:
                return jsonify({"message": f"Error deleting file from S3. {e}"}), 500
            
            files.delete_one({"_id": ObjectId(file_id)})

            containers.update_one(
                {"_id": ObjectId(container_id)}, {
                    "$set": {"lastModified": timestamp}
                }
            )

            return '', 204
        
        else:
            return jsonify({"message": "File not found within this container."}), 404
    elif containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
        return jsonify(
            {"message": "User is not authorized to delete this container's files."}
        ), 401
    else:
        return jsonify({"message": "Container not found."}), 404
    

@files_bp.route("/containers/<container_id>/files/search", methods=["POST"])
@require_auth
def search_files(container_id):
    data = request.get_json()
    offset = int(request.args.get("offset", 0))
    containers = app.db["containers"]
    files = app.db["files"]

    container = containers.find_one(
        {"_id": ObjectId(container_id), "owner": request.user["sub"]}
    )

    if container:

        if "query" not in data or not data["query"].strip():
            return jsonify({"message": "Please provide a valid search query."}), 400
        
        query = re.compile(re.escape(data["query"]), re.IGNORECASE)
        query_result = files.aggregate([
            {
                "$match": {
                    "containerId": ObjectId(container_id),
                    "name": query,
                }
            },
            { "$limit": offset + 10, },
            { "$skip": offset },
            { "$sort": {"name": 1}},
        ])
        result_count = files.count_documents(
            {
                "containerId": ObjectId(container_id),
                "name": query,
            }
        )    

        results = [
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
            for file in query_result
        ]

        return jsonify({
            "files": results,
            "total": result_count,
            "hasMore": result_count > offset + 10
        }), 200
    
    elif containers.count_documents({"_id": ObjectId(container_id)}, limit=1) != 0:
        return jsonify(
            {"message": "User is not authorized to search this container's files."}
        ), 401
    else:
        return jsonify({"message": "Container not found."}), 404