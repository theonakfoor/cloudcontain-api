import requests
from flask import Blueprint, jsonify, request
from pymongo import MongoClient

from src.cloudcontain_api.utils.auth import require_auth
from src.cloudcontain_api.utils.constants import (
    AUTH0_DOMAIN,
    MONGO_CONN_STRING,
    MONGO_DB_NAME,
)

users_bp = Blueprint("users", __name__)

db_client = MongoClient(MONGO_CONN_STRING)
db = db_client[MONGO_DB_NAME]


@users_bp.route("/user", methods=["GET"])
@require_auth
def get_user():
    users = db["users"]
    containers = db["containers"]

    user = users.find_one({"authId": request.user["sub"]})

    if user:
        containerCount = containers.count_documents({"owner": request.user["sub"]})
        return jsonify(
            {
                "authId": user["authId"],
                "email": user["email"],
                "firstName": user["firstName"],
                "lastName": user["lastName"],
                "image": user["image"],
                "containers": int(containerCount),
            }
        ), 200
    else:
        user_info_response = requests.get(
            f"https://{AUTH0_DOMAIN}/userinfo",
            headers={"Authorization": request.headers.get("Authorization")},
        )
        user_info = user_info_response.json()

        insert_response = users.insert_one(
            {
                "authId": request.user["sub"],
                "email": user_info["email"],
                "firstName": user_info["given_name"],
                "lastName": user_info["family_name"],
                "image": user_info["picture"],
            }
        )

        if insert_response.inserted_id:
            return jsonify(
                {
                    "authId": request.user["sub"],
                    "email": user_info["email"],
                    "firstName": user_info["given_name"],
                    "lastName": user_info["family_name"],
                    "image": user_info["picture"],
                    "containers": 0,
                }
            ), 201
        else:
            return jsonify({"message": "Error inserting user information."}), 500