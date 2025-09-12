import boto3
from flask import Flask
from flask_cors import CORS
from pusher import Pusher
from pymongo import MongoClient

from cloudcontain_api.routes.containers import containers_bp
from cloudcontain_api.routes.files import files_bp
from cloudcontain_api.routes.folders import folders_bp
from cloudcontain_api.routes.jobs import jobs_bp
from cloudcontain_api.routes.users import users_bp
from cloudcontain_api.utils.constants import (
    MONGO_CONN_STRING,
    MONGO_DB_NAME,
    PUSHER_APP_ID,
    PUSHER_CLUSTER,
    PUSHER_KEY,
    PUSHER_SECRET,
)

app = Flask(__name__)
CORS(
    app,
    origins=[
        "https://cloudcontain.net",
        "http://localhost:5173",
    ],
    supports_credentials=True,
)

db_client = MongoClient(MONGO_CONN_STRING)
app.db = db_client[MONGO_DB_NAME]

app.s3 = boto3.resource("s3")
app.sqs = boto3.client("sqs", region_name="us-west-1")
app.ec2 = boto3.client("ec2", region_name="us-west-1")

app.pusher = Pusher(
    app_id=PUSHER_APP_ID,
    key=PUSHER_KEY,
    secret=PUSHER_SECRET,
    cluster=PUSHER_CLUSTER,
)

app.register_blueprint(containers_bp)
app.register_blueprint(files_bp)
app.register_blueprint(folders_bp)
app.register_blueprint(jobs_bp)
app.register_blueprint(users_bp)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)
