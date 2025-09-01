from flask import Flask
from flask_cors import CORS

from src.cloudcontain_api.routes.containers import containers
from src.cloudcontain_api.routes.files import files
from src.cloudcontain_api.routes.folders import folders
from src.cloudcontain_api.routes.jobs import jobs
from src.cloudcontain_api.routes.users import users

app = Flask(__name__)
CORS(
    app,
    origins=[
        "https://cloudcontain.net",
        "http://localhost:5173",
    ],
    supports_credentials=True,
)

app.register_blueprint(containers)
app.register_blueprint(files)
app.register_blueprint(folders)
app.register_blueprint(jobs)
app.register_blueprint(users)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)