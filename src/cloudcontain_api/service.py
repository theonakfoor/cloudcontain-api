from flask import Flask
from flask_cors import CORS

from src.cloudcontain_api.routes.containers import containers_bp
from src.cloudcontain_api.routes.files import files_bp
from src.cloudcontain_api.routes.folders import folders_bp
from src.cloudcontain_api.routes.jobs import jobs_bp
from src.cloudcontain_api.routes.users import users_bp

app = Flask(__name__)
CORS(
    app,
    origins=[
        "https://cloudcontain.net",
        "http://localhost:5173",
    ],
    supports_credentials=True,
)

app.register_blueprint(containers_bp)
app.register_blueprint(files_bp)
app.register_blueprint(folders_bp)
app.register_blueprint(jobs_bp)
app.register_blueprint(users_bp)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)