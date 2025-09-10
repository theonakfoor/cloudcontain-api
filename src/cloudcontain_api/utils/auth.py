"""
Utility functions for authentication and authorization using Auth0.
"""

import requests
from flask import jsonify, request
from jose import jwt

from src.cloudcontain_api.utils.constants import (
    AUTH0_ALGORITHMS,
    AUTH0_API_IDENTIFIER,
    AUTH0_DOMAIN,
)


def get_jwks():
    jwks_json = requests.get(f"https://{AUTH0_DOMAIN}/.well-known/jwks.json")
    return jwks_json.json()


def require_auth(f):
    def wrapper(*args, **kwargs):
        token = request.headers.get("Authorization", None)
        if not token:
            return jsonify({"message": "Missing token"}), 401

        token = token.split()[1]
        jwks = get_jwks()
        unverified_header = jwt.get_unverified_header(token)
        rsa_key = {}
        for key in jwks["keys"]:
            if key["kid"] == unverified_header["kid"]:
                rsa_key = {
                    "kty": key["kty"],
                    "kid": key["kid"],
                    "use": key["use"],
                    "n": key["n"],
                    "e": key["e"],
                }
        if not rsa_key:
            return jsonify({"message": "Invalid token"}), 401

        try:
            payload = jwt.decode(
                token,
                rsa_key,
                algorithms=AUTH0_ALGORITHMS,
                audience=AUTH0_API_IDENTIFIER,
                issuer=f"https://{AUTH0_DOMAIN}/",
            )
        except jwt.ExpiredSignatureError:
            return jsonify({"message": "Token expired"}), 401
        except jwt.JWTClaimsError:
            return jsonify({"message": "Invalid claims"}), 401
        except Exception:
            return jsonify({"message": "Invalid token"}), 401

        request.user = payload
        return f(*args, **kwargs)

    wrapper.__name__ = f.__name__
    return wrapper