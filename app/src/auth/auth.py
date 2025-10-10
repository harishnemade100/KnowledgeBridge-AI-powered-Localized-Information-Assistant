import time
import hmac
import json
import base64
import hashlib

JWT_SECRET = "replace-with-strong-secret"
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_MINUTES = 60 * 24

def create_jwt(payload: dict, expire_minutes=JWT_EXPIRE_MINUTES):
    payload = payload.copy()
    payload["exp"] = int(time.time() + expire_minutes * 60)
    header = {"alg": JWT_ALGORITHM, "typ": "JWT"}
    def b64(e): return base64.urlsafe_b64encode(json.dumps(e).encode()).rstrip(b"=").decode()
    s = f"{b64(header)}.{b64(payload)}"
    sig = base64.urlsafe_b64encode(hmac.new(JWT_SECRET.encode(), s.encode(), hashlib.sha256).digest()).rstrip(b"=").decode()
    return f"{s}.{sig}"

def hash_password(password: str): return hashlib.sha256(password.encode()).hexdigest()

def verify_password(password: str, password_hash: str): return hash_password(password) == password_hash
