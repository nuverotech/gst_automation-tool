from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from passlib.context import CryptContext

SECRET_KEY = "your-secret-key-change-this-in-production-use-openssl-rand-hex-32"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7  # 7 days

# Use Argon2 instead of bcrypt (more secure and no compatibility issues)
pwd_context = CryptContext(
    schemes=["argon2"],
    deprecated="auto"
)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against a hash"""
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except Exception as e:
        print(f"Password verification error: {e}")
        return False


def get_password_hash(password: str) -> str:
    """Hash a password"""
    try:
        return pwd_context.hash(password)
    except Exception as e:
        print(f"Password hashing error: {e}")
        raise


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Create JWT access token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def decode_access_token(token: str):
    """Decode JWT token"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None
