# -*- coding: utf-8 -*-
"""
SMTP 비밀번호 암호화 도구
실행: python encrypt_tool.py
"""
from cryptography.fernet import Fernet

def generate_key():
    key = Fernet.generate_key()
    print("\n[1] 아래 ENCRYPT_KEY를 .env 파일에 저장하세요:")
    print(f"ENCRYPT_KEY={key.decode()}\n")
    return key

def encrypt_password(key: bytes, password: str) -> str:
    f = Fernet(key)
    return f.encrypt(password.encode()).decode()

def decrypt_password(key: bytes, encrypted: str) -> str:
    f = Fernet(key)
    return f.decrypt(encrypted.encode()).decode()

if __name__ == "__main__":
    print("=== SMTP 비밀번호 암호화 도구 ===")
    password = input("암호화할 비밀번호 입력: ").strip()

    key = generate_key()
    encrypted = encrypt_password(key, password)

    print("[2] 아래 SMTP_PASS_ENC를 .env 파일에 저장하세요:")
    print(f"SMTP_PASS_ENC={encrypted}\n")
    print("※ ENCRYPT_KEY와 SMTP_PASS_ENC 둘 다 .env(또는 Render 환경변수)에 설정하세요.")
