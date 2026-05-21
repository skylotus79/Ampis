"""
AMPIS S3 데이터베이스 동기화 모듈
- Render 환경의 임시 파일시스템(Ephemeral Filesystem) 특성으로 인한 데이터 휘발 문제를 극복합니다.
- 서버 기동 시 S3에서 최신 DB를 다운로드(Restore)하고, 크롤링/파싱 발생 시 S3로 업로드(Backup)합니다.
"""

import os
try:
    import boto3
    from botocore.exceptions import ClientError
    _BOTO3_AVAILABLE = True
except ImportError:
    _BOTO3_AVAILABLE = False

# 배포 환경 분리에 따른 SQLite 경로 일치화
if os.environ.get("RENDER"):
    DB_PATH = "/tmp/ampis.db"
else:
    DB_PATH = "ampis.db"

S3_BUCKET = os.environ.get("S3_BUCKET_NAME")
S3_KEY = "ampis.db"

def get_s3_client():
    """AWS 자격 증명 정보를 검증하여 boto3 S3 클라이언트를 반환합니다."""
    if not _BOTO3_AVAILABLE:
        return None
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    region = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")
    
    if not all([aws_key, aws_secret, S3_BUCKET]):
        return None
    
    return boto3.client(
        "s3",
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=region
    )

def backup_db() -> bool:
    """로컬(혹은 /tmp/)의 SQLite 파일을 AWS S3 저장소에 백업합니다."""
    client = get_s3_client()
    if not client:
        print("[S3 Sync] AWS 자격증명 또는 버킷명이 유효하지 않아 백업을 건너뜁니다.")
        return False
        
    if not os.path.exists(DB_PATH):
        print(f"[S3 Sync] 백업할 로컬 데이터베이스 파일({DB_PATH})이 존재하지 않습니다.")
        return False
        
    try:
        print(f"[S3 Sync] 백업 진행 중: {DB_PATH} -> S3://{S3_BUCKET}/{S3_KEY}")
        client.upload_file(DB_PATH, S3_BUCKET, S3_KEY)
        print("[S3 Sync] S3 데이터베이스 백업이 안전하게 완료되었습니다.")
        return True
    except ClientError as e:
        print(f"[S3 Sync Error] 백업 실패: {e}")
        return False

def restore_db() -> bool:
    """서버 기동 시 S3로부터 기존에 저장된 데이터베이스를 복원해옵니다."""
    client = get_s3_client()
    if not client:
        print("[S3 Sync] AWS 자격증명 또는 버킷명이 유효하지 않아 로컬 데이터베이스 파일로 작동합니다.")
        return False
        
    try:
        print(f"[S3 Sync] 복원 진행 중: S3://{S3_BUCKET}/{S3_KEY} -> {DB_PATH}")
        os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else ".", exist_ok=True)
        client.download_file(S3_BUCKET, S3_KEY, DB_PATH)
        print("[S3 Sync] S3로부터 기존 데이터베이스를 정상 복원했습니다.")
        return True
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') == "404":
            print("[S3 Sync] S3 저장소에 기존 백업본이 없습니다. 새로운 데이터베이스로 서비스를 개시합니다.")
        else:
            print(f"[S3 Sync Error] 복원 중 문제 발생: {e}")
        return False