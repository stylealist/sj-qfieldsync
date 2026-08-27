# 1. GIS 및 시스템 종속성을 해결하기 위해 OS 레벨의 라이브러리가 포함된 베이스 이미지 사용
FROM python:3.12.7-slim

# 2. 필수 시스템 패키지 설치
# - gdal-bin, libgdal-dev: GeoPandas 및 Fiona 실행용
# - ffmpeg: pydub를 통한 m4a -> wav 변환용
# - libpq-dev: psycopg2 빌드용
# - gcc, g++: 일부 파이썬 패키지 컴파일용
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    gdal-bin \
    libgdal-dev \
    libpq-dev \
    ffmpeg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 3. 환경 변수 설정 (GDAL 경로 및 파이썬 출력 버퍼링 해제)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# 4. 작업 디렉토리 생성
WORKDIR /app

# 5. 종속성 설치
# requirements.txt를 먼저 복사하여 캐시 효율성 높임
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 6. 소스 코드 복사
COPY . .

# 7. 볼륨 설정 (QField 프로젝트 파일 다운로드 경로)
# 컨테이너가 삭제되어도 다운로드된 데이터가 유지되도록 호스트와 연결 권장
RUN mkdir -p /app/qfield
VOLUME ["/app/qfield"]

# 8. 실행 명령
CMD ["python", "sync_watcher.py"]