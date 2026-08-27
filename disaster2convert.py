# -*- coding: utf-8 -*-
"""
Title   : 재난안전 음성파일 텍스트 처리 및 키워드 추출 실행
Created : 2025-05-07
Author  : 주성중
Description :
    - 음성파일(m4a/wav)을 STT로 변환하여 텍스트 추출
    - 사용자 정의 키워드 추출기 모듈을 이용해 핵심 단어 추출
    - 필요 시 PostgreSQL 연동 및 DB 저장 구조를 주석으로 포함
"""

import numpy as np
import os
import sys
# import shutil  # 파일 이동/삭제 등의 고급 작업 시 사용 가능
# from tabula import read_pdf  # PDF 파싱용 (사용하지 않음)
import time as t
import datetime

import speech_recognition as sr  # 🎤 음성 인식 라이브러리
from pydub import AudioSegment  # 🎵 다양한 오디오 포맷 변환 라이브러리

# ✅ 음성 인식기 생성 및 민감도 설정
recognizer = sr.Recognizer()
recognizer.energy_threshold = 300  # 배경 소음에 대한 인식 기준선

# ✅ wav 파일 읽어 텍스트 변환
def read_audio(filepath):
    print("read_audio 진입")
    text = ""

    # ✅ 파일 확장자 확인 및 변환 (wav로 변환)
    ext = os.path.splitext(filepath)[1]
    if '.wav' != ext:
        track = AudioSegment.from_file(filepath)  # 다른 포맷(m4a 등) → AudioSegment 객체
        filepath = filepath.replace(ext, ".wav")  # 확장자 변경
        track.export(filepath, format='wav')  # wav로 저장

    try:
        harvard_audio = sr.AudioFile(filepath)  # 파일 로드
        with harvard_audio as source:
            audio = recognizer.record(source)  # 전체 오디오 녹음

        # ✅ 구글 STT API 사용 (한국어 인식)
        text = recognizer.recognize_google(audio_data=audio, language="ko-KR")

        os.remove(filepath)  # wav 파일 삭제 (임시 파일 정리)
    except Exception as e:
        os.remove(filepath)  # 예외 발생 시에도 파일 제거
        print("Exception: ", str(e))  # 에러 로그 출력
        text = ""
    return text  # 최종 텍스트 반환
