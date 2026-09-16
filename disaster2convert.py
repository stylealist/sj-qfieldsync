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
import tempfile
# import shutil  # 파일 이동/삭제 등의 고급 작업 시 사용 가능
# from tabula import read_pdf  # PDF 파싱용 (사용하지 않음)
import time as t
import datetime

import speech_recognition as sr  # 🎤 음성 인식 라이브러리
from pydub import AudioSegment  # 🎵 다양한 오디오 포맷 변환 라이브러리

# ✅ 음성 인식기 생성 및 민감도 설정
recognizer = sr.Recognizer()
recognizer.energy_threshold = 300  # 배경 소음에 대한 인식 기준선

# ✅ 음성 파일을 읽어 텍스트로 변환
def read_audio(filepath):
    """
    m4a 등 오디오 파일을 wav 로 변환한 뒤 구글 STT(한국어)로 텍스트를 얻는다.

    실패 원인을 삼키지 않고 호출자에게 전달한다(호출부가 로그로 남긴다).
    인식 결과가 없는 경우(무음·발화 없음)에만 빈 문자열을 돌려준다.
    변환용 임시 wav 는 원본 옆이 아니라 임시 디렉터리에 만들고 반드시 정리한다.
    """
    root, ext = os.path.splitext(filepath)
    temp_wav = None

    try:
        if ext.lower() != ".wav":
            # ffmpeg 가 없으면 여기서 예외가 난다 → 원인이 그대로 호출부 로그에 남는다
            track = AudioSegment.from_file(filepath)
            handle, temp_wav = tempfile.mkstemp(prefix="stt_", suffix=".wav")
            os.close(handle)
            track.export(temp_wav, format="wav")
            wav_path = temp_wav
        else:
            wav_path = filepath

        with sr.AudioFile(wav_path) as source:
            audio = recognizer.record(source)

        try:
            # 구글 STT API 사용 (한국어 인식) — 네트워크가 필요하다
            return recognizer.recognize_google(audio_data=audio, language="ko-KR")
        except sr.UnknownValueError:
            # 말소리를 인식하지 못한 경우는 오류가 아니라 '결과 없음'
            return ""
    finally:
        if temp_wav and os.path.exists(temp_wav):
            try:
                os.remove(temp_wav)
            except OSError:
                pass
