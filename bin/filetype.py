#!/usr/bin/env python3

import sys
import csv
import json
import magic
import mimetypes
from chardet.universaldetector import UniversalDetector
import PyPDF2
import zipfile


detector = UniversalDetector()
m = magic.Magic(uncompress=False)


def detect_charset(path):
    detector.reset()
    for buf in open(path, 'rb'):
        detector.feed(buf)
        #if detector.done:
        break
    detector.close()
    return detector.result["encoding"]


def is_pdf(path):
    try:
        PyPDF2.PdfFileReader(open(path, "rb"))
    except PyPDF2.utils.PdfReadError:
        return "invalid"

    return ""


def is_json(path):
    # very slow ..
    try:
        data = json.load(open(path))
        return True
    except:
        return False


for path in sys.argv[1:]:
    fm = m.from_file(path)
    suffix = fm

    if zipfile.is_zipfile(path):
        iszip = ".zip"
    else:
        iszip = ""

    charset = ""
    if fm.startswith("PDF") and is_pdf(path):
        suffix = ".pdf"
    elif fm in ["Microsoft Excel 2007+"]:
        suffix = ".xlsx"
        # TBD: detect if contains macros, and is ".xslm" ..
    else:
        if is_json(path):
            suffix = ".json"

    print(path, suffix, iszip, charset)
