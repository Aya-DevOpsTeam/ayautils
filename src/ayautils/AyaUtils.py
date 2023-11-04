import csv
import datetime
import glob
import hashlib
import os
import re
import subprocess
import sys

from pandas import DataFrame, read_csv


class AccessMode:
    OVERWRITE = "w"
    APPEND = "a"
    ONLY_NEW = "x"


class Log:
    OUTPUT_DIR = "output"

    def __init__(
        self,
        file_name: str,
        datetime_in_filename: bool = True,
        datetime_format_str: str = "%Y%m%d-%H%M%S",
        append_existing_file: bool = False,
    ) -> None:
        os.makedirs(name=self.OUTPUT_DIR, exist_ok=True)
        TIMESTAMP = datetime.datetime.now()
        self.LOG_FILE = f"{file_name}"
        if datetime_in_filename:
            self.LOG_FILE = self.LOG_FILE + TIMESTAMP.strftime(datetime_format_str)
        else:
            self.LOG_FILE = file_name
        if (not append_existing_file) and os.path.isfile(
            f"{self.OUTPUT_DIR}/{self.LOG_FILE}.log"
        ):
            suffix = len(
                glob.glob(pathname=f"{self.OUTPUT_DIR}/{self.LOG_FILE}__*.log")
            )
            self.LOG_FILE = f"{self.LOG_FILE}__{suffix+1}"
        self.LOG_FILE = self.LOG_FILE + ".log"
        with open(
            file=f"{self.OUTPUT_DIR}/{self.LOG_FILE}",
            mode=AccessMode.APPEND,
        ):
            pass

    def info(self, message):
        output = f"[INFO] @ {datetime.datetime.now()}: {message}"
        print(output)
        with open(f"{self.OUTPUT_DIR}/{self.LOG_FILE}", AccessMode.APPEND) as log_file:
            log_file.write(f"{output}\n")

    def warning(self, message):
        output = f"[WARNING] @ {datetime.datetime.now()}: {message}"
        print(output)
        with open(f"{self.OUTPUT_DIR}/{self.LOG_FILE}", AccessMode.APPEND) as log_file:
            log_file.write(f"{output}\n")

    def error(self, message):
        output = f"[ERROR] @ {datetime.datetime.now()}: {message}"
        print(output)
        with open(f"{self.OUTPUT_DIR}/{self.LOG_FILE}", AccessMode.APPEND) as log_file:
            log_file.write(f"{output}\n")


class CsvDocument:
    ROWS: list[dict]
    HEADERS: list
    PATH: str
    NAME: str

    def __init__(self, path: str, name: str) -> None:
        self.ROWS = []
        self.HEADERS = []
        self.PATH = path
        self.NAME = name

    def write_to_file(self, include_parquet: bool = False) -> bool:
        os.makedirs(name=self.PATH, exist_ok=True)
        with open(
            file=f"{self.PATH}\\{self.NAME}.csv",
            mode="w",
            newline="",
            encoding="utf-8",
        ) as f:
            writer = csv.DictWriter(f=f, fieldnames=self.HEADERS)
            writer.writeheader()
            writer.writerows(self.ROWS)
        if include_parquet:
            try:
                df = read_csv(filepath_or_buffer=f"{self.PATH}\\{self.NAME}.csv")
                df.to_parquet(path=f"{self.PATH}\\{self.NAME}.parquet")
            except:
                print("Fail state")
            pass


class DocumentManager:
    SUB_DOCUMENTS: list[CsvDocument]

    def __init__(self, primary_key: str, primary_document: CsvDocument) -> None:
        self.PRIMARY_KEY = primary_key
        self.PRIMARY_DOCUMENT = primary_document
        self.SUB_DOCUMENTS = []


def unnest_to_csv(
    docman: DocumentManager,
    subj: dict,
    subdockeylabel: str = "key",
    subdocpath: str = None,
    foreignkeylabel: str = None,
    foreignkey=None,
    unnestdicts: bool = False,
    unnestsimplelists: bool = True,
) -> DocumentManager:
    if docman.PRIMARY_KEY is None or docman.PRIMARY_DOCUMENT is None:
        return docman
    mutable_subj = subj.copy()
    if subdocpath is None:
        isprimary = True
        docname = docman.PRIMARY_DOCUMENT.NAME
        workingdoc = docman.PRIMARY_DOCUMENT
        # foreignkeylabel = None
        localkeylabel = None
    else:
        keyhash = hashlib.sha256(str(mutable_subj).encode()).hexdigest()
        isprimary = False
        docname = f"{docman.PRIMARY_DOCUMENT.NAME}.{subdocpath}"
        wdidx = __getindexbyname(docman=docman, name=docname)
        if wdidx is None:
            docman.SUB_DOCUMENTS.append(
                CsvDocument(docman.PRIMARY_DOCUMENT.PATH, docname)
            )
            wdidx = len(docman.SUB_DOCUMENTS) - 1
        workingdoc = docman.SUB_DOCUMENTS[wdidx]
        # foreignkeylabel = f"{docman.PRIMARY_DOCUMENT.NAME}_{docman.PRIMARY_KEY}"
        localkeylabel = f"{subdocpath}_{subdockeylabel}"
        mutable_subj[foreignkeylabel] = foreignkey
        mutable_subj[localkeylabel] = keyhash

    dclone = mutable_subj.copy()
    top_level_key_value = dclone[docman.PRIMARY_KEY] if isprimary else None
    for key in dclone:
        if isinstance(dclone[key], dict) and unnestdicts:
            cursdpath = f"{subdocpath}.{key}" if subdocpath is not None else key
            keylabeltopass = (
                f"{docman.PRIMARY_DOCUMENT.NAME}_{docman.PRIMARY_KEY}"
                if isprimary
                else localkeylabel
            )
            keytopass = (
                dclone[docman.PRIMARY_KEY] if isprimary else mutable_subj[localkeylabel]
            )
            docman = unnest_to_csv(
                docman=docman,
                subj=dclone[key],
                subdocpath=cursdpath,
                foreignkeylabel=keylabeltopass,
                foreignkey=keytopass,
            )
            mutable_subj.pop(key)
        if isinstance(dclone[key], dict):
            __dictprocessor(
                docman=docman,
                subj=dclone[key],
                pkey=key,
                parent=mutable_subj,
                localkeylabel=localkeylabel,
                primarykey=top_level_key_value,
                subdocpath=subdocpath,
                isprimary=isprimary,
                unnestsimplelists=unnestsimplelists,
            )
            mutable_subj.pop(key)
        if isinstance(dclone[key], list):
            cleanlist = __listprocessor(
                listsubj=dclone[key],
                docman=docman,
                key=key,
                localkeylabel=localkeylabel,
                primarykey=top_level_key_value,
                mutable_subj=mutable_subj,
                subdocpath=subdocpath,
                isprimary=isprimary,
                unnestsimplelists=unnestsimplelists,
            )
            if cleanlist == []:
                mutable_subj.pop(key)
            else:
                mutable_subj[key] = cleanlist
    workingdoc.HEADERS = __getheaders(
        subj=mutable_subj,
        existing_headers=workingdoc.HEADERS,
        foreign_key_label=foreignkeylabel,
    )
    workingdoc.ROWS.append(mutable_subj)
    if isprimary:
        docman.PRIMARY_DOCUMENT = workingdoc
    else:
        docman.SUB_DOCUMENTS[wdidx] = workingdoc
    return docman


def __dictprocessor(
    docman: DocumentManager,
    subj: dict,
    pkey: str,
    parent: dict,
    localkeylabel: str,
    primarykey: any,
    subdocpath: str,
    isprimary: bool,
    unnestsimplelists: bool,
):
    for dkey in subj:
        flat_key = f"{pkey}_{dkey}"
        if isinstance(subj[dkey], dict):
            __dictprocessor(
                docman=docman,
                subj=subj[dkey],
                pkey=flat_key,
                parent=parent,
                localkeylabel=localkeylabel,
                primarykey=primarykey,
                subdocpath=subdocpath,
                isprimary=isprimary,
                unnestsimplelists=unnestsimplelists,
            )
            continue
        parent[flat_key] = subj[dkey]
        if isinstance(subj[dkey], list):
            cleanlist = __listprocessor(
                listsubj=parent[flat_key],
                docman=docman,
                key=flat_key,
                localkeylabel=localkeylabel,
                primarykey=primarykey,
                mutable_subj=parent,
                subdocpath=subdocpath,
                isprimary=isprimary,
                unnestsimplelists=unnestsimplelists,
            )
            if cleanlist == []:
                parent.pop(flat_key)
            else:
                parent[flat_key] = cleanlist


def __listprocessor(
    listsubj: list,
    docman: DocumentManager,
    key: str,
    localkeylabel: str,
    primarykey: str,
    mutable_subj: dict,
    subdocpath: str,
    isprimary: bool,
    unnestsimplelists: bool,
) -> list:
    cleanlist = []
    for el in listsubj:
        if isinstance(el, dict):
            cursdpath = f"{subdocpath}.{key}" if subdocpath is not None else key
            keylabeltopass = (
                f"{docman.PRIMARY_DOCUMENT.NAME}_{docman.PRIMARY_KEY}"
                if isprimary
                else localkeylabel
            )
            keytopass = primarykey if isprimary else mutable_subj[localkeylabel]
            docman = unnest_to_csv(
                docman=docman,
                subj=el,
                subdocpath=cursdpath,
                foreignkeylabel=keylabeltopass,
                foreignkey=keytopass,
            )
        elif unnestsimplelists:
            cursdpath = f"{subdocpath}.{key}" if subdocpath is not None else key
            keylabeltopass = (
                f"{docman.PRIMARY_DOCUMENT.NAME}_{docman.PRIMARY_KEY}"
                if isprimary
                else localkeylabel
            )
            keytopass = primarykey if isprimary else mutable_subj[localkeylabel]
            docman = unnest_to_csv(
                docman=docman,
                subj={"value": el},
                subdocpath=cursdpath,
                foreignkeylabel=keylabeltopass,
                foreignkey=keytopass,
            )
        else:
            cleanlist.append(el)
    return cleanlist


def __getindexbyname(docman: DocumentManager, name: str) -> int:
    if docman.PRIMARY_DOCUMENT.NAME == name:
        return -1
    for idx, doc in enumerate(docman.SUB_DOCUMENTS):
        if doc.NAME == name:
            return idx
    return None


def __getheaders(
    subj: dict,
    existing_headers: list[str] = [],
    foreign_key_label: str = None,
    local_key_label: str = None,
) -> list[str]:
    if foreign_key_label is not None and foreign_key_label not in existing_headers:
        existing_headers.append(foreign_key_label)
    if local_key_label is not None and local_key_label not in existing_headers:
        existing_headers.append(local_key_label)
    for key in subj.keys():
        if key not in existing_headers:
            existing_headers.append(key)
    return existing_headers
