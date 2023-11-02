import datetime
import glob
import os
import re
import subprocess
import sys
import csv
import hashlib


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

    def write_to_file(self) -> bool:
        with open(
            file=f".\\{self.PATH}\\{self.NAME}.csv",
            mode="w",
            newline="",
            encoding="utf-8",
        ) as f:
            writer = csv.DictWriter(f=f, fieldnames=self.HEADERS)
            writer.writeheader()
            writer.writerows(self.ROWS)


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
    unnestdicts: bool = True,
    unnestsimplelists: bool = True,
) -> DocumentManager:
    if docman.PRIMARY_KEY is None or docman.PRIMARY_DOCUMENT is None:
        return docman
    if subdocpath is None:
        isprimary = True
        docname = docman.PRIMARY_DOCUMENT.NAME
        workingdoc = docman.PRIMARY_DOCUMENT
        # foreignkeylabel = None
        localkeylabel = None
    else:
        keyhash = hashlib.sha256(str(subj).encode()).hexdigest()
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
        subj[foreignkeylabel] = foreignkey
        subj[localkeylabel] = keyhash

    dclone = subj.copy()
    for key in dclone:
        if isinstance(dclone[key], dict) and unnestdicts:
            cursdpath = f"{subdocpath}.{key}" if subdocpath is not None else key
            keylabeltopass = (
                f"{docman.PRIMARY_DOCUMENT.NAME}_{docman.PRIMARY_KEY}"
                if isprimary
                else localkeylabel
            )
            keytopass = dclone[docman.PRIMARY_KEY] if isprimary else subj[localkeylabel]
            docman = unnest_to_csv(
                docman=docman,
                subj=dclone[key],
                subdocpath=cursdpath,
                foreignkeylabel=keylabeltopass,
                foreignkey=keytopass,
            )
            subj.pop(key)
        if isinstance(dclone[key], list):
            cleanlist = []
            for el in dclone[key]:
                if isinstance(el, dict) and unnestdicts:
                    cursdpath = f"{subdocpath}.{key}" if subdocpath is not None else key
                    keylabeltopass = (
                        f"{docman.PRIMARY_DOCUMENT.NAME}_{docman.PRIMARY_KEY}"
                        if isprimary
                        else localkeylabel
                    )
                    keytopass = (
                        dclone[docman.PRIMARY_KEY] if isprimary else subj[localkeylabel]
                    )
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
                    keytopass = (
                        dclone[docman.PRIMARY_KEY] if isprimary else subj[localkeylabel]
                    )
                    docman = unnest_to_csv(
                        docman=docman,
                        subj={"value": el},
                        subdocpath=cursdpath,
                        foreignkeylabel=keylabeltopass,
                        foreignkey=keytopass,
                    )
                else:
                    cleanlist.append(el)
            if cleanlist == []:
                subj.pop(key)
            else:
                subj[key] = cleanlist
    workingdoc.HEADERS = __getheaders(
        subj=subj,
        existing_headers=workingdoc.HEADERS,
        foreign_key_label=foreignkeylabel,
    )
    workingdoc.ROWS.append(subj)
    if isprimary:
        docman.PRIMARY_DOCUMENT = workingdoc
    else:
        docman.SUB_DOCUMENTS[wdidx] = workingdoc
    return docman


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


def run(
    env_file_path: str = ".\\.env",
    requirements_file_path=".\\requirements.txt",
    main_file_path=".\\main.py",
) -> None:
    ENVIRONMENT_FILE = env_file_path
    REQUIREMENTS_FILE = requirements_file_path
    MAIN_FILE = main_file_path

    run_log = Log(
        file_name=f"{os.path.split(os.getcwd())[1]}_run",
        datetime_in_filename=False,
        append_existing_file=True,
    )

    # Open environment file.
    with open(file=ENVIRONMENT_FILE, mode="+r") as env:
        lines = env.readlines()
    run_log.info("Loaded environment variable file.")

    # Load environment variables.
    for line in lines:
        if re.match(r"^\#.*", string=line) is not None:
            continue
        env_var_parse = re.match(r"^([^=]*)\=\s*[\"\']?([^\"\']*)[\"\']?$", line)
        if env_var_parse is not None:
            run_log.info("Found an environment variable.")
            try:
                os.environ[env_var_parse.group(1).strip()] = env_var_parse.group(
                    2
                ).strip()
                run_log.info(
                    f'Env Label: "{env_var_parse.group(1).strip()}", Env Value: "{env_var_parse.group(2).strip() }"'
                )
            except:
                run_log.error(f'Error parsing variable set: "{line}"')

    run_log.info(f"Current Working Directory is: {os.curdir}")
    run_log.info(
        f"Attempting to install packages from '{REQUIREMENTS_FILE}' using 'pip'"
    )
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-r", REQUIREMENTS_FILE],
        )
    except:
        run_log.error("Failed during 'pip' command execution!")
        run_log.error(
            f"ERROR {sys.exc_info()[0]}: {sys.exc_info()[1]} on line {sys.exc_info()[2].tb_lineno}"
        )
        exit(1)
    run_log.info(f"Successfully installed packages from '{REQUIREMENTS_FILE}'")
    run_log.info(f"Attempting to execute '{MAIN_FILE}'")
    try:
        subprocess.check_call([sys.executable, f"{MAIN_FILE}"])
    except subprocess.CalledProcessError as cpe:
        run_log.error("Got non-zero exit code.")
        run_log.error(f"Code {cpe.returncode}: {cpe.output}")
    run_log.info("Execution completed.")
