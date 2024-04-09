from flask import Flask, request, jsonify, make_response
from waitress import serve
import os
import time
import threading
import configparser
import logging
from datetime import datetime
from enum import Enum
from fs import open_fs, errors
import fnmatch
import requests
import re
from fs.sshfs import SSHFS
from urllib.parse import urlparse

# "const" vars
FILEHUB_CONF_FILENAME = "filehub.conf"
FILEHUB_CONF_ENV_VAR = "FILEHUB_CONF"
FILEHUB_DEBUG_ENV_VAR = "FILEHUB_DEBUG"
CONFIG_PROXY_PREFIX = "fh."
PROVIDER_URI_DELEMITER = "://"
LOCKING_FILE_EXTENSION = ".tmp"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
MI_POST_RESPONSE_RC = 201
ARCHIVE_FOLDER_NAME = "archive"
FILE_NAME_HEADER = "X-File-Name"

# vars
web_app = Flask(__name__)
filehub_configs = []
debug = os.environ.get(FILEHUB_DEBUG_ENV_VAR, "").lower() == "true"

# init
logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
if debug:
    logging.getLogger().setLevel(logging.DEBUG)


# classes
class FilehubConfigEntry:
     def __init__(self,name: str, config_section):
        self.Name = removePrefix(name)
        self.Type = config_section.get("Type")
        self.FileURI = config_section.get("FileURI")
        self.MISendURI = config_section.get("MISendURI")
        self.FileNamePattern = config_section.get("FileNamePattern")
        self.ContentType = config_section.get("ContentType")
        self.PollInterval = config_section.getint("PollInterval")
        self.Auth = config_section.get("Auth")
        self.ActionAfterProcess = config_section.get("ActionAfterProcess")
        self.ActionAfterFailure = config_section.get("ActionAfterFailure")
        self.MoveAfterProcess = config_section.get("MoveAfterProcess")
        self.MoveAfterProcessDatedArchive = config_section.get("MoveAfterProcessDatedArchive")
        self.MoveAfterFailure = config_section.get("MoveAfterFailure")
        self.Locking = config_section.getboolean("Locking")
        self.SFTPIdentities = config_section.get("SFTPIdentities")
        self.SFTPIdentityPassPhrase = config_section.get("SFTPIdentityPassPhrase")
        # additional fields when parsing
        self.URIType = getProviderType(self.FileURI)

        # set Enum if possible
        if self.ActionAfterFailure:
            self.ActionAfterFailure = ActionAfter(self.ActionAfterFailure.lower())
        if self.ActionAfterProcess:
            self.ActionAfterProcess = ActionAfter(self.ActionAfterProcess.lower())

        # correct bool if not set
        if not self.Locking == True:
            self.Locking = False
        if not self.MoveAfterProcessDatedArchive == True:
            self.MoveAfterProcessDatedArchive = False

        # check config valid based on type
        if self.Type.lower() == "in":
            # check fields
            if not self.FileURI or not self.MISendURI or not self.ContentType or not self.PollInterval or not self.FileNamePattern or not self.ActionAfterFailure or not self.ActionAfterProcess:
                raise Exception(f"invalid config for {name}, required fields: Type, FileURI, MISendURI, ContentType, POllInterval, FileNamePattern, ActionAfterFailure, ActionAfterProcess")
            # check send uri is http
            if not self.MISendURI.lower().startswith("http"):
                raise Exception(f"MISendURI is for sending to the Micro-Integrator. A http URI must be provided. Found in file {self.MISendURI}")
            # check auth provided
            if self.Auth:
                if self.Auth.lower() == "cert":
                    if not self.SFTPIdentities or not self.SFTPIdentityPassPhrase:
                        raise Exception("for certificate authentication, SFTPIdentities and SFTPIdentityPassPhrase is needed")
                else:
                    if not ":" in self.Auth.lower() or len(self.Auth.lower().split(":", 1)) == 2:
                        raise Exception(f"unable to get creadentials from Auth field. Expceted is username:password or 'cert'")
            # check action afters
            if self.ActionAfterProcess == ActionAfter.MOVE:
                if not self.MoveAfterProcess:
                    raise Exception("ActionAfterProcess is set to move, but no location is set! Please set MoveAfterProcess.")
            if self.ActionAfterFailure == ActionAfter.MOVE:
                if not self.MoveAfterFailure:
                    raise Exception("ActionAfterFailure is set to move, but no location is set! Please set MoveAfterFailure.")
        
        if self.Type.lower() == "out":
            # check fields
            if not self.FileURI:
                raise Exception(f"invalid config for {name}, required field: FileURI")
            # check send provider settings
            if self.URIType == URIType.FILE:
                logging.debug("dont need to check something here - only FileURI is needed")
            if self.URIType == URIType.SFTP:
                if self.Auth:
                    if self.Auth.lower() == "cert":
                        if not self.SFTPIdentities or not self.SFTPIdentityPassPhrase:
                            raise Exception("for certificate authentication, SFTPIdentities and SFTPIdentityPassPhrase is needed")
                    else:
                        if not ":" in self.Auth.lower() or len(self.Auth.lower().split(":", 1)) == 2:
                            raise Exception(f"unable to get creadentials from Auth field. Expceted is username:password or 'cert'")

        if self.Type.lower() not in ("in", "out"):
            raise Exception(f"type is not in nor out in config: {name}")

class ActionAfter(Enum):
    MOVE = "move"
    DELETE = "delete"
    NONE = "none"

class URIType(Enum):
    FILE = "file"
    SFTP = "sftp"


# helper functions
def removePrefix(s: str) -> str:
    if not s.startswith(CONFIG_PROXY_PREFIX):
        raise Exception(f"unknown section proxy: {s} - config entry must start with {CONFIG_PROXY_PREFIX}")
    return s[len(CONFIG_PROXY_PREFIX):]


@web_app.route('/health', methods=['GET'])
def health():
    response = {"Status": "UP"}
    return jsonify(response), 200

@web_app.route('/send', methods=['POST'])
def send():
    # get param
    profile_param = request.args.get("profile")
    if not profile_param:
        response = {"error": "missing profile param"}
        return jsonify(response), 400
    
    # check incoming request (files, body etc.)
    request_files_count = len(request.files)

    request_body_empty = request.data == b''
    filename_header = request.headers.get(FILE_NAME_HEADER)

    if request_files_count == 0 and request_body_empty:
        response = {"error": "no file sent"}
        return jsonify(response), 400
    
    if not request_body_empty and not filename_header:
        response = {"error": "no file name sent"}
        return jsonify(response), 400
    
    if request_files_count > 1:
        response = {"error": "only 1 file per request allowed"}
        return jsonify(response), 400
    
    if request_files_count > 0 and not request_body_empty:
        response = {"error": "file and body received"}
        return jsonify(response), 400
    
    # before consuming the file from request, check whether a profile is available
    
    # check for profile in config
    config_entry = [entry for entry in filehub_configs if entry.Name == profile_param and entry.Type.lower() == "out"]
    if len(config_entry) == 0:
        response = {"error": f"profile not found: {profile_param}"}
        return jsonify(response), 404

    # more than 1 should not be possible because the configParser throws an error during startup for duplicate names
    if len(config_entry) > 1:
        response = {"error": f"multiple profiles found with name: {profile_param}"}
        return jsonify(response), 404
    
    send_config = config_entry[0]

    msg, rc =handle_file_send(request, send_config)
    if msg == None:
        response = {"message": "transfer succeeded"}
        return jsonify(response), MI_POST_RESPONSE_RC
    response = {"error" : f"{msg}"}
    return jsonify(response), rc




def handle_file_send(request, config) -> (str, int):

    if config.URIType != URIType.FILE and config.URIType != URIType.SFTP:
        return "filesystem not supported", 500

    # get file from multipart
    if len(request.files) == 1:
        # we only allow 1 file so we iterate over the immutable request.files
        for key, value in request.files.items():
            file = request.files[key]
        try:
            uploaded_file_name = file.filename
            uploaded_file_data = file.read()
            if not uploaded_file_name or not uploaded_file_data or len(uploaded_file_name) == 0 or len(uploaded_file_data) == 0:
                raise Exception("unable to read request data")
        except Exception as e:
            return e, 500
    # get body if request (xml, json, csv, txt, etc.)
    else:

        # get content-type (not used, maybe handy)
        request_content_type = request.headers.get("Content-Type")
        if request_content_type and "charset=" in request_content_type.lower():
            match = re.search(r'charset=([^\s;]+)', request_content_type)
            if match:
                charset = match.group(1)

        # get filename & data (ignoring encoding because content is irrelevant --> file will be writen)
        uploaded_file_name = request.headers.get(FILE_NAME_HEADER)
        uploaded_file_data = request.data
    

    # define fs based on submitted URI
    fs = None
    additional_path = None
    if config.URIType == URIType.FILE:
        additional_path = ""
        uri_without_provider = get_uri_without_provider(config.FileURI)
        fs = open_fs(f"osfs://{uri_without_provider}")
    else:
        if config.Auth != None and config.Auth == "cert":
            # parse the uri to use SSHFS constructor
            parsed_uri = urlparse(config.FileURI)
            hostname = parsed_uri.hostname
            username = parsed_uri.username
            # password is used for either basic auth pw or to decrypt private key
            password = None
            if parsed_uri.password:
                password = parsed_uri.password
            if config.SFTPIdentityPassPhrase:
                password = config.SFTPIdentityPassPhrase
            # need to the additional path for different sftp handling witch basic or with cert
            if parsed_uri.path:
                additional_path = parsed_uri.path
            else:
                additional_path = ""
            fs = SSHFS(host=hostname, user=username, passwd=password, pkey=config.SFTPIdentities)
        else:
            # parse the uri for additional path which is no automatically submitted when using open_fs
            parsed_uri = urlparse(config.FileURI)
            hostname = parsed_uri.hostname
            username = parsed_uri.username
            password = parsed_uri.password
            if parsed_uri.path:
                additional_path = parsed_uri.path
            else:
                additional_path = ""
            fs = SSHFS(host=hostname, user=username, passwd=password)

    # write file to destination
    try:

        # determine target file name based on lock
        if config.Locking:
            file_path = f"{additional_path}/{uploaded_file_name}{LOCKING_FILE_EXTENSION}"
            file_path_no_lock = f"{additional_path}/{uploaded_file_name}"

            # check if there is already a file locked with this name
            if fs.exists(file_path):
                fs.close()
                raise Exception(f"lock file already exists: {file_path}")
        else:
            file_path = f"{additional_path}/{uploaded_file_name}"
        
        # write file (we dont care about encoding, since we write the same bytes as received)

        with fs.open(file_path, "wb") as file:
            file.write(uploaded_file_data)
            file.flush()

        # if locking, release lock on file
        if config.Locking:
            # rename file
            fs.move(file_path, file_path_no_lock, True)
            # after renaming, overwrite variable for check
            file_path = file_path_no_lock

        # check existence of file
        if not fs.exists(file_path):
            logging.error(f"unable to write file {file_path}")

        # close fs
        fs.close()

        # log file transfer
        logging.info(f"Successfully transfered file {uploaded_file_name} of config {config.Name}")
    except Exception as e:
        return e, 500

    # all good
    return None, 201

def get_uri_without_provider(uri) -> str:
    return uri.split("://")[1]

def start_listener():

    # check IN configs
    has_in_config = False
    in_config_count = 0
    for filehub_config in filehub_configs:
        if not filehub_config.Type == None:
            if filehub_config.Type.lower() == "in":
                has_in_config = True
                in_config_count =+ 1
                in_config = filehub_config


    # handle more than 1 listerner (which is not supported)
    if in_config_count != 1:
        raise Exception("multiple IN configs defined, only 1 In config is supported!")

    # check listener needed
    if has_in_config:
        listener_thread = threading.Thread(target=listener, args=(in_config,))
        listener_thread.start()
    else: 
        logging.info("no IN-Config defined. No listener will start...")

def getProviderType(provider) -> URIType:
    if not PROVIDER_URI_DELEMITER in provider:
        raise Exception(f"invalid url provided: {provider}")
    
    uri_provider_prefix = provider.split(PROVIDER_URI_DELEMITER)[0]
    return URIType(uri_provider_prefix)
    
def listener(config):

    logging.info(f"Starting listener for {config.Name}")

    # start listeining
    handle_file_listen(config)
    
def handle_file_listen(config):

    uri_without_provider = get_uri_without_provider(config.FileURI)

    # define fs for listening
    fs = None

    # run polling
    while True:

        # current timestamp
        before_polling_timestamp = current_milli_time()

        if fs == None:
            additional_path = None
            if config.URIType == URIType.FILE:
                uri_without_provider = get_uri_without_provider(config.FileURI)
                fs = open_fs(f"osfs://{uri_without_provider}")
            else:
                if config.Auth != None and config.Auth == "cert":
                    # parse the uri to use SSHFS constructor
                    parsed_uri = urlparse(config.FileURI)
                    hostname = parsed_uri.hostname
                    username = parsed_uri.username
                    # password is used for either basic auth pw or to decrypt private key
                    password = None
                    if parsed_uri.password:
                        password = parsed_uri.password
                    if config.SFTPIdentityPassPhrase:
                        password = config.SFTPIdentityPassPhrase
                    # need to the additional path for different sftp handling witch basic or with cert
                    if parsed_uri.path:
                        additional_path = parsed_uri.path
                    else:
                        additional_path = ""
                    fs = SSHFS(host=hostname, user=username, passwd=password, pkey=config.SFTPIdentities)
                else:
                    additional_path = ""
                    fs = open_fs(config.FileURI)

        # read files from location
        try:

            # define lookup params
            folder = None
            if additional_path:
                folder = additional_path
            else: 
                folder = "/"

            pattern = config.FileNamePattern

            # get files from folder
            folder_entries = fs.scandir(folder)
            matching_files = [entry.name for entry in folder_entries if fnmatch.fnmatch(entry.name, pattern)]

            print(f"found files: {len(matching_files)}")
            
            #loop through the files
            for file_name in matching_files:
                # preserve original file name
                original_file_name = file_name
                # add additional path for file name if required
                if additional_path:
                    file_name = additional_path + "/" + file_name
                # check if file is locked
                if config.Locking:
                    if fs.exists(f"{file_name}{LOCKING_FILE_EXTENSION}"):
                        continue
                    #lock file
                    fs.move(f"{file_name}", f"{file_name}{LOCKING_FILE_EXTENSION}")
                    file_name = f"{file_name}{LOCKING_FILE_EXTENSION}"

                # read file - always as byte array, encoding is not interpreted
                with fs.open(f"{file_name}", "rb") as file:
                    file_content = file.read()

                # send file to mi
                # create files for request
                files = {'file': (original_file_name, file_content)}
                # request
                request_has_failed = False
                error = None
                try:
                    # set content type header
                    headers = {
                        FILE_NAME_HEADER : original_file_name,
                        "Content-Type" : config.ContentType
                    }
                    response = requests.post(config.MISendURI, data=file_content, headers=headers)
                except Exception as e:
                    logging.error(f"unable to send to MI: {e}")
                    request_has_failed = True
                    error = e
                    response = None

                # handle move after error
                if request_has_failed or response.status_code != MI_POST_RESPONSE_RC:
                    logging.error(f"unable to send file to {config.MISendURI}")
                    if request_has_failed:
                        logging.error(f"request has failed! Error: {error}")
                    if response.status_code != MI_POST_RESPONSE_RC:
                        logging.error(f"response code: {response.status_code}")

                    # handle failure
                    # action none
                    if config.ActionAfterFailure == ActionAfter.NONE:
                        # if locking is applied, remove lock
                        if config.Locking:
                            fs.move(file_name, original_file_name)
                        continue
                    
                    # action delete
                    if config.ActionAfterFailure == ActionAfter.DELETE:
                        logging.error(f"ActionAfterFailure is set to delete: deleting file: {original_file_name}")
                        fs.remove(file_name)
                        continue

                    # action move
                    if config.ActionAfterFailure == ActionAfter.MOVE:
                        fail_folder = f"{config.MoveAfterFailure}"
                        try:
                            fs.getinfo(fail_folder, namespaces=['basic'])
                        except errors.ResourceNotFound:
                            fs.makedirs(fail_folder)
                        # move to error folder (and remove locking extension if needed)
                        target_file_name = f"{fail_folder}/{datetime.now().strftime('%Y%m%d_%H%M%S%f')[:-2]}_{original_file_name}"
                        logging.error(f"ActionAfterFailure is set to move: moving file {additional_path}/{original_file_name} to {target_file_name}")
                        fs.move(file_name, target_file_name, True)

                        # handle next file - move after process is irrelevant because this file has failed
                        continue


                # handle action after process
                if config.ActionAfterProcess:
                    # action none
                    if config.ActionAfterProcess == ActionAfter.NONE:
                        logging.info(f"ActionAfterProcess is set to none - leaving file: {original_file_name}")
                        if config.ActionAfterProcess == ActionAfter.NONE:
                            if config.Locking:
                                fs.move(file_name, original_file_name)
                        logging.info(f"Successfully transfered file: {original_file_name}")
                        continue
                    
                    # action delete
                    if config.ActionAfterProcess == ActionAfter.DELETE:
                        logging.info(f"ActionAfterProcess is set to delete: deleting file: {original_file_name}")
                        fs.remove(file_name)
                        logging.info(f"Successfully transfered file: {original_file_name}")
                        continue

                    # action move
                    if config.ActionAfterProcess == ActionAfter.MOVE:
                        move_folder = f"{additional_path}/{config.MoveAfterProcess}"
                        # create archive folder
                        create_folder(fs, move_folder)
                        target_folder = move_folder

                        # handle dated archive
                        if config.MoveAfterProcessDatedArchive:
                            # update target folder if must be dated
                            target_folder = f"{target_folder}/{get_dated_folder_name()}"

                        # set file target name with timestamp
                        file_name_with_timestamp=f"{datetime.now().strftime('%Y%m%d_%H%M%S%f')[:-2]}_{original_file_name}"
                        target_folder_file_name = f"{target_folder}/{file_name_with_timestamp}"

                        # if file was locked, remove lock in target
                        if config.Locking:
                            target_folder_file_name = f"{target_folder}/{original_file_name}"

                        # move the file to the target location
                        logging.info(f"ActionAfterProcess is set to move: moving file {original_file_name} to {target_folder_file_name}")
                        fs.move(file_name, target_folder_file_name)
                        
                        logging.info(f"Successfully transfered file: {original_file_name}")
                    
        except Exception as e:
            logging.error(f"Exception during file access: {e}")

        print("sleep...")
            
        # calc sleep to match poll interval
        after_polling_timestamp = current_milli_time()
        delta_sleep = (config.PollInterval*1000) - (after_polling_timestamp - before_polling_timestamp)
        if delta_sleep > 0:
            # millis / 1000 for sec as defined in param
            time.sleep(delta_sleep / 1000)

# create folder if it does not exist
def create_folder(fs, folder_name):
    try:
        fs.getinfo(folder_name, namespaces=['basic'])
    except errors.ResourceNotFound:
        fs.makedirs(folder_name)
        
def current_milli_time():
    return round(time.time() * 1000)

def get_dated_folder_name():
    return datetime.now().strftime('%Y-%m-%d')

def check_conf_file():
    conf_file = os.getenv(FILEHUB_CONF_ENV_VAR)
    if conf_file:
        if not os.path.exists(conf_file):
            raise Exception(f"defined conf file {conf_file} does not exist!")
    else:
        if os.path.exists(f"/{FILEHUB_CONF_FILENAME}"):
            conf_file = "/" + FILEHUB_CONF_FILENAME
        else:
            raise Exception(f"no filehub config defined. Use either environment variable {FILEHUB_CONF_ENV_VAR} or put a {FILEHUB_CONF_FILENAME} in the root folder")

    config = configparser.ConfigParser()

    try:
        config.read(conf_file)
    except configparser.Error as e:
        raise Exception(f"unable to read config file: {e}")
    
    for section_name in config.sections():
        filehub_config_entry = FilehubConfigEntry(name=section_name, config_section=config[section_name])
        filehub_configs.append(filehub_config_entry)

    if len(filehub_configs) < 1:
        raise Exception("at least one config entry is needed for the filehub service!")

    logging.info(f"Config file {conf_file} read successfully! found {len(filehub_configs)} config entries")
    if debug:
        logging.debug("---------------------------------------------------------")

        for filehub_config in filehub_configs:
            logging.debug(f"Name: {filehub_config.Name}")
            logging.debug(f"Type: {filehub_config.Type}")
            logging.debug(f"FileURI: {filehub_config.FileURI}")
            logging.debug(f"MISendURI: {filehub_config.FileURI}")
            logging.debug(f"FileNamePattern: {filehub_config.FileNamePattern}")
            logging.debug(f"ContentType: {filehub_config.ContentType}")
            logging.debug(f"PollInterval: {filehub_config.PollInterval}")
            logging.debug(f"Auth: {filehub_config.Auth}")
            logging.debug(f"MoveAfterProcess: {filehub_config.MoveAfterProcess}")
            logging.debug(f"MoveAfterProcessDatedArchive: {filehub_config.MoveAfterProcessDatedArchive}")
            logging.debug(f"MoveAfterFailure: {filehub_config.MoveAfterFailure}")
            logging.debug(f"Locking: {filehub_config.Locking}")
            logging.debug(f"SFTPIdentities: {filehub_config.SFTPIdentities}")
            logging.debug(f"SFTPIdentityPassPhrase: {filehub_config.SFTPIdentityPassPhrase}")
            logging.debug("---------------------------------------------------------")

if __name__ == '__main__':

    logging.info("Starting filehub microservice...")

    # check config file is okl
    check_conf_file()

    # run the process thread
    start_listener()

    # start webserver
    logging.info("Starting Web-Server")
    serve(web_app, host="0.0.0.0", port=5000)
    