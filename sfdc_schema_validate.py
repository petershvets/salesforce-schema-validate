import requests
import json
from collections import OrderedDict
import os

#************* Functions, Classes

# Logging
#Log levels
_MESSAGE = 0
_INFO = 1
_DEBUG = 2
_EXTRA = 3
_WARNING = 5
_ERROR = -1
_DEBUG_LEVEL = 2

_DEBUG_CONF_LEVEL = {
    "DEBUG": _DEBUG,
    "EXTRA": _EXTRA,
    "NORMAL": _INFO
}

_LOGGER = None

_MESSAGE_TEXT = {
    _MESSAGE: "",
    _INFO: "INFO: ",
    _ERROR: "ERROR: ",
    _EXTRA: "DEBUG: ",
    _DEBUG: "DEBUG: ",
    _WARNING: "WARNING "
}

def debug (msg, level = _MESSAGE, json_flag = False):

    global _LOGGER

    if level <= _DEBUG_LEVEL :
        if json_flag:
            log_msg = json.dumps(msg, indent=4, separators=(',', ': '))
            print(log_msg)
            if _LOGGER:
                _LOGGER.info(log_msg)
        else:
            print(_MESSAGE_TEXT[level]+str(msg))
            if _LOGGER:
                _LOGGER.info(_MESSAGE_TEXT[level]+str(msg))
    return None


def get_json_prop(in_json_file):
	#json_file_content_str = open(in_json_file, encoding='utf-8').read()
	json_file_content_str = open(in_json_file, encoding='utf-8')
	try:
		#out_json_properties = json.loads(json_file_content_str, encoding='utf-8', object_pairs_hook=OrderedDict)
		out_json_properties = json.load(json_file_content_str, encoding='utf-8', object_pairs_hook=OrderedDict)
		return out_json_properties
	except json.decoder.JSONDecodeError as json_err:
		debug("Provided config file {} is not valid JSON document".format(in_json_file), _ERROR)
		debug(json_err, _ERROR)
		raise
#************************************
def get_token(in_prop_file):
	""" Args:
	"""
	conn = get_json_prop(in_prop_file)
	payload = {'grant_type': 'password',
           'client_id': conn["consumer_key"],
           'client_secret': conn["consumer_secret"],
           'username': conn["username"],
           'password': conn["password"]
          }	
	r = requests.post(token_uri, headers={"Content-Type":"application/x-www-form-urlencoded"}, data=payload)

#*************************************
def run_restapi(in_token, in_instance_url, in_uri):
        """Args:

        """
        url = in_instance_url + in_uri
        r = requests.get(url, headers = {"Authorization":"Bearer " + in_token});
        response_json = r.json()
        return response_json


def get_files(directory, fpath = False):
    """
    Args:
        directory - starting directory to list files
        fpath - flag indicates whether to return file path and name or just name
            False - returns only filename
            True  - returns fully qualified filename: path/filename
        Function generates the file names in a directory
        tree by walking the tree either top-down or bottom-up. For each
        directory in the tree rooted at directory top (including top itself),
        it yields a 3-tuple (dirpath, dirnames, filenames).
    """
    file_paths = []  # List stores filenames with or without path.

    # Walk the tree.
    for root, directories, files in os.walk(directory):
        for filename in files:
            if fpath:
                # Join the two strings in order to form the full filepath.
                filepath = os.path.join(root, filename)
                file_paths.append(filepath)
            else:
                file_paths.append(filename)

    return file_paths

#######################################
# Define REST API URI
token_uri="https://login.salesforce.com/services/oauth2/token"
limits_uri="/services/data/v36.0/limits"
versions_uri = "/services/data/"
list_of_resources = versions_uri + "v38.0"
list_of_objects_uri = "/services/data/v37.0/sobjects/"
#**************************************

# Establish connectivity
# TBD - Convert to class
# TBD - Check User-agent flow, used by applications that cannot securely store the consumer secret.

#consumer_key = '3MVG9szVa2RxsqBbUzW_tP7Kx46qui1Wi_cCozQJQv6kTqv0pm3WmJf6j_aj2B8Wy_KdfGojVvobTSbrre1mu'
#consumer_secret = '4963787064368000765'
#username = 'dataintelcpq1@modeln.com'
#password = 'modeln@123WU2S7kBO1uiUREPvvLtt5umw'

# Process connectivity credentials
conn = get_json_prop('sfdc_conn_prop.json')

# TBD - Convert to function resturning response body
# Construct payload
payload = {'grant_type': 'password',
           'client_id': conn["consumer_key"],
           'client_secret': conn["consumer_secret"],
           'username': conn["username"],
           'password': conn["password"]
          }
# Construct request for token. This is the first step in establishing connection to SFDC
# Token received is used by all consequtive REST API calls

r = requests.post(token_uri, headers={"Content-Type":"application/x-www-form-urlencoded"}, data=payload)

# Convert stream of bytes into json formatted list
body = r.json()
#***********************

token = body["access_token"]
#print ("Access token {}".format(token))

instance_url = body["instance_url"]
#print ("Instance URL: {}".format(instance_url))
#*************** end of getting token and url ******************************

# Retrieve URI versions
url = instance_url+versions_uri
r = requests.get(url, headers = {"Authorization":"Bearer " + token});
#print(type(r.json()))
#for list_iter in r.json():
#	print ("Versions: {}".format(list_iter))

# Get a List of Resources
#url = instance_url + list_of_resources
#r = requests.get(url, headers = {"Authorization":"Bearer " + token});
#for list_iter in r.json():
#	print("Resource: {}".format(list_iter))

# List of Objects
#url = instance_url+list_of_objects_uri
#r = requests.get(url, headers = {"Authorization":"Bearer " + token});
# Dump result into json file
#obj_lst = r.json()
#with open('list_of_objects.json', 'w') as f:
#	json.dump(obj_lst, f)
#print (type(obj_lst))

#objects_list = r.json()["sobjects"]
#print objects_list["sobjects"]
#print (type(objects_list))

#for dictList in objects_list:
#	print ("Object name: {}".format(dictList["name"]))
#	print("Describe URI: {}".format(dictList["urls"]["describe"]))


# Describe Object
#url = instance_url + '/services/data/v37.0/sobjects/REVVY__MnGeography__c/describe'
url = instance_url + "/services/data/v37.0/sobjects/Account/describe" 
r = requests.get(url, headers = {"Authorization":"Bearer " + token});
obj_desc = r.json()
#with open('obj_desc.json', 'w') as f:
#	json.dump(obj_desc, f)

#for dictIter in obj_desc["fields"]:
#	print("Attribute: name:{} type:{} precision:{} scale:{}".format(dictIter["name"], dictIter["type"], dictIter["precision"], dictIter["scale"]))

# Compile a list of objects we need to verify and validate
os.environ['REF_DATA'] = os.path.expanduser('/home/pshvets/sfdc_validation_app/ref_data')
_REF_DATA = os.environ['REF_DATA']

obj_verify = get_files(_REF_DATA)
print ("Objects will be verified: {}".format(obj_verify))

obj_name = [line.split('.')[0] for line in obj_verify]
#for line in obj_name:
#	print ("Objects to be verified: {}".format(list(obj_name)))

#************
# Loop through all objects and get each object columns with datatypes
# Datatypes are Salesforce specific, need to build mapping between
# SFDC and Informatica
obj_lst=run_restapi(token, instance_url, list_of_objects_uri)["sobjects"]
comp_data = set()
for dictList in obj_lst:
	#print (type( dictList["name"]))
	if dictList["name"] not in obj_name:continue 
#	if dictList["name"] != "Account":continue	
	obj_name = dictList["name"]

	#Open reference metafile and load content into a set for comparison
	with open('/home/pshvets/sfdc_validation_app/ref_data/'+obj_name+'.csv', 'r') as ref_file:
#	with open(obj_name+'.csv', 'r') as ref_file:
		ref_lines = [word.strip() for word in ref_file]
		ref_data = set(ref_lines)


	print ("Validating Object: {}".format(dictList["name"]))
	desc_object_uri = dictList["urls"]["describe"]
#	print("Describe URI: {}".format(desc_object_uri))
	obj_desc = run_restapi(token,instance_url,desc_object_uri)
	for dictIter in obj_desc["fields"]:
		obj_rec = "OBJECT:"+dictList["name"]+" ATTR:"+dictIter["name"]+" type:"+dictIter["type"]+" precision:"+str(dictIter["precision"])+" scale:"+str(dictIter["scale"])
#		print(obj_rec)
#		print("OBJECT:{} ATTR:{} type:{} precision:{} scale:{}".format(dictList["name"], dictIter["name"], dictIter["type"], dictIter["precision"], dictIter["scale"]))
		comp_data.add(obj_rec.strip())
#	print("Comp data: {}".format(comp_data))
#	for line in comp_data:
#		print ("Comp record: {}".format(line))
	# Perform data comparion - we compare reference and comparison data
	diff_attr = list(ref_data.difference(comp_data))
	for line in diff_attr:
		print("Attributes missing in SFDC: {}".format(line))
	# TBD - Write pass or failure result into validation file


#************************
# TBD - Create a map of Salesforce Datatypes and Informatica

