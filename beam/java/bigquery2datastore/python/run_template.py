"""


"""


from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
import pprint
import time

SLEEPTIME=15

credentials = GoogleCredentials.get_application_default()
service = build('dataflow', 'v1b3', credentials=credentials)

# Set the following variables to your values.
JOBNAME = 'testingbq2datastoretemplate'
PROJECT = 'dev-ocd-eu-datascience'
VERSION='0.0.1'
BUCKET='dataflow-dev-ocd-eu-datascience'
TEMPLATE = 'TemplateBigqueryViaTextToDatastoreWithSchema'

DATASET = "dev-ocd-eu-datascience"
NAMESPACE = "testnamespace"

INPUT_TEXT = "gs://testdata-dev-ocd-eu-datascience/dataflow/original/*"
INPUT = "dev-ocd-eu-datascience:aaa.original"
KEYNAME = "id"
KIND = "bqtablerowsxxx"

#INPUT_TEXT = "gs://testdata-dev-ocd-eu-datascience/dataflow/small_datastore/*"
#INPUT = "dev-ocd-eu-datascience:aaa.small_datastore"
#KEYNAME = "CustomerIdentifier"
#KIND = "bqtablerows"

BODY = {
    "jobName": "{jobname}".format(jobname=JOBNAME),
    "gcsPath": "gs://{bucket}/templates/{template}/{version}".format(bucket=BUCKET, template=TEMPLATE, version=VERSION),
    "parameters": {
        "dataset" : DATASET,
        "namespace": NAMESPACE,
        "inputText" : INPUT_TEXT,
        "input" : INPUT,
        "keyName" : KEYNAME,
        "kind" : KIND
    },
#    "environment": {
#        "tempLocation": "gs://tmp-dataflow-dev-ocd-eu-datascience/xxx",
#        "gcpTempLocation": "gs://tmp-dataflow-dev-ocd-eu-datascience/xxx",
#        "zone": "us-central1-f"
#    }
}

request = service.projects().templates().create(projectId=PROJECT, body=BODY)
response = request.execute()
job_id = response['id']

pprint.pprint(response)

while(True):
    response = service.projects().jobs().get(projectId=PROJECT, jobId=job_id).execute()
    """
        possible responses:
            - JOB_STATE_RUNNING
            - JOB_STATE_DONE
            - JOB_STATE_FAILED
    """
    pprint.pprint(response)
    if response['currentState'] != 'JOB_STATE_RUNNING':
        break
    else:
        time.sleep(SLEEPTIME)


