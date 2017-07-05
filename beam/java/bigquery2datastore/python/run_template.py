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
TEMPLATE = 'TemplateBigqueryToDatastore'

DATASET = "dev-ocd-eu-datascience"
NAMESPACE = "testnamespace"
KIND = "bqtablerows"

#INPUT = "dev-ocd-eu-datascience:aaa.original"
#KEYNAME = "id"
INPUT = "dev-ocd-eu-datascience:aaa.small_datastore"
KEYNAME = "CustomerIdentifier"

BODY = {
    "jobName": "{jobname}".format(jobname=JOBNAME),
    "gcsPath": "gs://{bucket}/templates/{template}/{version}".format(bucket=BUCKET, template=TEMPLATE, version=VERSION),
    "parameters": {
        "dataset" : DATASET,
        "namespace": NAMESPACE,
        "input" : INPUT,
        "keyName" : KEYNAME,
        "kind" : KIND
    },
    "environment": {
        "tempLocation": "gs://tmp-dataflow-dev-ocd-eu-datascience/xxx",
#        "gcpTempLocation": "gs://tmp-dataflow-dev-ocd-eu-datascience/xxx",
#        "zone": "us-central1-f"
    }
}

request = service.projects().templates().create(projectId=PROJECT, body=BODY)
response = request.execute()
job_id = response['id']

pprint.pprint(response)

while(True):
    response = service.projects().jobs().get(projectId=PROJECT, jobId=job_id).execute()
    # JOB_STATE_RUNNING
    # JOB_STATE_DONE
    # JOB_STATE_FAILED
    pprint.pprint(response)
    if response['currentState'] != 'JOB_STATE_RUNNING':
        break
    else:
        time.sleep(SLEEPTIME)


