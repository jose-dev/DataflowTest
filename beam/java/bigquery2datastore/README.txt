
MINIMAL (BASIC)

mvn clean install

gcloud auth application-default login

mvn exec:java -Dexec.mainClass=com.jose.dataflow.MinimalBigqueryToDatastore -Dexec.cleanupDaemonThreads=false -Dexec.args="--project=dev-osp-eu-mlservices --stagingLocation=gs://dataflow-dev-osp-eu-mlservices/xxx --runner=DataflowRunner --tempLocation=gs://tmp-dataflow-dev-osp-eu-mlservices/xxx --dataset=dev-osp-eu-mlservices --namespace=testnamespace --input=dev-osp-eu-mlservices:aaa.small_datastore --keyName=CustomerIdentifier --kind=bqtablerows" -e -X



--------------------------------------------------------------------------------------------------------------------------------------------


MINIMAL (WITH SIDEINPUT TEMPLATE)

mvn clean install

gcloud auth application-default login

mvn exec:java -Dexec.mainClass=com.jose.dataflow.MinimalBigqueryToDatastoreWithSchema -Dexec.cleanupDaemonThreads=false -Dexec.args="--project=dev-osp-eu-mlservices --stagingLocation=gs://dataflow-dev-osp-eu-mlservices/xxx --runner=DataflowRunner --tempLocation=gs://tmp-dataflow-dev-osp-eu-mlservices/xxx --dataset=dev-osp-eu-mlservices --namespace=testnamespace --input=dev-osp-eu-mlservices:aaa.small_datastore --keyName=CustomerIdentifier --kind=bqtablerows" -e -X



--------------------------------------------------------------------------------------------------------------------------------------------


MINIMAL BQ TABLE DATA READ FROM TEXT FILE (WITH SIDEINPUT TEMPLATE)

mvn clean install

gcloud auth application-default login

mvn exec:java -Dexec.mainClass=com.jose.dataflow.MinimalBigqueryViaTextToDatastoreWithSchema -Dexec.cleanupDaemonThreads=false -Dexec.args="--project=dev-osp-eu-mlservices --stagingLocation=gs://dataflow-dev-osp-eu-mlservices/xxx --runner=DataflowRunner --tempLocation=gs://tmp-dataflow-dev-osp-eu-mlservices/xxx --dataset=dev-osp-eu-mlservices --namespace=testnamespace --inputText=gs://testdata-dev-osp-eu-mlservices/dataflow/small_datastore/* --input=dev-osp-eu-mlservices:aaa.small_datastore --keyName=CustomerIdentifier --kind=bqtablerows" -e -X




--------------------------------------------------------------------------------------------------------------------------------------------


TEMPLATE (READING FROM TEXT FILE CONTAINING BQ TABLE DATA)
----------------------------------------------------------

 mvn compile exec:java \
     -Dexec.mainClass=com.jose.dataflow.TemplateBigqueryViaTextToDatastoreWithSchema \
     -Dexec.cleanupDaemonThreads=false \
     -Dexec.args="--runner=DataflowRunner \
                  --project=dev-osp-eu-mlservices \
                  --stagingLocation=gs://dataflow-dev-osp-eu-mlservices/staging \
                  --templateLocation=gs://dataflow-dev-osp-eu-mlservices/templates/TemplateBigqueryViaTextToDatastoreWithSchema/0.0.1 " \
                  -e -X


 python python/run_template.py



--------------------------------------------------------------------------------------------------------------------------------------------


TEMPLATE (READING DIRECTLY FROM BQ TABLE)
-----------------------------------------

WARNING: it is not working as the BQ connector only allows one run :(


 mvn compile exec:java \
     -Dexec.mainClass=com.jose.dataflow.TemplateBigqueryToDatastore \
     -Dexec.cleanupDaemonThreads=false \
     -Dexec.args="--runner=DataflowRunner \
                  --project=dev-osp-eu-mlservices \
                  --stagingLocation=gs://dataflow-dev-osp-eu-mlservices/staging \
                  --templateLocation=gs://dataflow-dev-osp-eu-mlservices/templates/TemplateBigqueryToDatastore/0.0.1 " \
                  -e -X




--------------------------------------------------------------------------------------------------------------------------------------------
