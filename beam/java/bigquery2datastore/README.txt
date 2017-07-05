
MINIMAL (BASIC)

mvn clean install

gcloud auth application-default login

mvn exec:java -Dexec.mainClass=com.jose.dataflow.MinimalBigqueryToDatastore -Dexec.cleanupDaemonThreads=false -Dexec.args="--project=dev-ocd-eu-datascience --stagingLocation=gs://dataflow-dev-ocd-eu-datascience/xxx --runner=DataflowRunner --tempLocation=gs://tmp-dataflow-dev-ocd-eu-datascience/xxx --dataset=dev-ocd-eu-datascience --namespace=testnamespace --input=dev-ocd-eu-datascience:aaa.small_datastore --keyName=CustomerIdentifier --kind=bqtablerows" -e -X



--------------------------------------------------------------------------------------------------------------------------------------------


MINIMAL (WITH SIDEINPUT TEMPLATE)

mvn clean install

gcloud auth application-default login

mvn exec:java -Dexec.mainClass=com.jose.dataflow.MinimalBigqueryToDatastoreWithSchema -Dexec.cleanupDaemonThreads=false -Dexec.args="--project=dev-ocd-eu-datascience --stagingLocation=gs://dataflow-dev-ocd-eu-datascience/xxx --runner=DataflowRunner --tempLocation=gs://tmp-dataflow-dev-ocd-eu-datascience/xxx --dataset=dev-ocd-eu-datascience --namespace=testnamespace --input=dev-ocd-eu-datascience:aaa.small_datastore --keyName=CustomerIdentifier --kind=bqtablerows" -e -X



--------------------------------------------------------------------------------------------------------------------------------------------


MINIMAL BQ TABLE DATA READ FROM TEXT FILE (WITH SIDEINPUT TEMPLATE)

mvn clean install

gcloud auth application-default login

mvn exec:java -Dexec.mainClass=com.jose.dataflow.MinimalBigqueryViaTextToDatastoreWithSchema -Dexec.cleanupDaemonThreads=false -Dexec.args="--project=dev-ocd-eu-datascience --stagingLocation=gs://dataflow-dev-ocd-eu-datascience/xxx --runner=DataflowRunner --tempLocation=gs://tmp-dataflow-dev-ocd-eu-datascience/xxx --dataset=dev-ocd-eu-datascience --namespace=testnamespace --inputText=gs://testdata-dev-ocd-eu-datascience/dataflow/small_datastore/* --input=dev-ocd-eu-datascience:aaa.small_datastore --keyName=CustomerIdentifier --kind=bqtablerows" -e -X




--------------------------------------------------------------------------------------------------------------------------------------------


TEMPLATE (READING FROM TEXT FILE CONTAINING BQ TABLE DATA)
----------------------------------------------------------

 mvn compile exec:java \
     -Dexec.mainClass=com.jose.dataflow.TemplateBigqueryViaTextToDatastoreWithSchema \
     -Dexec.cleanupDaemonThreads=false \
     -Dexec.args="--runner=DataflowRunner \
                  --project=dev-ocd-eu-datascience \
                  --stagingLocation=gs://dataflow-dev-ocd-eu-datascience/staging \
                  --templateLocation=gs://dataflow-dev-ocd-eu-datascience/templates/TemplateBigqueryViaTextToDatastoreWithSchema/0.0.1 " \
                  -e -X


 python python/run_template.py



--------------------------------------------------------------------------------------------------------------------------------------------


TEMPLATE (READING DIRECTLY FROM BQ TABLE)
-----------------------------------------

 mvn compile exec:java \
     -Dexec.mainClass=com.jose.dataflow.TemplateBigqueryToDatastore \
     -Dexec.cleanupDaemonThreads=false \
     -Dexec.args="--runner=DataflowRunner \
                  --project=dev-ocd-eu-datascience \
                  --stagingLocation=gs://dataflow-dev-ocd-eu-datascience/staging \
                  --templateLocation=gs://dataflow-dev-ocd-eu-datascience/templates/TemplateBigqueryToDatastore/0.0.1 " \
                  -e -X




--------------------------------------------------------------------------------------------------------------------------------------------
