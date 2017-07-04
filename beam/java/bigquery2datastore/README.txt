mvn clean install

gcloud auth application-default login

mvn exec:java -Dexec.mainClass=com.jose.dataflow.MinimalBigqueryToDatastore -Dexec.cleanupDaemonThreads=false -Dexec.args="--project=dev-ocd-eu-datascience --stagingLocation=gs://dataflow-dev-ocd-eu-datascience/xxx --runner=DataflowRunner --tempLocation=gs://tmp-dataflow-dev-ocd-eu-datascience/xxx --dataset=dev-ocd-eu-datascience --namespace=testnamespace --input=dev-ocd-eu-datascience:aaa.small_datastore --keyName=CustomerIdentifier --kind=bqtablerows" -e -X





--------------------------------------------------------------------------------------------------------------------------------------------

TEMPLATE
--------

 mvn compile exec:java \
     -Dexec.mainClass=com.jose.dataflow.TemplateBigqueryToDatastore \
     -Dexec.cleanupDaemonThreads=false \
     -Dexec.args="--runner=DataflowRunner \
                  --project=dev-ocd-eu-datascience \
                  --stagingLocation=gs://dataflow-dev-ocd-eu-datascience/staging \
                  --tempLocation=gs://tmp-dataflow-dev-ocd-eu-datascience/tmploc \
                  --templateLocation=gs://dataflow-dev-ocd-eu-datascience/templates/TemplateBigqueryToDatastore/0.0.1" \
                  -e -X




 python python/run_template.py