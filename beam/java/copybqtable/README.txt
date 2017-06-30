mvn clean install

gcloud auth application-default login

mvn exec:java -Dexec.mainClass=com.jose.dataflow.MinimalCopyBqTable -Dexec.cleanupDaemonThreads=false -Dexec.args="--project=dev-ocd-eu-datascience --stagingLocation=gs://dataflow-dev-ocd-eu-datascience/xxx --runner=DataflowRunner --tempLocation=gs://tmp-dataflow-dev-ocd-eu-datascience/xxx --inputTable=dev-ocd-eu-datascience:aaa.original --outputTable=dev-ocd-eu-datascience:aaa.copied" -e -X
