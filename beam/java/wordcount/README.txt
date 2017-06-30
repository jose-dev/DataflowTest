mvn clean install

gcloud auth application-default login

mvn exec:java -Dexec.mainClass=com.jose.dataflow.MinimalWordCount -Dexec.cleanupDaemonThreads=false -Dexec.args="--project=dev-ocd-eu-datascience --stagingLocation=gs://dataflow-dev-ocd-eu-datascience/xxx --runner=DataflowRunner --gcpTempLocation=gs://tmp-dataflow-dev-ocd-eu-datascience/xxx -outputFile=gs://dev-ocd-eu-datascience/wordcounts/junk" -e -X
