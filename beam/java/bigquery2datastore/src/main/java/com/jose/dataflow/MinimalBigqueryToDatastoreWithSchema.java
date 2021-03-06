package com.jose.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

public class MinimalBigqueryToDatastoreWithSchema {
    static final List<String> DUMMY_LINES = Arrays.asList("");

    public interface Options extends PipelineOptions {
        @Description("Path of the file to read from and store to Cloud Datastore")
        @Validation.Required
        String getInput();
        void setInput(String value);

        // Note: This maps to Project ID for v1 version of datastore
        @Description("Dataset ID to read from Cloud Datastore")
        @Validation.Required
        String getDataset();
        void setDataset(String value);

        @Description("Name of key")
        @Validation.Required
        String getKeyName();
        void setKeyName(String value);

        @Description("Cloud Datastore Entity Kind")
        @Validation.Required
        String getKind();
        void setKind(String value);

        @Description("Dataset namespace")
        @Validation.Required
        String getNamespace();
        void setNamespace(@Nullable String value);
    }


    static class CreateEntityFn extends DoFn<TableRow, Entity> {
        private final String namespace;
        private final String kind;
        private final String keyName;
        private PCollectionView<Map<String, String>> dataTypes;

        CreateEntityFn(String namespace, String kind, String keyName, PCollectionView<Map<String, String>> dataTypes) {
            this.namespace = namespace;
            this.kind = kind;
            this.keyName = keyName;
            this.dataTypes = dataTypes;
        }

        // convert Object to Float
        private static float convertObjectToFloat(Object obj) throws Exception {
            float floatValue = 0;

            if ( obj instanceof Integer ) {
                floatValue = ((Integer) obj).floatValue();
            }
            else if ( obj instanceof Double ) {
                floatValue = ((Double) obj).floatValue();
            }
            else if ( obj instanceof Float ) {
                floatValue = (Float) obj;
            }
            else if ( obj instanceof Object ) {
                floatValue = Float.valueOf((String) obj);
            }

            return floatValue;
        }

        // convert Object to Integer
        private static int convertObjectToInteger(Object obj) throws Exception {
            int intValue = 0;

            if ( obj instanceof Integer ) {
                intValue = ((Integer) obj);
            }
            else if ( obj instanceof Object ) {
                intValue = Integer.valueOf((String) obj);
            }

            return intValue;
        }

        // convert Object to Boolean
        private static boolean convertObjectToBoolean(Object obj) throws Exception {
            boolean boolValue = false;

            if ( obj instanceof Boolean ) {
                boolValue = ((Boolean) obj);
            }
            else if ( obj instanceof Object ) {
                boolValue = Boolean.valueOf((String) obj);
            }

            return boolValue;
        }

        private Value translateValue(Object obj, String typeName) throws Exception {
            if (typeName.equals("FLOAT")) {
                return makeValue(convertObjectToFloat(obj)).setExcludeFromIndexes(true).build();
            }
            else if (typeName.equals("INTEGER")) {
                return makeValue(convertObjectToInteger(obj)).setExcludeFromIndexes(true).build();
            }
            else if (typeName.equals("BOOLEAN")) {
                return makeValue(convertObjectToBoolean(obj)).setExcludeFromIndexes(true).build();
            }
            else {
                return makeValue(obj.toString()).setExcludeFromIndexes(true).build();
            }
        }

        public Entity makeEntity(TableRow content, Map<String, String> dtypes) throws Exception {
            Entity.Builder entityBuilder = Entity.newBuilder();

            Key.Builder keyBuilder = makeKey(kind, content.get(keyName).toString());

            if (namespace != null) {
                keyBuilder.getPartitionIdBuilder().setNamespaceId(namespace);
            }

            entityBuilder.setKey(keyBuilder.build());

            Map<String, Value> properties = new HashMap<String, Value>();
            for (String fieldName: content.keySet()) {
                properties.put(fieldName,
                               translateValue(content.get(fieldName), dtypes.get(fieldName)));
            }
            entityBuilder.putAllProperties(properties);

            return entityBuilder.build();
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            Map<String, String> dtype = c.sideInput(dataTypes);
            c.output(makeEntity(c.element(), dtype));
        }
    }



    static class MapDataTypesFn extends DoFn<String, KV<String, String>> {
        private static final long serialVersionUID = 1L;

        private String tableName;

        MapDataTypesFn(String tableName)	{
            this.tableName = tableName;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            HashMap<String, String> dataTypes = mapDataTypes(tableName);
            for (String key: dataTypes.keySet()) {
                c.output( KV.of(key, dataTypes.get(key)) );
            }
        }

        private static HashMap<String, String> mapDataTypes (String tableName) {
            List<String> bqTableName = splitBigQueryTableName(tableName);
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            Table t = bigquery.getTable(bqTableName.get(1), bqTableName.get(2));
            Schema sc = t.getDefinition().getSchema();
            return getDataTypes(sc);
        }

        private static List<String> splitBigQueryTableName (String tableName) {
            List<String> s1 = Arrays.asList(tableName.split(":"));
            List<String> s2 = Arrays.asList(s1.get(1).split("\\."));
            return Arrays.asList(s1.get(0), s2.get(0), s2.get(1));
        }

        private static HashMap<String, String> getDataTypes(Schema schema) {
            HashMap<String, String> out = new HashMap<String, String>();
            for (Field field: schema.getFields()) {
                out.put(field.getName().toString(),
                        translateDataTypes(field.getType()));
            }
            return out;
        }

        private static String translateDataTypes(Field.Type type) {
            LegacySQLTypeName t = type.getValue();
            if (t.equals(LegacySQLTypeName.INTEGER)) {
                return "INTEGER";
            }
            else if (t.equals(LegacySQLTypeName.BOOLEAN)) {
                return "BOOLEAN";
            }
            else if (t.equals(LegacySQLTypeName.FLOAT)) {
                return "FLOAT";
            }
            else if (t.equals(LegacySQLTypeName.TIMESTAMP)) {
                return "TIMESTAMP";
            }
            else {
                return "STRING";
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Pipeline p = Pipeline.create(options);

        // get schema of table to be uploaded
        PCollectionView<Map<String, String>> dataTypes =
                p.apply(Create.of(DUMMY_LINES)).setCoder(StringUtf8Coder.of())
                 .apply("Getting Schema", ParDo.of(new MapDataTypesFn(options.getInput())))
                 .apply(View.<String, String>asMap());

        // load BQ table to datastore
        p.apply("Reading BigQuery Table", BigQueryIO.read().from(options.getInput()))
         .apply("TableRow to Entities", ParDo.of(new CreateEntityFn(options.getNamespace(),
                                                                    options.getKind(),
                                                                    options.getKeyName(),
                                                                    dataTypes)).withSideInputs(dataTypes))
         .apply("Saving to Datastore", DatastoreIO.v1().write().withProjectId(options.getDataset()));

        p.run().waitUntilFinish();
    }

}
