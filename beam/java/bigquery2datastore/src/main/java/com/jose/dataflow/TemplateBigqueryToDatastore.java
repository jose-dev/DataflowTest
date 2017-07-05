package com.jose.dataflow;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

public class TemplateBigqueryToDatastore {

    public interface Options extends PipelineOptions {
        @Description("Path of the file to read from and store to Cloud Datastore")
        ValueProvider<String> getInput();
        void setInput(ValueProvider<String> value);

        // Note: This maps to Project ID for v1 version of datastore
        @Description("Dataset ID to read from Cloud Datastore")
        @Validation.Required
        ValueProvider<String> getDataset();
        void setDataset(ValueProvider<String> value);

        @Description("Name of key")
        @Validation.Required
        ValueProvider<String> getKeyName();
        void setKeyName(ValueProvider<String> value);

        @Description("Cloud Datastore Entity Kind")
        @Validation.Required
        ValueProvider<String> getKind();
        void setKind(ValueProvider<String> value);

        @Description("Dataset namespace")
        @Validation.Required
        ValueProvider<String> getNamespace();
        void setNamespace(@Nullable ValueProvider<String> value);
    }


    static class CreateEntityFn extends DoFn<TableRow, Entity> {
        private final ValueProvider<String> namespace;
        private final ValueProvider<String> kind;
        private final ValueProvider<String> keyName;
        private HashMap<String, String> dataTypes = new HashMap<String, String>();

        CreateEntityFn(ValueProvider<String> namespace, ValueProvider<String> kind,
                       ValueProvider<String> keyName, HashMap<String, String> dataTypes) {
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
            if (typeName == null ) {
                return makeValue(obj.toString()).setExcludeFromIndexes(true).build();
            }

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

        public Entity makeEntity(TableRow content) throws Exception {
            Entity.Builder entityBuilder = Entity.newBuilder();

            Key.Builder keyBuilder = makeKey(kind.get(), content.get(keyName.get()).toString());

            if (namespace.get() != null) {
                keyBuilder.getPartitionIdBuilder().setNamespaceId(namespace.get());
            }

            entityBuilder.setKey(keyBuilder.build());

            Map<String, Value> properties = new HashMap<String, Value>();
            for (String fieldName: content.keySet()) {
                properties.put(fieldName,
                               translateValue(content.get(fieldName), this.dataTypes.get(fieldName)));
            }
            entityBuilder.putAllProperties(properties);

            return entityBuilder.build();
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            c.output(makeEntity(c.element()));
        }
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

    private static HashMap<String, String> mapDataTypes(String tableName) {
        List<String> bqTableName = splitBigQueryTableName(tableName);
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        Table t = bigquery.getTable(bqTableName.get(1), bqTableName.get(2));
        Schema sc = t.getDefinition().getSchema();
        return getDataTypes(sc);
    }


    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        // get schema of table to be uploaded
        HashMap<String, String> dataTypes = new HashMap<String, String>();

        // load data from BQ to datastore
        Pipeline p = Pipeline.create(options);
        p.apply("Reading BigQuery Table", BigQueryIO.read().from(options.getInput()))
         .apply("TableRow to Entities", ParDo.of(new CreateEntityFn(options.getNamespace(),
                                                                    options.getKind(),
                                                                    options.getKeyName(),
                                                                    dataTypes)))
         .apply("Saving to Datastore", DatastoreIO.v1().write().withProjectId(options.getDataset()));

        p.run();
    }

}
