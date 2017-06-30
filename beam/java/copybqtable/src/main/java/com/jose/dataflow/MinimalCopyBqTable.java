/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jose.dataflow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.SerializableFunction;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.BigQueryOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MinimalCopyBqTable {

    public interface Options extends PipelineOptions {
        @Description("Input table")
        @Validation.Required
        String getInputTable();
        void setInputTable(String value);

        @Description("Output table")
        @Validation.Required
        String getOutputTable();
        void setOutputTable(String value);
    }

    static final SerializableFunction<TableRow, TableRow> IDENTITY_FORMATTER =
            new SerializableFunction<TableRow, TableRow>() {
                @Override
                public TableRow apply(TableRow input) {
                    return input;
                }
            };

    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Pipeline p = Pipeline.create(options);

        TableSchema ts = getTableSchema(options.getInputTable());
        //TableSchema ts = getTableSchema();

        p.apply("Reading BQ Table", BigQueryIO.read().from(options.getInputTable()))
         .apply("Writing BQ Table", BigQueryIO.<TableRow>write()
                 .to(options.getOutputTable())
                 .withFormatFunction(IDENTITY_FORMATTER)
                 .withSchema(ts));

        p.run().waitUntilFinish();
    }

    private static List<String> splitBigQueryTableName (String tableName) {
        List<String> s1 = Arrays.asList(tableName.split(":"));
        List<String> s2 = Arrays.asList(s1.get(1).split("\\."));
        return Arrays.asList(s1.get(0), s2.get(0), s2.get(1));
    }

    private static TableSchema getTableSchema(String tableName) {
        List<String> bqTableName = splitBigQueryTableName(tableName);
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        Table t = bigquery.getTable(bqTableName.get(1), bqTableName.get(2));

        Schema schema = t.getDefinition().getSchema();
        List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
        for (Field fld: schema.getFields()) {
            System.out.println("name: " + fld.getName());
            System.out.println("type: " + fld.getType().getValue().toString());
            System.out.println("mode: " + fld.getMode().toString());


            fields.add(new TableFieldSchema().setName(fld.getName())
                      .setType(fld.getType().getValue().toString())
                      .setMode(fld.getMode().toString()));
        }

        System.out.println("number of fields: " + fields.size());
        return new TableSchema().setFields(fields);

    }

//    public static TableSchema getTableSchema() {
//        List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
//        fields.add(new TableFieldSchema().setName("id").setType("INTEGER").setMode("NULLABLE"));
//        fields.add(new TableFieldSchema().setName("value").setType("STRING").setMode("NULLABLE"));
//        return new TableSchema().setFields(fields);
//    }
}
