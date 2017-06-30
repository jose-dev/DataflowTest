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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

public class MinimalWordCount {

    public interface Options extends PipelineOptions {
        @Description("Input text file")
        @Default.String("gs://apache-beam-samples/shakespeare/*")
        String getInputFile();
        void setInputFile(String value);

        @Description("Output text file")
        @Validation.Required
        String getOutputFile();
        void setOutputFile(String value);
    }

    static class TokenizerFn extends DoFn<String, String> {
        private static final long serialVersionUID = 1L;
        public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception	{
            for (String word : c.element().split(TOKENIZER_PATTERN)) {
                if (!word.isEmpty()) {
                    c.output(word);
                }
            }
        }

    }

    static class MergeCountsFn extends DoFn<KV<String, Long>, String> {
        private static final long serialVersionUID = 1L;

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception	{
            KV<String, Long> input = c.element();
            c.output(input.getKey() + ": " + input.getValue());
        }
    }


    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Pipeline p = Pipeline.create(options);

        p.apply(TextIO.read().from(options.getInputFile()))
                .apply("ExtractWords", ParDo.of(new TokenizerFn()))
                .apply(Count.<String>perElement())
                .apply("FormatResults", ParDo.of(new MergeCountsFn()))
                .apply(TextIO.write().to(options.getOutputFile()));

        p.run().waitUntilFinish();
    }
}
