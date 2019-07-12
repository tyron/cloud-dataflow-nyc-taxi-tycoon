/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.codelabs.dataflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.Tuple;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.InferableFunction;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import com.google.codelabs.dataflow.utils.CustomPipelineOptions;
import com.google.codelabs.dataflow.utils.JsonToTableRow;
import com.google.codelabs.dataflow.utils.RidePoint;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;


// Dataflow command-line options must be specified:
//   --project=<your project ID>
//   --sinkProject=<your project ID>
//   --stagingLocation=gs://<your staging bucket>
//   --runner=DataflowRunner
//   --streaming=true
//   --numWorkers=3
//   --zone=<your compute zone>
// You can launch the pipeline from the command line using:
// mvn exec:java -Dexec.mainClass="com.google.codelabs.dataflow.AllRides" -e -Dexec.args="<your arguments>"

@SuppressWarnings("serial")
public class AllRides {
  private static final Logger LOG = LoggerFactory.getLogger(AllRides.class);

  // ride format from PubSub
  // {
  // "ride_id":"a60ba4d8-1501-4b5b-93ee-b7864304d0e0",
  // "latitude":40.66684000000033,
  // "longitude":-73.83933000000202,
  // "timestamp":"2016-08-31T11:04:02.025396463-04:00",
  // "meter_reading":14.270274,
  // "meter_increment":0.019336415,
  // "ride_status":"enroute" / "pickup" / "dropoff"
  // "passenger_count":2
  // }

  private static class PassThroughAllRides extends DoFn<RidePoint, RidePoint> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      RidePoint ride = c.element();

      LOG.info(ride.rideId);
      
      // Access to data fields:
      // float lat = Float.parseFloat(ride.get("latitude").toString());
      // float lon = Float.parseFloat(ride.get("longitude").toString());

      c.output(ride);
    }
  }
  
  final static TupleTag<Map<String, String>> successfulTransformation = new TupleTag<Map<String, String>>();
final static TupleTag<Tuple<String, String>> failedTransformation = new TupleTag<Tuple<String, String>>();

  public static void main(String[] args) {
    
    CustomPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
    Pipeline p = Pipeline.create(options);
    
    CoderRegistry cr = p.getCoderRegistry();
    cr.registerCoderForClass(RidePoint.class, AvroCoder.of(RidePoint.class));

    
    p.apply("read from PubSub", PubsubIO.readStrings()
        .fromTopic(String.format("projects/%s/topics/%s", options.getSourceProject(), options.getSourceTopic()))
        .withTimestampAttribute("ts"))
    
     .apply(ParseJsons.of(RidePoint.class))
     // .setCoder(SerializableCoder.of(RidePoint.class))
     /*.apply("Transform",
          ParDo.of(new JsonTransformer())
                  .withOutputTags(successfulTransformation,
                          TupleTagList.of(failedTransformation)));
*/
     // A Parallel Do (ParDo) transforms data elements one by one.
     // It can output zero, one or more elements per input element.
     /* .apply("pass all rides through 1", ParDo.of(new PassThroughAllRides()))

     // In Java 8 you can also use a simpler syntax through MapElements.
     // MapElements allows a single output element per input element.
     .apply("pass all rides through 2",
        MapElements.into(TypeDescriptor.of(RidePoint.class)).via((RidePoint e) -> e))
        /*MapElements.into(TypeDescriptor.of(String.class)).via((String e) ->  {
          InputStream inputStream = new ByteArrayInputStream(e.getBytes(StandardCharsets.UTF_8.name()));
          return TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
        }))*/

     // In java 8, if you need to return zero one or more elements per input, you can use
     // the FlatMapElements syntax. It expects you to return an iterable and will
     // gather all of its values into the output PCollection.
     /*.apply("pass all rides through 3",
        FlatMapElements.into(TypeDescriptors.strings()).via(
          (String e) -> {
            List<String> a = new ArrayList<>();
              a.add(e);
              return a;
            }))
* /
     .apply(AsJsons.of(RidePoint.class))

     .apply("write to PubSub", PubsubIO.writeStrings()
        .to(String.format("projects/%s/topics/%s", options.getSinkProject(), options.getSinkTopic())))*/;
    p.run();
  }

   /**
   * Deserialize the input and convert it to a key-value pairs map.
   */
  static class JsonTransformer extends DoFn<PubsubMessage, Map<String, String>> {

    /**
     * Process each element.
     *
     * @param c the processing context
     */
    @ProcessElement
    public void processElement(ProcessContext c) {
        String messagePayload = new String(c.element().getPayload());
        try {
            Type type = new TypeToken<Map<String, String>>() {
            }.getType();
            Gson gson = new Gson();
            Map<String, String> map = gson.fromJson(messagePayload, type);
            c.output(map);
        } catch (Exception e) {
            LOG.error("Failed to process input {} -- adding to dead letter file", c.element(), e);
            String attributes = c.element()
                    .getAttributeMap()
                    .entrySet().stream().map((entry) ->
                            String.format("%s -> %s\n", entry.getKey(), entry.getValue()))
                    .collect(Collectors.joining());
            c.output(failedTransformation, Tuple.of(attributes, messagePayload));
        }

    }
  }

}