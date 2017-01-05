package com.google.test_dataflow_datastore;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.datastore.DatastoreIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatastoreRead {
  static Logger LOG = LoggerFactory.getLogger(DatastoreRead.class);
  public static void main(String[] args) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName("Test5");

    pipeline
        .apply(DatastoreIO.v1().read()
            .withQuery(q.build())
            .withProjectId("deft-testing-integration2")
            .withNamespace("vikasrk"))
        .apply(MapElements.via(new EntityFn()));


    /*
    pipeline
        .apply(Create.of(q.build()))
        .apply(ParDo.of(new QueryCloneDoFn()));
    */

    pipeline.run();
  }

  public static class QueryCloneDoFn extends DoFn<Query, Query> {
    @Override
    public void processElement(ProcessContext c) throws Exception {
      Query q = c.element();
      c.output(q.toBuilder().clone().build());
    }
  }

  public static class EntityFn extends SimpleFunction<Entity, String> {
    public String apply(Entity entity) {
      return entity.toString();
    }
  }
}
