/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package convertAvroToParquet;

import com.amazonaws.AmazonServiceException;
import exception.NoSchemaTableInfoException;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import utils.FailureTracker;
import utils.S3Operation;

@SideEffectFree
@SupportsBatching
@Tags({"avro", "convert", "parquet"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Converts a Binary Avro record into Parquet")

public class DatashineConvertAvroToParquet extends AbstractProcessor {

  private static final PropertyDescriptor S3_BUCKET_NAME = new PropertyDescriptor.Builder()
      .name("S3 Bucket Name")
      .description("Bucket Name")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .required(true)
      .build();

  private static final PropertyDescriptor AWS_ACCESS_KEY = new PropertyDescriptor.Builder()
      .name("S3 Access Key")
      .description("Access Key")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .required(true)
      .sensitive(true)
      .build();

  private static final PropertyDescriptor AWS_SECRET_KEY = new PropertyDescriptor.Builder()
      .name("S3 Secret Key")
      .description("Secret Key")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .required(true)
      .sensitive(true)
      .build();

  private static final Relationship REL_SUCCESS = new Relationship.Builder()
      .name("success")
      .description("A FlowFile is routed to this relationship after it has been converted to JSON")
      .build();
  private static final Relationship REL_FAILURE = new Relationship.Builder()
      .name("failure")
      .description(
          "A FlowFile is routed to this relationship if it cannot be parsed as Avro or cannot be converted to JSON for any reason")
      .build();

  private List<PropertyDescriptor> properties;

  @Override
  protected void init(ProcessorInitializationContext context) {
    super.init(context);

    final List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(AWS_ACCESS_KEY);
    properties.add(AWS_SECRET_KEY);
    properties.add(S3_BUCKET_NAME);
    this.properties = Collections.unmodifiableList(properties);
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return properties;
  }

  @Override
  public Set<Relationship> getRelationships() {
    final Set<Relationship> rels = new HashSet<>();
    rels.add(REL_SUCCESS);
    rels.add(REL_FAILURE);
    return rels;
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    FlowFile flowFile = session.get();

    final long flowFileID = flowFile.getId();

    //Get attributes from previous processors
    final String schemaNameAttribute = flowFile.getAttribute("schemaName");
    final String tableNameAttribute = flowFile.getAttribute("tableName");

    //Get properties
    final String awsAccessKey = context.getProperty(AWS_ACCESS_KEY).getValue();
    final String awsSecretKey = context.getProperty(AWS_SECRET_KEY).getValue();
    final String s3BucketName = context.getProperty(S3_BUCKET_NAME).getValue();

    try {
      final AtomicLong written = new AtomicLong(0L);
      final FailureTracker failures = new FailureTracker();
      flowFile = session.write(flowFile, new StreamCallback() {
        @Override
        public void process(final InputStream rawIn, final OutputStream rawOut) throws IOException {

          final GenericData genericData = GenericData.get();
          byte[] byteArray = IOUtils.toByteArray(rawIn);
          final InputStream rawInSchema = new ByteArrayInputStream(byteArray);
          final InputStream rawInRecord = new ByteArrayInputStream(byteArray);
          final InputStream outRecord = new ByteArrayInputStream(byteArray);

          try (final InputStream inSchema = new BufferedInputStream(rawInSchema);
              final InputStream inRecord = new BufferedInputStream(rawInRecord);

              final DataFileStream<GenericRecord> readerSchema = new DataFileStream<>(inSchema,
                  new GenericDatumReader<GenericRecord>());
              final DataFileStream<GenericRecord> readerRecord = new DataFileStream<>(inRecord,
                  new GenericDatumReader<GenericRecord>())) {


            if (schemaNameAttribute == null || schemaNameAttribute.equals("")
                || tableNameAttribute == null || tableNameAttribute.equals("")) {
              if (readerSchema.hasNext()) {
                GenericRecord firstRecord = readerSchema.next();
                String schemaName = String.valueOf(firstRecord.get("_schema"));
                String tableName = String.valueOf(firstRecord.get("_table"));

                if (schemaName.isEmpty() || tableName.isEmpty()
                    || schemaName.equals("null") || tableName.equals("null")) {
                  throw new NoSchemaTableInfoException();
                } else {
                  S3Operation
                      .putParquetDataToS3(readerRecord, awsAccessKey, awsSecretKey, s3BucketName,
                          schemaName, tableName, written, getLogger());
                }
              }
            } else {
              S3Operation
                  .putParquetDataToS3(readerRecord, awsAccessKey, awsSecretKey, s3BucketName,
                      schemaNameAttribute, tableNameAttribute, written, getLogger());
            }

          } catch (AmazonServiceException e) {
            failures.add(e);
            getLogger()
                .warn("FlowfileID: " + flowFileID + " - put parquet to S3 has problems " +
                    e.toString());
          } catch (NoSchemaTableInfoException e) {
            failures.add(e);
            getLogger().warn("FlowfileID: " + flowFileID + "  - no schema or table info");
          } finally {
            IOUtils.copy(outRecord, rawOut);
            outRecord.close();
            rawOut.close();
          }
        }
      });

      //Check for error
      long errors = failures.count();

      if (errors > 0L) {
        session.transfer(flowFile, REL_FAILURE);
        getLogger().warn("FlowfileID: " + flowFile.getId()
                + " Failed to convert {}/{} records from Avro to Parquet ",
            new Object[]{errors, errors + written.get()});
      } else {
        session.transfer(flowFile, REL_SUCCESS);
      }

    } catch (final ProcessException pe) {
      session.transfer(flowFile, REL_FAILURE);
      getLogger()
          .error("FlowfileID: " + flowFile.getId()
                  + " - Failed to convert {} from Avro to Parquet due to {}; transferring to failure",
              new Object[]{flowFile, pe});
    } catch (Exception e) {
      session.transfer(flowFile, REL_FAILURE);
      getLogger()
          .error("FlowfileID: " + flowFile.getId() + " - " + e.getMessage(),
              new Object[]{flowFile, e});
    }
  }
}