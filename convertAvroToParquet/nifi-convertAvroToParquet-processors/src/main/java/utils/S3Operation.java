package utils;

import com.amazonaws.AmazonServiceException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.logging.ComponentLog;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class S3Operation {

  public static void putParquetDataToS3(DataFileStream<GenericRecord> reader, String awsAccessKey,
      String awsSecretKey, String s3Bucket, String schemaName, String tableName, AtomicLong written,
      ComponentLog logger) throws AmazonServiceException, IOException {

    //schema of avro file
    Schema avroSchema = reader.getSchema();

    String S3BucketPath =
        "s3a://" + s3Bucket + "/" + schemaName + "/" + tableName + "/";

    //current date
    Date currentDate = new Date(System.currentTimeMillis());

    //convert date to iso
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+7:00"));
    String currentDateToString = sdf.format(currentDate);

    //file name save to s3
    String fileName = schemaName + '-' + tableName + '-' + currentDateToString + ".parquet";
    logger.info("S3 Path: " + S3BucketPath + fileName);
    //put S3
    Configuration conf = new Configuration();

    conf.set("fs.s3a.access.key", awsAccessKey);
    conf.set("fs.s3a.secret.key", awsSecretKey);

    Path path = new Path(S3BucketPath + fileName);

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(avroSchema)
        .withCompressionCodec(CompressionCodecName.GZIP)
        .withConf(conf)
        .withPageSize(4 * 1024 * 1024) //For compression
        .withRowGroupSize(16 * 1024 * 1024) //For write buffering (Page size)
        .build()) {

      while (reader.hasNext()) {
        GenericRecord record = reader.next();
        written.incrementAndGet();
        writer.write(record);
      }
    }
  }


}
