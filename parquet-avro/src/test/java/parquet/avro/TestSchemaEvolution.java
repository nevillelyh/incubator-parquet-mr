package parquet.avro;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility;

public class TestSchemaEvolution {

  private static final String PARQUET_PATH = "/tmp/parquet-avro-test.parquet";
  private static final String AVRO_PATH = "/tmp/parquet-avro-test.avro";

  @Test
  public void testCompatibility() throws Exception {
//    System.out.println(RecordOne.getClassSchema().toString(true));
//    System.out.println(RecordTwo.getClassSchema().toString(true));
//    System.out.println(RecordThree.getClassSchema().toString(true));

    Schema schemaOne = RecordOne.getClassSchema();
    Schema schemaTwo = RecordTwo.getClassSchema();
    Schema schemaThree = RecordThree.getClassSchema();

    System.out.println(checkReaderWriterCompatibility(rename(schemaTwo, "RecordOne"), schemaOne));
    System.out.println(checkReaderWriterCompatibility(rename(schemaThree, "RecordOne"), schemaOne));

    writeAvro();
    writeParquet();

    readAvro(RecordOne.class);
    readAvro(RecordTwo.class);
    readAvro(RecordThree.class);

    readParquet(RecordOne.class);
    readParquet(RecordTwo.class);
    readParquet(RecordThree.class);
  }

  private void writeAvro() throws IOException {
    File tmp = new File(AVRO_PATH);
    //tmp.deleteOnExit();
    tmp.delete();

    DataFileWriter<RecordOne> writer = new DataFileWriter<RecordOne>(new SpecificDatumWriter<RecordOne>(RecordOne.class));
    writer.create(RecordOne.getClassSchema(), tmp);
    writer.append(new RecordOne("s1", 1, 1L));
    writer.append(new RecordOne("s2", 2, 2L));
    writer.append(new RecordOne("s3", 3, 3L));
    writer.close();
  }

  private <T> void readAvro(Class<T> clazz) throws IOException {
    File tmp = new File(AVRO_PATH);

    System.out.println("Reading Avro with " + clazz.getSimpleName());
    DataFileReader<T> reader = new DataFileReader<T>(tmp, new SpecificDatumReader<T>(clazz));
    for (T t : reader) {
      System.out.println(t);
    }
    reader.close();
    System.out.println();
  }

  private void writeParquet() throws IOException {
    File tmp = new File(PARQUET_PATH);
    //tmp.deleteOnExit();
    tmp.delete();
    Path path = new Path(PARQUET_PATH);

    AvroParquetWriter<RecordOne> writer = new AvroParquetWriter<RecordOne>(path, RecordOne.getClassSchema());
    writer.write(new RecordOne("s1", 1, 1L));
    writer.write(new RecordOne("s2", 2, 2L));
    writer.write(new RecordOne("s3", 3, 3L));
    writer.close();
  }

  private <T extends IndexedRecord> void readParquet(Class<T> clazz) throws Exception {
    System.out.println("Reading Parquet with " + clazz.getSimpleName());

    Configuration conf = new Configuration();
    Schema schema = (Schema) clazz.getMethod("getClassSchema").invoke(null);
    System.out.println(schema);
    AvroReadSupport.setAvroReadSchema(conf, schema);
    AvroReadSupport.setRequestedProjection(conf, Projections.projectSchema(schema, "str_field", "int_field"));
    AvroParquetReader<T> reader = new AvroParquetReader<T>(conf, new Path(PARQUET_PATH));
    T t = reader.read();
    while (t != null) {
      System.out.println(t);
      t = reader.read();
    }
    reader.close();
    System.out.println();
  }

  private static Schema rename(Schema schema, String name) {
    Schema record = Schema.createRecord(name, schema.getDoc(), schema.getNamespace(), false);

    List<Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field field : schema.getFields()) {
      fields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()));
    }
    record.setFields(fields);

    return record;
  }

}
