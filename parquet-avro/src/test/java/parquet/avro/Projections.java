package parquet.avro;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;

/**
 * Utilities for Parquet schema projections.
 */
public class Projections {
  private static class HashMapSelfValue extends HashMap<String, HashMapSelfValue> {
  }

  public static <T extends SpecificRecord> Schema projectSchema(Class<T> clazz, String... paths) {
    if (paths == null || paths.length == 0) {
      return null;
    }

    try {
      Schema schema = clazz.getDeclaredConstructor().newInstance().getSchema();
      return projectSchema(schema, paths);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static Schema projectSchema(Schema schema, String... paths) {
    return projectFields(schema, buildTree(paths));
  }

  private static HashMapSelfValue buildTree(String... paths) {
    HashMapSelfValue result = new HashMapSelfValue();
    for (String path : paths) {
      HashMapSelfValue parent = result;

      for (String k : Splitter.on('.').split(path)) {
        HashMapSelfValue node = parent.get(k);

        if (node == null) {
          node = new HashMapSelfValue();
          parent.put(k, node);
        }

        parent = node;
      }
    }
    return result;
  }

  private static Schema projectFields(Schema schema, HashMapSelfValue pathTree) {

    if (isNullable(schema)) {
      Schema result = projectFields(getNonNull(schema), pathTree);
      return Schema.createUnion(ImmutableList.of(result, nullSchema()));
    } else if (schema.getType() == Schema.Type.ARRAY) {
      Schema result = projectFields(schema.getElementType(), pathTree);
      return Schema.createArray(result);
    } else if (schema.getType() == Schema.Type.MAP) {
      Schema result = projectFields(schema.getValueType(), pathTree);
      return Schema.createMap(result);
    } else if (schema.getType() == Schema.Type.RECORD) {
      Schema result = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());

      Map<Integer, Schema.Field> fields = new TreeMap<Integer, Schema.Field>();
      for (Map.Entry<String, HashMapSelfValue> entry : pathTree.entrySet()) {
        String fieldName = entry.getKey();
        Schema.Field f = schema.getField(fieldName);
        HashMapSelfValue subFields = entry.getValue();
        if (subFields.isEmpty()) {
          fields.put(f.pos(), new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultValue()));
        } else {
          Schema s = projectFields(f.schema(), subFields);
          fields.put(f.pos(), new Schema.Field(f.name(), s, f.doc(), f.defaultValue()));
        }
      }
      result.setFields(Lists.newArrayList(fields.values()));

      return result;
    }

    throw new RuntimeException("Can not project fields from non-record schema: " + schema);
  }

  private static boolean isNullable(Schema schema) {
    return schema.getType() == Schema.Type.UNION
        && schema.getTypes().contains(nullSchema())
        && schema.getTypes().size() == 2;// do not support unions with multiple non-null types
  }

  private static Schema getNonNull(Schema schema) {
    return Iterables.find(schema.getTypes(), not(equalTo(nullSchema())));
  }

  private static Schema nullSchema() {
    return Schema.create(Schema.Type.NULL);
  }

}
