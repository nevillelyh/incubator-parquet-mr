/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package redelm.io;


import java.util.Arrays;
import java.util.List;

import redelm.column.ColumnDescriptor;
import redelm.column.ColumnReader;
import redelm.column.ColumnWriter;
import redelm.column.ColumnsStore;
import redelm.schema.PrimitiveType.Primitive;
import redelm.schema.Type;


public class PrimitiveColumnIO extends ColumnIO {
//  private static final Logger logger = Logger.getLogger(PrimitiveColumnIO.class.getName());

  private ColumnWriter columnWriter;
  private ColumnIO[] path;
  private ColumnsStore columns;
  private ColumnDescriptor columnDescriptor;

  PrimitiveColumnIO(Type type, GroupColumnIO parent) {
    super(type, parent);
  }

  @Override
  void setLevels(int r, int d, String[] fieldPath, int[] fieldIndexPath, List<ColumnIO> repetition, List<ColumnIO> path, ColumnsStore columns) {
    this.columns = columns;
    super.setLevels(r, d, fieldPath, fieldIndexPath, repetition, path, columns);
    this.columnDescriptor = new ColumnDescriptor(fieldPath, getType().asPrimitiveType().getPrimitive(), getRepetitionLevel(), getDefinitionLevel());
    this.columnWriter = columns.getColumnWriter(columnDescriptor);
    this.path = path.toArray(new ColumnIO[path.size()]);
  }

  @Override
  List<String[]> getColumnNames() {
    return Arrays.asList(new String[][] { getFieldPath() });
  }

  @Override
  void writeNull(int r, int d) {
    columnWriter.writeNull(r, d);
  }

  ColumnReader getColumnReader() {
    return columns.getColumnReader(columnDescriptor);
  }

  public ColumnWriter getColumnWriter() {
    return columnWriter;
  }

  public ColumnIO[] getPath() {
    return path;
  }

  public boolean isLast(int r) {
    return getLast(r) == this;
  }

  private PrimitiveColumnIO getLast(int r) {
    ColumnIO parent = getParent(r);

    PrimitiveColumnIO last = parent.getLast();
    return last;
  }

  @Override
  PrimitiveColumnIO getLast() {
    return this;
  }

  @Override
  PrimitiveColumnIO getFirst() {
    return this;
  }
  public boolean isFirst(int r) {
    return getFirst(r) == this;
  }

  private PrimitiveColumnIO getFirst(int r) {
    ColumnIO parent = getParent(r);
    return parent.getFirst();
  }

  public Primitive getPrimitive() {
    return getType().asPrimitiveType().getPrimitive();
  }

}
