package org.apache.accumulo.core.file.rfile.predicate;

import java.util.Arrays;

import org.apache.accumulo.core.data.ByteSequence;

public class UniqueRowPredicate extends RowPredicate {

  byte[] prev = null;

  @Override
  public boolean acceptRow(ByteSequence row, boolean isSame) {
    // in this edition let's assume the row matches our terms
    if (isSame) {
      prev = row.getBackingArray();
      return false;
    } else {
      if (null != prev) {
        if (Arrays.equals(row.getBackingArray(), prev)) {
          return false;
        }
      }
      prev = row.getBackingArray();
      return true;
    }
  }

  public boolean acceptRow(byte[] row, boolean isSame) {
    // in this edition let's assume the row matches our terms
    if (isSame) {
      prev = row;
      return false;
    } else {
      if (null != prev) {
        if (Arrays.equals(row, prev)) {
          return false;
        }
      }
      prev = row;
      return true;
    }
  }

}
