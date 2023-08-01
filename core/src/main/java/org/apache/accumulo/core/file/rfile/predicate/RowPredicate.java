package org.apache.accumulo.core.file.rfile.predicate;

import org.apache.accumulo.core.data.ByteSequence;

public abstract class RowPredicate {

  public boolean acceptRow(ByteSequence row, boolean isSame) {
    return true;
  }

  public boolean acceptRow(byte[] row, boolean isSame) {
    return true;
  }
}
