package org.apache.accumulo.core.file.rfile.predicate;

import java.util.Arrays;

import org.apache.accumulo.core.data.Key;

public class UniqueFieldPredicate extends KeyPredicate {

  byte[] prevRow = null;
  byte[] prevCf = null;

  protected static final byte BIT = 0x01;

  static final byte ROW_SAME = BIT << 0;
  static final byte CF_SAME = BIT << 1;
  static final byte CQ_SAME = BIT << 2;
  static final byte CV_SAME = BIT << 3;
  static final byte TS_SAME = BIT << 4;
  static final byte DELETED = BIT << 5;

  public boolean accept(Key key, int fieldSame) {
    var rowIsSame = (fieldSame & ROW_SAME) == ROW_SAME;
    var cfIsSame = (fieldSame & CF_SAME) == CF_SAME;
    return acceptColumn(key.getRowData().getBackingArray(), rowIsSame,
        key.getColumnFamilyData().getBackingArray(), cfIsSame);
  }

  @Override
  public boolean acceptColumn(byte[] row, boolean rowIsSame, byte[] cf, boolean cfIsSame) {
    // in this edition let's assume the row matches our terms
    if (rowIsSame && cfIsSame) {
      prevRow = row;
      prevCf = cf;
      return false;
    } else {
      if (null != prevRow && null != prevCf) {
        if (Arrays.equals(row, prevRow) && Arrays.equals(cf, prevCf)) {
          return false;
        }
      }
      prevRow = row;
      prevCf = cf;
      return true;
    }
  }

  @Override
  public boolean acceptRow(byte[] row, boolean isSame) {
    // in this edition let's assume the row matches our terms
    if (isSame) {
      prevRow = row;
      return false;
    } else {
      if (null != prevRow) {
        if (Arrays.equals(row, prevRow)) {
          return false;
        }
      }
      prevRow = row;
      return true;
    }
  }

}
