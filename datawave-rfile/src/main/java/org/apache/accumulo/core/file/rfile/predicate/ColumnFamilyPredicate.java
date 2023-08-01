package org.apache.accumulo.core.file.rfile.predicate;

import org.apache.accumulo.core.data.ByteSequence;

public abstract class ColumnFamilyPredicate {

    public boolean acceptColumn(ByteSequence row, boolean rowIsSame, ByteSequence cf, boolean cfIsSame){
        return acceptColumn(row.getBackingArray(),rowIsSame,cf.getBackingArray(),cfIsSame);
    }

    public boolean acceptColumn(byte [] row,boolean rowIsSame, byte [] cf, boolean cfIsSame){
        return true;
    }


}
