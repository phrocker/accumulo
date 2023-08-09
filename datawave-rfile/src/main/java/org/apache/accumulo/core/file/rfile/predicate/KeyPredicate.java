package org.apache.accumulo.core.file.rfile.predicate;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;

public abstract class KeyPredicate {


    public Key topKey;

    public Key nextBlockKey;



    public boolean accept(Key key, int fieldSame){
        return true;
    }

    public boolean acceptColumn(ByteSequence row, boolean rowIsSame, ByteSequence cf, boolean cfIsSame){
        return acceptColumn(row.getBackingArray(),rowIsSame,cf.getBackingArray(),cfIsSame, true);
    }

    public boolean acceptColumn(byte [] row,boolean rowIsSame, byte [] cf, boolean cfIsSame, boolean set){
        return true;
    }

    public boolean acceptRow(ByteSequence row, boolean isSame){
        return acceptRow(row.getBackingArray(),isSame);
    }

    public boolean acceptRow(byte [] row, boolean isSame){
        return true;
    }

    public abstract boolean getLastKeyRowFiltered();

    public abstract boolean getLastRowCfFiltered();

    public boolean endKeyComparison(){
        return true;
    }
}
