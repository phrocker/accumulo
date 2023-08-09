package org.apache.accumulo.core.file.rfile.predicate;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.file.rfile.PushdownRelativeKey;

import java.util.Arrays;

public class UniqueFieldPredicate extends KeyPredicate {

    byte [] prevRow = null;
    byte [] prevCf = null;

    protected static final byte BIT = 0x01;

    boolean lastRowFiltered = false;
    boolean lastRowCfFiltered = false;



    static final byte ROW_SAME = BIT << 0;
    static final byte CF_SAME = BIT << 1;
    static final byte CQ_SAME = BIT << 2;
    static final byte CV_SAME = BIT << 3;
    static final byte TS_SAME = BIT << 4;
    static final byte DELETED = BIT << 5;

    public boolean accept(Key key, int fieldSame) {
        var rowIsSame = (fieldSame& ROW_SAME) == ROW_SAME;
        var cfIsSame = (fieldSame& CF_SAME) == CF_SAME;
        return acceptColumn(key.getRowData().getBackingArray(),rowIsSame,key.getColumnFamilyData().getBackingArray(),cfIsSame, false);
    }



    @Override
    public boolean acceptColumn(byte [] row,boolean rowIsSame,  byte [] cf, boolean cfIsSame, boolean set){
        // in this edition let's assume the row matches our terms
        if (rowIsSame && cfIsSame){
            if (set) {
                prevRow = row;
                prevCf = cf;
            }
            lastRowFiltered = true;
            lastRowCfFiltered  = true;
            return false;
        }
        else {
            if (null != prevRow && null != prevCf){
                //System.out.println("Comparing " + new String(row) + " to " + new String(prevRow));
                //System.out.println("Comparing " + new String(cf) + " to " + new String(prevCf));
                if (Arrays.equals(row, prevRow) && Arrays.equals(cf, prevCf)){
                    lastRowFiltered = true;
                    lastRowCfFiltered  = true;
                    return false;
                }
            }
            else{
                //System.out.println("Comparing " + new String(cf) + " to null");
            }
            if (set) {
                prevRow = row;
                prevCf = cf;
            }
            lastRowFiltered = false;
            lastRowCfFiltered  = false;
            return true;
        }
    }

    @Override
    public boolean acceptRow(byte [] row, boolean isSame){
        // in this edition let's assume the row matches our terms
        if (isSame){
            prevRow = row;
            lastRowFiltered = true;
            return false;
        }
        else {
            if (null != prevRow){
                //System.out.println("Comparing " + new String(row) + " to " + new String(prevRow));
                if (Arrays.equals(row,prevRow)){
                    lastRowFiltered = true;
                    return false;
                }
            }
            else{
                //System.out.println("Comparing " + new String(row) + " to null");
            }
            prevRow = row;
            lastRowFiltered = false;
            return true;
        }
    }


    @Override
    public boolean getLastKeyRowFiltered() {
        return lastRowFiltered;
    }

    @Override
    public boolean getLastRowCfFiltered() {
        return lastRowCfFiltered;
    }


    @Override
    public boolean endKeyComparison(){
        return false;
    }

}
