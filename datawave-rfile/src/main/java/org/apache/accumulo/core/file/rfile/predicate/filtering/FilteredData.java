package org.apache.accumulo.core.file.rfile.predicate.filtering;

import org.apache.accumulo.core.data.ByteSequence;

public class FilteredData {

    public boolean filtered = false;
    public byte [] originalSequence = null;

    public FilteredData(byte [] data, boolean filtered){
        this.originalSequence = data;
        this.filtered=filtered;
    }

    public static FilteredData of(byte [] data, boolean filtered){
        return new FilteredData(data,filtered);
    }
}
