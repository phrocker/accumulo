package org.apache.accumulo.core.file.rfile.rfiletests;

import org.apache.hadoop.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hadoop.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.File;
import java.util.Comparator;

public class TestRuntime implements Comparator<TestRuntime>, Comparable<TestRuntime> {
    public final long averageRuntime;
    public final long keysFound;
    public final long fileSize;

    public final String filename;

    public long totalRuntime;
    public String description;


    public TestRuntime(String description, String filename, long keysFound, long averageRuntime, long totalRuntime) {
        this.description = description;
        this.filename = filename;
        this.keysFound = keysFound;
        this.averageRuntime = averageRuntime;
        this.totalRuntime=totalRuntime;
        this.fileSize = new File(filename).length();
    }

    @Override
    public int compare(TestRuntime testRuntime, TestRuntime t1) {
        int compare = Long.compare(testRuntime.totalRuntime,t1.totalRuntime);
        if (compare ==0){
            compare = Long.compare(testRuntime.fileSize, t1.fileSize );
            if (compare ==0) {
                compare = testRuntime.description.compareTo(t1.description);
            }
        }
        return compare;
    }

    @Override
    public String toString(){
        ObjectMapper mapper =new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int compareTo(TestRuntime testRuntime) {
        return compare(this,testRuntime);
    }
}
