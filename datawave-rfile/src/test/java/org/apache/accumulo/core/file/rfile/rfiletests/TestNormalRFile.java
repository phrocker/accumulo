package org.apache.accumulo.core.file.rfile.rfiletests;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.uids.GlobalIndexMatchingFilter;
import org.apache.accumulo.core.file.rfile.uids.GlobalIndexTermMatchingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TestNormalRFile extends RFileTestRun {

    protected TestNormalRFile(TestRFile rfileTest, String filename) {
        super(rfileTest, filename);
    }

    @Override
    public void configureIterators() throws IOException {
    topIter =
            VisibilityFilter.wrap(rfileTest.reader, new Authorizations(auths.iterator().next()), "".getBytes());
    var gi = new GlobalIndexTermMatchingIterator();
    Map<String,String> options = new HashMap<String,String>();
    options.put(GlobalIndexMatchingFilter.PATTERN + "1","c.*");
        options.put(GlobalIndexTermMatchingIterator.UNIQUE_TERMS_IN_FIELD ,"true");
        gi.init(topIter,options,null);
        topIter = gi;
    }

    @Override
    public void configurebaseLayer() throws IOException {
        rfileTest.openNormalReader(file, file.length(), false);
    }

    public static class Builder{

        private Set<String> auths;
        private TestRFile rfileTest;
        private String filename;

        private Builder(){

        }

        public static Builder newBuilder(TestRFile rfileTest, String filename){
            Builder b = new Builder();
            b.rfileTest=rfileTest;
            b.filename=filename;
            return b;
        }

        public Builder withAuths(Set<String> auths){
            this.auths=auths;
            return this;
        }

        public TestNormalRFile build(){
            var tr = new TestNormalRFile(rfileTest,filename);
            tr.setAuths(auths);
            return tr;
        }
    }

}