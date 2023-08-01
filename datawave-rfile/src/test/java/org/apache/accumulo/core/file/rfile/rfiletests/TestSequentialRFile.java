package org.apache.accumulo.core.file.rfile.rfiletests;


import org.apache.accumulo.core.file.rfile.predicate.ColumnFamilyPredicate;
import org.apache.accumulo.core.file.rfile.predicate.KeyPredicate;
import org.apache.accumulo.core.file.rfile.predicate.RowPredicate;
import org.apache.accumulo.core.file.rfile.predicate.UniqueRowPredicate;

import java.io.IOException;
import java.util.Set;

public class TestSequentialRFile extends RFileTestRun {

    private RowPredicate rowPredicate;

    private KeyPredicate keyPredicate;

    protected TestSequentialRFile(TestRFile rfileTest, String filename) {
        super(rfileTest, filename);
    }

    @Override
    public void configureIterators() throws IOException {


        topIter = rfileTest.reader;
    }

    @Override
    public void configurebaseLayer() throws IOException {
        rfileTest.openSequentialReader(file, file.length(), false, auths != null ? auths.iterator().next() : null, keyPredicate);
    }

    public static class Builder{

        private Set<String> auths;
        private TestRFile rfileTest;
        private String filename;
        private KeyPredicate keyPredicate;

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

        public TestSequentialRFile build(){
            var tr = new TestSequentialRFile(rfileTest,filename);
            tr.setAuths(auths);
            tr.setKeyPredicate(keyPredicate);
            return tr;
        }


        public Builder withKeyPredicate(KeyPredicate keyPredicate) {
            this.keyPredicate=keyPredicate;
            return this;
        }

    }

    private void setRowPredicate(RowPredicate rowPredicate) {
        this.rowPredicate=rowPredicate;
    }
    
    private void setKeyPredicate(KeyPredicate keyPredicate){ this.keyPredicate=keyPredicate;}


}
