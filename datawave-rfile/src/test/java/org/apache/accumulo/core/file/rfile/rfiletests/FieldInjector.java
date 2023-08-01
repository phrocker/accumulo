package org.apache.accumulo.core.file.rfile.rfiletests;

import org.apache.hadoop.io.Text;

public class FieldInjector {
    public Text fieldValue;
    public String fieldName;
    public long numberOfDocs;

    public Text auth;

    public FieldInjector(String fieldName, String fieldValue, String auth, long numberOfDocs) {
        this.fieldName = fieldName;
        this.fieldValue = new Text(fieldValue);
        this.numberOfDocs = numberOfDocs;
        this.auth = new Text(auth);
    }
}
