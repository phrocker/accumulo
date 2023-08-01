package org.apache.accumulo.core.file.rfile.rfiletests.keycreator;

import java.io.IOException;

public interface IndexingStrategy {

    void select(TestRFileGenerator generator) throws IOException;
}
