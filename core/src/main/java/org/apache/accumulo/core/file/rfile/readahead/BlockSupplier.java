package org.apache.accumulo.core.file.rfile.readahead;

import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex;

public interface BlockSupplier {

  CachableBlockFile.CachedBlockRead get(MultiLevelIndex.IndexEntry entry);
}
