package org.apache.accumulo.core.file.rfile.readahead;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile;

public final class BlockedRheadAhead {
  public CachableBlockFile.CachedBlockRead currBlock;
  public int entriesLeft;
  public int numEntries;
  public int threshHold;
  public Key topKey;
  public Key nextBlockKey;
}
