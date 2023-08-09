package org.apache.accumulo.core.file.rfile.readahead;

import java.util.concurrent.ExecutionException;

public interface ReadAheadRequestor {

  BlockedRheadAhead getNextBlock() throws ExecutionException, InterruptedException;

  boolean hasNextRead();

  BlockedRheadAhead peek() throws ExecutionException, InterruptedException;
  /**
   * Identifies that we've reached a checkpoint.
   */
  void checkpoint(long entriesRemaining);

  void reset();

}
