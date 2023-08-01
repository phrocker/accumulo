package org.apache.accumulo.core.file.rfile.readahead;

import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.file.rfile.MultiLevelIndex;

public abstract class BaseReadAhead implements ReadAheadRequestor {

  protected MultiLevelIndex.Reader.IndexIterator iiter;

  public BaseReadAhead(final MultiLevelIndex.Reader.IndexIterator iiter) {
    this.iiter = iiter;
  }

  @Override
  public BlockedRheadAhead getNextBlock() throws ExecutionException, InterruptedException {
    return null;
  }

  public void setIterator(final MultiLevelIndex.Reader.IndexIterator iiter) {
    this.iiter = iiter;
  }

  @Override
  public abstract boolean hasNextRead();

  @Override
  public void checkpoint(long entriesRemaining) {

  }

  @Override
  public void reset(){

  }
}
