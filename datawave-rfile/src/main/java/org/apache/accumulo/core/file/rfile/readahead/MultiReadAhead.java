package org.apache.accumulo.core.file.rfile.readahead;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex;

public class MultiReadAhead extends BaseReadAhead {

  private final BlockSupplier dataRetrieval;
  private final int readAheadCount;
  BlockReadAhead readAhead;
  FutureReadAhead nextRead = null;

  Deque<FutureReadAhead> readAheads = new ArrayDeque<>();

  public MultiReadAhead(BlockReadAhead readAhead, BlockSupplier dataRetrieval, int readAheadCount) {
    super(null);
    this.dataRetrieval = dataRetrieval;
    this.readAhead = readAhead;
    this.nextRead = null;
    this.readAheadCount = readAheadCount;
  }


  @Override
  public BlockedRheadAhead peek() throws ExecutionException, InterruptedException {
    var block = nextRead == null ? null : nextRead.get();
    if (!readAheads.isEmpty() || iiter.hasNext()) {
      if (!readAheads.isEmpty()) {
        block = readAheads.peek().get();
      }
    }

    return block;
  }
  @Override
  public BlockedRheadAhead getNextBlock() throws ExecutionException, InterruptedException {
    var block = nextRead == null ? null : nextRead.get();
    if (!readAheads.isEmpty() || iiter.hasNext()) {
      if (!readAheads.isEmpty()) {
        nextRead = readAheads.pop();
      }
      if (readAheads.size() <= readAheadCount) {

        // launch enough to fill our readAheads
        var thou = (readAheadCount - readAheads.size());
        Deque<FutureReadAhead> myReadAheads = new ArrayDeque<>();
        for (int i = 0; i < thou && iiter.hasNext(); i++) {
          MultiLevelIndex.IndexEntry nxt = iiter.next();
          var nextRH = initiateReadAhead(nxt, dataRetrieval);
          if (iiter.hasNext()){
            // store the next key
            MultiLevelIndex.IndexEntry nxtNxt = iiter.peek();
            nextRH.setNextKey(nxtNxt.getKey());
          }
          myReadAheads.add(nextRH);
        }
        readAheads.addAll(myReadAheads);
      }
      if (null == block) {
        return getNextBlock();
      }
    } else {
      nextRead = null;
    }

    return block;
  }

  @Override
  public void setIterator(final MultiLevelIndex.Reader.IndexIterator iiter) {
    super.setIterator(iiter);
    // if we re-seek we need to re-create state. Any work done with the executor
    // will be for naught, so using judiciously.
    nextRead = null;
    readAhead.drain();
    readAheads = new ArrayDeque<>();
  }

  @Override
  public boolean hasNextRead() {
    return null != nextRead || !readAheads.isEmpty() || (null != iiter && iiter.hasNext());
  }

  @Override
  public void checkpoint(long entriesRemaining) {
    // nextRead = readAhead.submitReadAheadRequest(onRun);
  }

  private FutureReadAhead initiateReadAhead(MultiLevelIndex.IndexEntry indexEntry,
      BlockSupplier dataRetrieval) {
    return new FutureReadAhead( readAhead.submitReadAheadRequest(() -> {
      BlockedRheadAhead readAhead = new BlockedRheadAhead();
      readAhead.entriesLeft = indexEntry.getNumEntries();
      readAhead.currBlock = dataRetrieval.get(indexEntry);

      readAhead.numEntries = indexEntry.getNumEntries();
      if (readAhead.numEntries < 50000) {
        readAhead.threshHold = 0;
      } else {
        readAhead.threshHold = (int) (Math.ceil(readAhead.numEntries * .80));
      }
      readAhead.topKey = indexEntry.getKey();
      return readAhead;
    }));
  }


  @Override
  public void reset(){
  }
}
