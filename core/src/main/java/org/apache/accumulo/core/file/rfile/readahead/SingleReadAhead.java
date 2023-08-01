package org.apache.accumulo.core.file.rfile.readahead;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.accumulo.core.file.rfile.MultiLevelIndex;

public class SingleReadAhead extends BaseReadAhead {

  private final BlockSupplier dataRetrieval;
  BlockReadAhead readAhead;
  Future<BlockedRheadAhead> nextRead = null;

  public SingleReadAhead(BlockReadAhead readAhead, BlockSupplier dataRetrieval) {
    super(null);
    this.dataRetrieval = dataRetrieval;
    this.readAhead = readAhead;
    this.nextRead = null;
  }

  @Override
  public BlockedRheadAhead getNextBlock() throws ExecutionException, InterruptedException {
    var block = nextRead == null ? null : nextRead.get();
    if (iiter.hasNext()) {
      var nxt = iiter.next();
      nextRead = initiateReadAhead(nxt, dataRetrieval);
      if (null == block) {
        return getNextBlock();
      }
    } else {
      nextRead = null;
    }

    return block;
  }

  @Override
  public boolean hasNextRead() {
    return null != nextRead || (null != iiter && iiter.hasNext());
  }

  @Override
  public void checkpoint(long entriesRemaining) {
    // nextRead = readAhead.submitReadAheadRequest(onRun);
  }

  private Future<BlockedRheadAhead> initiateReadAhead(MultiLevelIndex.IndexEntry indexEntry,
      BlockSupplier dataRetrieval) {
    return readAhead.submitReadAheadRequest(() -> {
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
    });
  }
}
