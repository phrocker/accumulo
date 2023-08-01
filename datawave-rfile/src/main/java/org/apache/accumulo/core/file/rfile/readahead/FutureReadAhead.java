package org.apache.accumulo.core.file.rfile.readahead;

import org.apache.accumulo.core.data.Key;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureReadAhead implements Future<BlockedRheadAhead> {

    private final Future<BlockedRheadAhead> delegate;
    private Key nextKey = null;

    public FutureReadAhead(Future<BlockedRheadAhead> delegate){
        this.delegate = delegate;
    }
    @Override
    public boolean cancel(boolean b) {
        return delegate.cancel(b);
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    public void setNextKey(Key nextKey){
        this.nextKey=nextKey;
    }

    public Key getNextKey(){
        return this.nextKey;
    }

    @Override
    public BlockedRheadAhead get() throws InterruptedException, ExecutionException {
        var rt = delegate.get();
        rt.nextBlockKey=nextKey;
        return rt;
    }

    @Override
    public BlockedRheadAhead get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.get(l,timeUnit);
    }
}
