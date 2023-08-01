package org.apache.accumulo.core.file.rfile;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.file.rfile.predicate.KeyPredicate;

public class SkippedRelativeKey<R extends RelativeKey> {

  public R rk;
  public int skipped;
  public Key prevKey;
  public boolean filtered;

  public KeyPredicate keyPredicate = null;

  public SkippedRelativeKey(R rk, int skipped, Key prevKey, KeyPredicate keyPredicate) {
    this(rk, skipped, prevKey, false, keyPredicate);
  }

  public SkippedRelativeKey(R rk, int skipped, Key prevKey, boolean filtered,
      KeyPredicate keyPredicate) {
    this.rk = rk;
    this.skipped = skipped;
    this.prevKey = prevKey;
    this.filtered = filtered;
    this.keyPredicate = keyPredicate;
  }

}
