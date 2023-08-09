/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.file.rfile;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.file.rfile.predicate.KeyPredicate;
import org.apache.accumulo.core.file.rfile.predicate.filtering.FilteredData;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.MutableByteSequence;
import org.apache.accumulo.core.util.UnsynchronizedBuffer;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.hadoop.io.WritableUtils;

/**
 * Allow certain filtering at the file level.
 */
public class PushdownRelativeKey extends RelativeKey {

  private KeyPredicate keyPredicate;
  private Authorizations authObj;
  private VisibilityEvaluator ve;
  protected static final Map<ByteSequence,Boolean> cache= new LRUMap<>(1000);;
  boolean skipVisibilityFiltering = true;
  boolean lastKeyCvFiltered = false;

  boolean lastKeyRowFiltered = false;

  boolean lastRowCfFIltered = false;

  /**
   * This constructor is used when one needs to read from an input stream
   */
  public PushdownRelativeKey() {
    this.ve = null;
    authObj = null;
  }

  public PushdownRelativeKey(String auths,KeyPredicate keyPredicate) {
    authObj = auths == null || auths.isEmpty() ? new Authorizations()
        : new Authorizations(auths.getBytes(UTF_8));
    this.ve = new VisibilityEvaluator(authObj);
    this.keyPredicate = keyPredicate;
  }

  public void setAuths(String auths) {
    authObj = auths == null || auths.isEmpty() ? new Authorizations()
        : new Authorizations(auths.getBytes(UTF_8));
    this.ve = new VisibilityEvaluator(authObj);
  }


  public void setkeyPredicate(KeyPredicate keyPredicate){
    this.keyPredicate =keyPredicate;
  }
  /**
   * This constructor is used when constructing a key for writing to an output stream
   */
  public PushdownRelativeKey(Key prevKey, Key key) {
    super(prevKey, key);
    this.ve = null;
    this.authObj=null;
  }

  public PushdownRelativeKey(Key prevKey, Key key, String auths) {
    super(prevKey, key);
    authObj = auths == null || auths.isEmpty() ? new Authorizations()
        : new Authorizations(auths.getBytes(UTF_8));
    this.ve = new VisibilityEvaluator(authObj);
  }

  public void setPrevKey(Key pk) {
    this.prevKey = pk;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    fieldsSame = in.readByte();
    if ((fieldsSame & PREFIX_COMPRESSION_ENABLED) == PREFIX_COMPRESSION_ENABLED) {
      fieldsPrefixed = in.readByte();
    } else {
      fieldsPrefixed = 0;
    }

    byte[] row , cf , cq , cv;
    final long ts;
    // check the auths.
    if (lastKeyRowFiltered){
      lastKeyRowFiltered = false;
    }

    if (lastKeyCvFiltered) {
      // we have the same visibility.
      if ((fieldsSame & CV_SAME) == CV_SAME) {
        lastKeyCvFiltered = true;
      } else {
        lastKeyCvFiltered = false;
      }
    }
    /*
    row = getData(in,ROW_SAME,ROW_COMMON_PREFIX,()-> prevKey.getRowData(),lastKeyCvFiltered);

    cf = getData(in,CF_SAME,CF_COMMON_PREFIX,()-> prevKey.getColumnFamilyData(),lastKeyCvFiltered);*/


    var rowFil = getRowData(in, () -> prevKey.getRowData(), keyPredicate, lastKeyCvFiltered);

    if (rowFil.filtered && null != keyPredicate){
      //System.out.println("row is filterd");


      row = prevKey.getRowData().toArray();
    }
    else{
      row = rowFil.originalSequence;

    }

    lastKeyRowFiltered = keyPredicate.getLastKeyRowFiltered();

    var cfData = getCfData(in,() -> prevKey.getRowData(), () -> prevKey.getColumnFamilyData(),
            keyPredicate,
            lastKeyCvFiltered, lastKeyRowFiltered);
    if (cfData.filtered && null != keyPredicate){
      //System.out.println("Skipping " + new String(row) + " " +  " " + prevKey);
      cf = prevKey.getColumnFamilyData().toArray();
    }
    else{
      cf = cfData.originalSequence;

    }
    lastKeyRowFiltered = keyPredicate.getLastKeyRowFiltered();
    lastRowCfFIltered = keyPredicate.getLastRowCfFiltered();


    cq = getData(in, CQ_SAME, CQ_COMMON_PREFIX, () -> prevKey.getColumnQualifierData(),
            lastKeyCvFiltered || lastKeyRowFiltered || lastRowCfFIltered);
    if (null == authObj || authObj.isEmpty()){
      cv = getData(in, CV_SAME, CV_COMMON_PREFIX, () -> prevKey.getColumnVisibilityData(),
              lastKeyCvFiltered || lastKeyRowFiltered || lastRowCfFIltered);
    }
    else {
      cv = getCvFiltered(in, CV_SAME, CV_COMMON_PREFIX, () -> prevKey.getColumnVisibilityData(),
              lastKeyCvFiltered || lastKeyRowFiltered || lastRowCfFIltered);
    }

    if ((fieldsSame & TS_SAME) == TS_SAME) {
      ts = prevKey.getTimestamp();
    } else if ((fieldsPrefixed & TS_DIFF) == TS_DIFF) {
      ts = WritableUtils.readVLong(in) + prevKey.getTimestamp();
    } else {
      ts = WritableUtils.readVLong(in);
    }



    if (!lastKeyCvFiltered && !lastKeyRowFiltered && !lastRowCfFIltered) {
      try {
        this.key = new Key(row, cf, cq, cv, ts, (fieldsSame & DELETED) == DELETED, false);
        //System.out.println("produced key " + this.key);
      }
      catch (NullPointerException npe){
        npe.printStackTrace();
        throw npe;
      }
      if (null != keyPredicate && keyPredicate.endKeyComparison() && !keyPredicate.accept(this.key,fieldsSame)){
        //System.out.println(" no returning " + key);
        lastRowCfFIltered = true;
      }
      else{
        //System.out.println("returning " + key);
        lastRowCfFIltered = false;
      }
      this.prevKey = this.key;
    } else {
      //System.out.println(lastKeyCvFiltered + " " + lastKeyRowFiltered + " " + lastRowCfFIltered);
        this.key = null;
        /*
      if (row != null)
        //System.out.println("got " + new String(row));
      else{
        //System.out.println("got null");
      }

      if (cf != null)
        //System.out.println("got " + new String(cf));
      else{
        //System.out.println("got null");
      }
      */
      //this.prevKey = key;

    }

  }

  public boolean isLastKeyFiltered() {
    return isLastKeyCvFiltered() || isLastKeyRowFiltered() || lastRowCfFIltered;
  }


  public boolean isLastKeyCvFiltered() {
    return this.lastKeyCvFiltered;
  }

  public boolean isLastKeyRowFiltered() {
    return this.lastKeyRowFiltered;
  }

  protected byte[] getData(DataInput in, byte fieldBit, byte commonPrefix,
      Supplier<ByteSequence> data) throws IOException {
    if ((fieldsSame & fieldBit) == fieldBit) {
      return data.get().toArray();
    } else if ((fieldsPrefixed & commonPrefix) == commonPrefix) {
      return readPrefix(in, data.get());
    } else {
      return read(in);
    }
  }

  protected byte[] getData(DataInput in, byte fieldBit, byte commonPrefix,
      Supplier<ByteSequence> data, boolean skipCurrentKey) throws IOException {
    if ((fieldsSame & fieldBit) == fieldBit) {
      if (skipCurrentKey) {
        return null;
      }
      return data.get().toArray();
    } else if ((fieldsPrefixed & commonPrefix) == commonPrefix) {
      return readPrefix(in, data.get(), skipCurrentKey);
    } else {
      return read(in, skipCurrentKey);
    }
  }

  protected FilteredData getRowData(DataInput in, Supplier<ByteSequence> data,
                                    KeyPredicate predicate, boolean skipCurrentKey) throws IOException {
    if ((fieldsSame & ROW_SAME) == ROW_SAME) {
      //System.out.println("samne "  + skipCurrentKey);
      if (skipCurrentKey) {
        return FilteredData.of(data.get().toArray(),true);
      }
      else {
        if (null == predicate || (null != predicate && predicate.acceptRow(data.get(),true))) {
          return FilteredData.of(data.get().toArray(),false);
        }
        else{
          return FilteredData.of(data.get().toArray(),true);
        }
      }
    } else if ((fieldsPrefixed & ROW_COMMON_PREFIX) == ROW_COMMON_PREFIX) {
      var dt = readPrefix(in, data.get(), skipCurrentKey);
      //System.out.println("got a prefix "  + skipCurrentKey);
      if (null == predicate || (null != predicate &&predicate.acceptRow(dt,false))) {
        return FilteredData.of(dt,false);
      }
      else{
        return FilteredData.of(dt,true);
      }
    } else {
      //System.out.println("row not a prefix "  + skipCurrentKey);
      var dt = read(in, skipCurrentKey);
      if (null == predicate || (null != predicate &&predicate.acceptRow(dt,false))) {
        return FilteredData.of(dt,false);
      }
      else{
        return FilteredData.of(dt,true);
      }
    }
  }

  protected FilteredData getCfData(DataInput in, Supplier<ByteSequence> rowData, Supplier<ByteSequence> cfData,
                                   KeyPredicate predicate, boolean skipByCv, boolean skipByRow) throws IOException {
    //System.out.println("entre");
    boolean rowIsSame = (fieldsSame & ROW_SAME) == ROW_SAME;
    if ((fieldsSame & CF_SAME) == CF_SAME) {
      //System.out.println("samne cf "  + skipByCv);
      if (skipByCv && skipByRow) {
        return FilteredData.of(cfData.get().toArray(),true);
      }
      else {
        if (null == predicate || (null != predicate && predicate.acceptColumn(rowData.get(), rowIsSame, cfData.get(),true))) {
          return FilteredData.of(cfData.get().toArray(),false);
        }
        else{
          return FilteredData.of(cfData.get().toArray(),true);
        }
      }
    } else if ((fieldsPrefixed & CF_COMMON_PREFIX) == CF_COMMON_PREFIX) {
      var dt = readPrefix(in, cfData.get(), skipByCv);
      //System.out.println("got a cf prefix "  + skipByCv + " - " + (!skipByCv ? new String(dt) : ""));
      if (null == predicate || (null != predicate && predicate.acceptColumn(rowData.get().getBackingArray(), rowIsSame, dt,false, true))) {
        return FilteredData.of(dt,false);
      }
      else{
        return FilteredData.of(dt,true);
      }
    } else {
      var dt = read(in, skipByCv);
      //System.out.println("not a cf prefix "  + skipByCv + " - " + (!skipByCv ? new String(dt) : ""));
      if (null == predicate || (null != predicate && predicate.acceptColumn(rowData.get().getBackingArray(), rowIsSame, dt,false, true))) {
        //System.out.println("not filtered");
        return FilteredData.of(dt,false);
      }
      else{
        //System.out.println("filtered");
        return FilteredData.of(dt,true);
      }
    }
  }


  protected static byte[] readPrefix(DataInput in, ByteSequence prefixSource, boolean skip)
      throws IOException {
    int prefixLen = WritableUtils.readVInt(in);
    int remainingLen = WritableUtils.readVInt(in);
    if (skip) {
      in.skipBytes(remainingLen);
      return null;
    }
    byte[] data = new byte[prefixLen + remainingLen];
    if (prefixSource.isBackedByArray()) {
      System.arraycopy(prefixSource.getBackingArray(), prefixSource.offset(), data, 0, prefixLen);
    } else {
      byte[] prefixArray = prefixSource.toArray();
      System.arraycopy(prefixArray, 0, data, 0, prefixLen);
    }
    // read remaining
    in.readFully(data, prefixLen, remainingLen);
    return data;
  }

  protected boolean isVisibilityAccepted(byte[] visibility) {

    if (visibility.length == 0) {
      return true;
    }

    Boolean b = cache.get(visibility);
    if (b != null) {
      return b;
    }

    try {
      boolean bb = ve.evaluate(new ColumnVisibility(visibility));
      cache.put(new ArrayByteSequence(visibility), bb);
      return bb;
    } catch (VisibilityParseException | BadArgumentException e) {
      return false;
    }
  }

  protected byte[] getCvFiltered(DataInput in, byte fieldBit, byte commonPrefix,
      Supplier<ByteSequence> data, boolean skipCurrentKey) throws IOException {
    if ((fieldsSame & fieldBit) == fieldBit) {
      if (skipCurrentKey) {
        return null;
      }
      // not filtered, meaning we don't need to check again
      return data.get().toArray();
    } else if ((fieldsPrefixed & commonPrefix) == commonPrefix) {
      byte[] resp = readPrefix(in, data.get(), skipCurrentKey);
      if (!skipCurrentKey) {
        lastKeyCvFiltered = !isVisibilityAccepted(resp);
      }
      return resp;
    } else {
      byte[] resp = read(in, skipCurrentKey);
      if (!skipCurrentKey) {
        lastKeyCvFiltered = !isVisibilityAccepted(resp);
      }
      return resp;
    }
  }

  protected static void readSkippedPrefix(DataInput in, MutableByteSequence dest,
      ByteSequence prefixSource, boolean skip) throws IOException {
    int prefixLen = WritableUtils.readVInt(in);
    int remainingLen = WritableUtils.readVInt(in);
    if (skip) {
      in.skipBytes(remainingLen);
      return;
    }
    int len = prefixLen + remainingLen;
    if (dest.getBackingArray().length < len) {
      dest.setArray(new byte[UnsynchronizedBuffer.nextArraySize(len)], 0, 0);
    }
    if (prefixSource.isBackedByArray()) {
      System.arraycopy(prefixSource.getBackingArray(), prefixSource.offset(),
          dest.getBackingArray(), 0, prefixLen);
    } else {
      byte[] prefixArray = prefixSource.toArray();
      System.arraycopy(prefixArray, 0, dest.getBackingArray(), 0, prefixLen);
    }
    // read remaining
    in.readFully(dest.getBackingArray(), prefixLen, remainingLen);
    dest.setLength(len);
  }

  protected static byte[] read(DataInput in, boolean skip) throws IOException {
    int len = WritableUtils.readVInt(in);
    if (skip) {
      byte[] data = new byte[len];
      in.readFully(data);
      //System.out.println("Skipped " + new String(data));
      //in.skipBytes(len);
      return null;
    } else {
      byte[] data = new byte[len];
      in.readFully(data);
      //System.out.println("Getting " + new String(data));
      return data;
    }
  }

  public Key getKey() {
    return key;
  }

  protected static void write(DataOutput out, ByteSequence bs) throws IOException {
    WritableUtils.writeVInt(out, bs.length());
    out.write(bs.getBackingArray(), bs.offset(), bs.length());
  }

  protected static void writePrefix(DataOutput out, ByteSequence bs, int commonPrefixLength)
      throws IOException {
    WritableUtils.writeVInt(out, commonPrefixLength);
    WritableUtils.writeVInt(out, bs.length() - commonPrefixLength);
    out.write(bs.getBackingArray(), bs.offset() + commonPrefixLength,
        bs.length() - commonPrefixLength);
  }

  @Override
  public void write(DataOutput out) throws IOException {

    out.writeByte(fieldsSame);

    if ((fieldsSame & PREFIX_COMPRESSION_ENABLED) == PREFIX_COMPRESSION_ENABLED) {
      out.write(fieldsPrefixed);
    }

    if ((fieldsSame & ROW_SAME) == ROW_SAME) {
      // same, write nothing
    } else if ((fieldsPrefixed & ROW_COMMON_PREFIX) == ROW_COMMON_PREFIX) {
      // similar, write what's common
      writePrefix(out, key.getRowData(), rowCommonPrefixLen);
    } else {
      // write it all
      write(out, key.getRowData());
    }

    if ((fieldsSame & CF_SAME) == CF_SAME) {
      // same, write nothing
    } else if ((fieldsPrefixed & CF_COMMON_PREFIX) == CF_COMMON_PREFIX) {
      // similar, write what's common
      writePrefix(out, key.getColumnFamilyData(), cfCommonPrefixLen);
    } else {
      // write it all
      write(out, key.getColumnFamilyData());
    }

    if ((fieldsSame & CQ_SAME) == CQ_SAME) {
      // same, write nothing
    } else if ((fieldsPrefixed & CQ_COMMON_PREFIX) == CQ_COMMON_PREFIX) {
      // similar, write what's common
      writePrefix(out, key.getColumnQualifierData(), cqCommonPrefixLen);
    } else {
      // write it all
      write(out, key.getColumnQualifierData());
    }

    if ((fieldsSame & CV_SAME) == CV_SAME) {
      // same, write nothing
    } else if ((fieldsPrefixed & CV_COMMON_PREFIX) == CV_COMMON_PREFIX) {
      // similar, write what's common
      writePrefix(out, key.getColumnVisibilityData(), cvCommonPrefixLen);
    } else {
      // write it all
      write(out, key.getColumnVisibilityData());
    }

    if ((fieldsSame & TS_SAME) == TS_SAME) {
      // same, write nothing
    } else if ((fieldsPrefixed & TS_DIFF) == TS_DIFF) {
      // similar, write what's common
      WritableUtils.writeVLong(out, tsDiff);
    } else {
      // write it all
      WritableUtils.writeVLong(out, key.getTimestamp());
    }
  }

  private static boolean isAccepted(VisibilityEvaluator ve,Map<ByteSequence,Boolean> cache,byte [] visibility ){
    if (visibility.length == 0) {
      return true;
    }

    Boolean b = cache.get(visibility);
    if (b != null) {
      return b;
    }

    try {
      boolean bb = ve.evaluate(new ColumnVisibility(visibility));
      cache.put(new ArrayByteSequence(visibility), bb);
      return bb;
    } catch (VisibilityParseException | BadArgumentException e) {
      return false;
    }
  }

  public static SkippedRelativeKey<PushdownRelativeKey> pushdownFastSkip(String auths, DataInput in, Key seekKey,
                                                                         MutableByteSequence value, Key prevKey, Key currKey, int entriesLeft, KeyPredicate keyPredicate) throws IOException {
    // this method mostly avoids object allocation and only does compares when the row changes

    Authorizations authObj = auths == null || auths.isEmpty() ? new Authorizations()
            : new Authorizations(auths.getBytes(UTF_8));
    VisibilityEvaluator ve = new VisibilityEvaluator(authObj);


    MutableByteSequence row, cf, cq, cv;
    MutableByteSequence prow, pcf, pcq, pcv;

    ByteSequence stopRow = seekKey.getRowData();
    ByteSequence stopCF = seekKey.getColumnFamilyData();
    ByteSequence stopCQ = seekKey.getColumnQualifierData();

    long ts = -1;
    long pts = -1;
    boolean pdel = false;

    int rowCmp = -1, cfCmp = -1, cqCmp = -1;

    boolean lastKeyFiltered;
    if (authObj.isEmpty()){
      lastKeyFiltered = false;
    }
    else{
      lastKeyFiltered = currKey != null ? !isAccepted(ve,cache,currKey.getColumnVisibilityData().toArray()) : false;
    }

    if (currKey != null && !lastKeyFiltered) {

      prow = new MutableByteSequence(currKey.getRowData());
      pcf = new MutableByteSequence(currKey.getColumnFamilyData());
      pcq = new MutableByteSequence(currKey.getColumnQualifierData());
      pcv = new MutableByteSequence(currKey.getColumnVisibilityData());
      pts = currKey.getTimestamp();

      row = new MutableByteSequence(currKey.getRowData());
      cf = new MutableByteSequence(currKey.getColumnFamilyData());
      cq = new MutableByteSequence(currKey.getColumnQualifierData());
      cv = new MutableByteSequence(currKey.getColumnVisibilityData());
      ts = currKey.getTimestamp();

      rowCmp = row.compareTo(stopRow);
      cfCmp = cf.compareTo(stopCF);
      cqCmp = cq.compareTo(stopCQ);

      if (rowCmp >= 0) {
        if (rowCmp > 0) {
          PushdownRelativeKey rk = new PushdownRelativeKey(auths, keyPredicate);
          rk.key = rk.prevKey = new Key(currKey);
          int skipped = rk.isLastKeyFiltered() ? 0 : 1;
          return new SkippedRelativeKey(rk, skipped, prevKey, keyPredicate);
        }

        if (cfCmp >= 0) {
          if (cfCmp > 0) {
            PushdownRelativeKey rk = new PushdownRelativeKey(auths, keyPredicate);
            rk.key = rk.prevKey = new Key(currKey);
            int skipped = rk.isLastKeyFiltered() ? 0 : 1;
            return new SkippedRelativeKey(rk, skipped, prevKey, keyPredicate);
          }

          if (cqCmp >= 0) {
            PushdownRelativeKey rk = new PushdownRelativeKey(auths, keyPredicate);
            rk.key = rk.prevKey = new Key(currKey);
            int skipped = rk.isLastKeyFiltered() ? 0 : 1;
            return new SkippedRelativeKey(rk, skipped, prevKey, keyPredicate);
          }
        }
      }

    } else {
      row = new MutableByteSequence(new byte[64], 0, 0);
      cf = new MutableByteSequence(new byte[64], 0, 0);
      cq = new MutableByteSequence(new byte[64], 0, 0);
      cv = new MutableByteSequence(new byte[64], 0, 0);

      prow = new MutableByteSequence(new byte[64], 0, 0);
      pcf = new MutableByteSequence(new byte[64], 0, 0);
      pcq = new MutableByteSequence(new byte[64], 0, 0);
      pcv = new MutableByteSequence(new byte[64], 0, 0);
    }

    byte fieldsSame = -1;
    byte fieldsPrefixed = 0;
    int count = 0;
    Key newPrevKey = null;

    while (count < entriesLeft) {

      pdel = (fieldsSame & DELETED) == DELETED;

      fieldsSame = in.readByte();

      if (lastKeyFiltered) {
        // we have the same visibility.
        if ((fieldsSame & CV_SAME) == CV_SAME) {
          lastKeyFiltered = true;
        } else {
          lastKeyFiltered = false;
        }
      }


      if ((fieldsSame & PREFIX_COMPRESSION_ENABLED) == PREFIX_COMPRESSION_ENABLED) {
        fieldsPrefixed = in.readByte();
      } else {
        fieldsPrefixed = 0;
      }

      boolean changed = false;

      if ((fieldsSame & ROW_SAME) != ROW_SAME) {

        MutableByteSequence tmp = prow;
        prow = row;
        row = tmp;

        if ((fieldsPrefixed & ROW_COMMON_PREFIX) == ROW_COMMON_PREFIX) {
          readPrefix(in, row, prow, lastKeyFiltered);
        } else {
          read(in, row, lastKeyFiltered);
        }

        // read a new row, so need to compare...
        if (!lastKeyFiltered) {
          rowCmp = row.compareTo(stopRow);
        }

/*        if (null != rowPredicate && !rowPredicate.acceptRow(row,true)){
          lastKeyFiltered = true;
        }

 */
        changed = true;
      } // else the row is the same as the last, so no need to compare
      else{
        /*if (null != rowPredicate && !rowPredicate.acceptRow(row,true)){
          lastKeyFiltered = true;
        }*/
      }

      if ((fieldsSame & CF_SAME) != CF_SAME) {

        MutableByteSequence tmp = pcf;
        pcf = cf;
        cf = tmp;

        if ((fieldsPrefixed & CF_COMMON_PREFIX) == CF_COMMON_PREFIX) {
          readPrefix(in, cf, pcf, lastKeyFiltered);
        } else {
          read(in, cf, lastKeyFiltered);
        }

        if (!lastKeyFiltered) {
          cfCmp = cf.compareTo(stopCF);
        }
        changed = true;
      }

      if ((fieldsSame & CQ_SAME) != CQ_SAME) {

        MutableByteSequence tmp = pcq;
        pcq = cq;
        cq = tmp;

        if ((fieldsPrefixed & CQ_COMMON_PREFIX) == CQ_COMMON_PREFIX) {
          readPrefix(in, cq, pcq, lastKeyFiltered);
        } else {
          read(in, cq, lastKeyFiltered);
        }

        if (!lastKeyFiltered) {
          cqCmp = cq.compareTo(stopCQ);
        }
        changed = true;
      }

      if ((fieldsSame & CV_SAME) != CV_SAME) {

        MutableByteSequence tmp = pcv;
        pcv = cv;
        cv = tmp;

        if ((fieldsPrefixed & CV_COMMON_PREFIX) == CV_COMMON_PREFIX) {
          readPrefix(in, cv, pcv);
        } else {
          read(in, cv);
        }

      }
      if (!authObj.isEmpty()) {
        lastKeyFiltered = !isAccepted(ve, cache, cv.toArray());
      }

      if ((fieldsSame & TS_SAME) != TS_SAME) {
        pts = ts;

        if ((fieldsPrefixed & TS_DIFF) == TS_DIFF) {
          ts = WritableUtils.readVLong(in) + pts;
        } else {
          ts = WritableUtils.readVLong(in);
        }
      }

      readValue(in, value);

      count++;

      if (!lastKeyFiltered && changed && rowCmp >= 0) {
        if (rowCmp > 0) {
          break;
        }

        if (cfCmp >= 0) {
          if (cfCmp > 0) {
            break;
          }

          if (cqCmp >= 0) {
            break;
          }
        }
      }

    }

    if (count > 1) {
      MutableByteSequence trow, tcf, tcq, tcv;
      long tts;

      // when the current keys field is same as the last, then
      // set the prev keys field the same as the current key
      trow = (fieldsSame & ROW_SAME) == ROW_SAME ? row : prow;
      tcf = (fieldsSame & CF_SAME) == CF_SAME ? cf : pcf;
      tcq = (fieldsSame & CQ_SAME) == CQ_SAME ? cq : pcq;
      tcv = (fieldsSame & CV_SAME) == CV_SAME ? cv : pcv;
      tts = (fieldsSame & TS_SAME) == TS_SAME ? ts : pts;

      newPrevKey = new Key(trow.getBackingArray(), trow.offset(), trow.length(),
          tcf.getBackingArray(), tcf.offset(), tcf.length(), tcq.getBackingArray(), tcq.offset(),
          tcq.length(), tcv.getBackingArray(), tcv.offset(), tcv.length(), tts);
      newPrevKey.setDeleted(pdel);
    } else if (count == 1) {
      if (currKey != null) {
        newPrevKey = currKey;
      } else {
        newPrevKey = prevKey;
      }
    } else {
      throw new IllegalStateException();
    }

    PushdownRelativeKey result = new PushdownRelativeKey();
    result.key = new Key(row.getBackingArray(), row.offset(), row.length(), cf.getBackingArray(),
        cf.offset(), cf.length(), cq.getBackingArray(), cq.offset(), cq.length(),
        cv.getBackingArray(), cv.offset(), cv.length(), ts);
    result.key.setDeleted((fieldsSame & DELETED) != 0);
    result.prevKey = result.key;

    return new SkippedRelativeKey(result, count, newPrevKey, keyPredicate);
  }

  protected static void readPrefix(DataInput in, MutableByteSequence dest,
                                   ByteSequence prefixSource, boolean skipKey) throws IOException {
    int prefixLen = WritableUtils.readVInt(in);
    int remainingLen = WritableUtils.readVInt(in);
    if (skipKey) {
      in.skipBytes(remainingLen);
      return;
    }
    int len = prefixLen + remainingLen;
    if (dest.getBackingArray().length < len) {
      dest.setArray(new byte[UnsynchronizedBuffer.nextArraySize(len)], 0, 0);
    }
    if (prefixSource.isBackedByArray()) {
      System.arraycopy(prefixSource.getBackingArray(), prefixSource.offset(),
              dest.getBackingArray(), 0, prefixLen);
    } else {
      byte[] prefixArray = prefixSource.toArray();
      System.arraycopy(prefixArray, 0, dest.getBackingArray(), 0, prefixLen);
    }
    // read remaining
    in.readFully(dest.getBackingArray(), prefixLen, remainingLen);
    dest.setLength(len);
  }

  protected static void read(DataInput in, MutableByteSequence mbseq, boolean skip) throws IOException {
    int len = WritableUtils.readVInt(in);
    if (skip){
      in.skipBytes(len);
      return;
    }
    read(in, mbseq, len);
  }

  public void resetFilters() {
    this.lastRowCfFIltered=false;
    this.lastKeyCvFiltered=false;
    this.lastKeyRowFiltered=false;
  }
}
