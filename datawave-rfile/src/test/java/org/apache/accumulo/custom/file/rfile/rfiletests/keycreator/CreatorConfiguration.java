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
package org.apache.accumulo.custom.file.rfile.rfiletests.keycreator;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.accumulo.custom.file.rfile.rfiletests.FieldInjector;
import org.apache.accumulo.custom.file.rfile.uids.HashUID;
import org.apache.accumulo.custom.file.rfile.uids.UID;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.shaded.com.google.common.collect.Lists;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * Not thread safe
 */
public class CreatorConfiguration {

  private final int shardCount;

  Multimap<String,UID> authToDocId = ArrayListMultimap.create();
  public SortedSet<UID> uuidHashSet = new TreeSet<>();

  public SortedSet<String> indexedFieldNames = new TreeSet<>();
  public Multimap<String,FieldInjector> injectedMappings = ArrayListMultimap.create();

  public SortedSet<Text> fieldValues = new TreeSet<>();

  public SortedSet<Text> fieldValuesWithStringShards = new TreeSet<>();

  public SortedSet<String> dataTypes = new TreeSet<>();

  public Map<String,List<UID>> shardMapping = new HashMap<>();

  public Multimap<Text,UID> fieldValueToDoc = ArrayListMultimap.create();

  public Multimap<UID,Text> docToAuthMap = ArrayListMultimap.create();

  public Map<Text,FieldInjector> fvToFieldName = new HashMap<>();
  public Map<Text,FieldInjector> fvToFieldNameWithShard = new HashMap<>();

  public Map<String,TreeSet<Text>> shardToAuthList = new HashMap<>();

  public CreatorConfiguration(HashSet<String> dataTypes, HashSet<String> indexedFieldNames,
      int shardCount) {
    this.shardCount = shardCount;
    this.dataTypes = new TreeSet<>(dataTypes);
    this.indexedFieldNames = new TreeSet<>(indexedFieldNames);
    shardMapping = new HashMap<>();
  }

  List<String> shards = new ArrayList<>();
  boolean generated = false;
  protected long numberDocs = 1000;

  protected long numberOfFieldValues = 2;
  protected List<AuthEstimate> auths = new ArrayList<>();

  protected Text defaultAuth;

  public void injectum() {
    if (!generated) {
      for (var injected : injectedMappings.values()) {
        fieldValues.add(injected.fieldValue);
        fvToFieldName.put(injected.fieldValue, injected);
      }

      // fieldValuesWithStringShards.addAll(fieldValues);

      for (var injected : injectedMappings.values()) {
        for (String myShard : shardMapping.keySet()) {
          Text nfv = new Text(injected.fieldValue + GlobalIndexKeyCreator.NULL + myShard);
          fieldValuesWithStringShards.add(nfv);
          fvToFieldNameWithShard.put(nfv, injected);
        }
      }

      for (var injected : fieldValues) {
        for (String myShard : shardMapping.keySet()) {
          Text nfv = new Text(injected + GlobalIndexKeyCreator.NULL + myShard);
          fieldValuesWithStringShards.add(nfv);

        }
      }
    }
  }

  public byte[] longToBytes(long x) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(x);
    return buffer.array();
  }

  public void generate() {
    if (!generated) {
      generateFieldValues();
      generateUids(numberDocs);
      injectum();
      generated = true;
    }
  }

  /**
   * Generate half number values and half uuid strings.
   */
  public void generateFieldValues() {
    for (long i = 0; i < numberOfFieldValues / 2; i++) {
      fieldValues.add(new Text(longToBytes(i)));
    }
    for (long i = 0; i < numberOfFieldValues / 2; i++) {
      fieldValues.add(new Text(UUID.randomUUID().toString()));
    }
  }

  void generateUids(long number) {
    // we want these evenly distributed
    Multimap<Long,Text> numberToAuthMapping = ArrayListMultimap.create();
    for (AuthEstimate auth : auths) {
      // let's say we have .10 of 1000 docs, we then
      // want 100. 1000 / 100 = 10, so every 10
      long countOfTotal = (long) (auth.percentage * number);
      long spacing = number / countOfTotal;
      numberToAuthMapping.put(spacing, new Text(auth.auth));
    }
    for (int i = 0; i < number; i++) {
      UID docId =
          new HashUID(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8), (Date) null);
      this.uuidHashSet.add(docId);
      for (Long num : numberToAuthMapping.keySet()) {
        if ((i % num) == 0) {
          docToAuthMap.putAll(docId, numberToAuthMapping.get(num));
        }
      }
    }

    // now we want to create a mapping from shard to UID.
    // we'll evenly distribute throughout the shards.
    int numberPartitions = shards.size() * shardCount;
    int docsPerShard = (int) (number / numberPartitions);
    List<UID> docList = this.uuidHashSet.stream().collect(Collectors.toList());
    List<List<UID>> partitions = Lists.partition(docList, docsPerShard);

    // distribute docs per shard amongst

    Random rand = new Random();

    for (var fv : fieldValues) {
      // bound it.
      var rnd = rand.nextInt(20);
      fieldValueToDoc.putAll(fv, getRandomDocs(docList, rnd));
    }

    for (var fv : injectedMappings.values()) {
      // bound it.
      var rnd = rand.nextInt(20);
      fieldValueToDoc.putAll(fv.fieldValue, getRandomDocs(docList, rnd));
    }

    int i = 0;
    for (var uidList : partitions) {
      String shardStart = shards.get(rand.nextInt(shards.size()));
      String newShard = shardStart + "_" + String.format("%03d", rand.nextInt(shardCount));
      for (var uid : uidList) {
        var cvList = docToAuthMap.get(uid);
        for (var cv : cvList) {
          authToDocId.put(cv.toString(), uid);
        }
      }
      shardMapping.put(newShard, uidList);

    }

    /*
     * var fvStr = fv.toString();
     *
     * if (fvStr.length() <= 12){ continue; } var txtFv = new Text(
     * fvStr.substring(0,fvStr.length()-13)); var myShard = fvStr.substring(fvStr.length() - 12);
     *
     * var uidListForShard = config.shardMapping.get(myShard);
     */

    for (String myShard : shardMapping.keySet()) {
      var uidListForShard = shardMapping.get(myShard);
      TreeSet<Text> cvs = new TreeSet<>();
      for (var uid : uidListForShard) {
        var cvList = docToAuthMap.get(uid);
        for (var cv : cvList) {
          cvs.add(cv);
          // mymapping.put(cv,uid);
        }
      }
      shardToAuthList.put(myShard, cvs);
    }

    for (var fv : fvToFieldNameWithShard.keySet()) {
      var fvStr = fv.toString();

      if (fvStr.length() <= 12) {
        continue;
      }
      var txtFv = new Text(fvStr.substring(0, fvStr.length() - 13));
      var myShard = fvStr.substring(fvStr.length() - 12);
      var uidListForShard = shardMapping.get(myShard);
      TreeSet<Text> cvs = new TreeSet<>();
      for (var uid : uidListForShard) {
        var cvList = docToAuthMap.get(uid);
        for (var cv : cvList) {
          cvs.add(cv);
          // mymapping.put(cv,uid);
        }
      }
      shardToAuthList.put(myShard, cvs);
    }

  }

  public List<UID> getRandomDocs(List<UID> docs, int docCount) {
    Random rand = new Random();
    List<UID> newList = new ArrayList<>();
    for (int i = 0; i < docCount; i++) {
      int randomIndex = rand.nextInt(docs.size());

      newList.add(docs.get(randomIndex));
    }
    return newList;
  }

  public static class AuthEstimate {
    public double percentage; // from 0 to 1
    public String auth;

    public AuthEstimate(double percentage, String auth) {
      this.percentage = percentage;
      this.auth = auth;
    }
  }

  public void addInjectedField(String fieldName, String fieldValue, String auth,
      long numberOfDocs) {
    injectedMappings.put(fieldName.toUpperCase(),
        new FieldInjector(fieldName.toUpperCase(), fieldValue, auth, numberOfDocs));
  }

  public void addAuths(List<AuthEstimate> auths, String defaultAuth) {
    this.auths = auths;
    this.defaultAuth = new Text(defaultAuth);
  }

  public void setNumDocuments(long docs) {
    this.numberDocs = docs;
  }

  public void setNumFieldValues(long numberOfFieldValues) {
    this.numberOfFieldValues = numberOfFieldValues;
  }

  public void addShard(String shard) {
    shards.add(shard);
  }

}
