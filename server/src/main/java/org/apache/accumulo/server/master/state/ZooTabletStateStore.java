/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.master.state;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.accumulo.server.util.MetadataTable;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

public class ZooTabletStateStore extends TabletStateStore {

  private static final Logger log = Logger.getLogger(ZooTabletStateStore.class);
  final private DistributedStore store;

  public ZooTabletStateStore(DistributedStore store) {
    this.store = store;
  }

  public ZooTabletStateStore() throws DistributedStoreException {
    try {
      store = new ZooStore();
    } catch (IOException ex) {
      throw new DistributedStoreException(ex);
    }
  }

  @Override
  public Iterator<TabletLocationState> iterator() {
    return new Iterator<TabletLocationState>() {
      boolean finished = false;

      @Override
      public boolean hasNext() {
        return !finished;
      }

      @Override
      public TabletLocationState next() {
        finished = true;
        try {
          byte[] future = store.get(Constants.ZROOT_TABLET_FUTURE_LOCATION);
          byte[] current = store.get(Constants.ZROOT_TABLET_LOCATION);
          byte[] last = store.get(Constants.ZROOT_TABLET_LAST_LOCATION);

          TServerInstance currentSession = null;
          TServerInstance futureSession = null;
          TServerInstance lastSession = null;

          if (future != null)
            futureSession = parse(future);

          if (last != null)
            lastSession = parse(last);

          if (current != null) {
            currentSession = parse(current);
            futureSession = null;
          }
          List<Collection<String>> logs = new ArrayList<Collection<String>>();
          for (String entry : store.getChildren(Constants.ZROOT_TABLET_WALOGS)) {
            byte[] logInfo = store.get(Constants.ZROOT_TABLET_WALOGS + "/" + entry);
            if (logInfo != null) {
              MetadataTable.LogEntry logEntry = new MetadataTable.LogEntry();
              logEntry.fromBytes(logInfo);
              logs.add(logEntry.logSet);
              log.debug("root tablet logSet " + logEntry.logSet);
            }
          }
          TabletLocationState result = new TabletLocationState(Constants.ROOT_TABLET_EXTENT, futureSession, currentSession, lastSession, logs, false);
          log.debug("Returning root tablet state: " + result);
          return result;
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }

      @Override
      public void remove() {
        throw new NotImplementedException();
      }
    };
  }

  protected TServerInstance parse(byte[] current) {
    String str = new String(current, UTF_8);
    String[] parts = str.split("[|]", 2);
    InetSocketAddress address = AddressUtil.parseAddress(parts[0], 0);
    if (parts.length > 1 && parts[1] != null && parts[1].length() > 0) {
      return new TServerInstance(address, parts[1]);
    } else {
      // a 1.2 location specification: DO NOT WANT
      return null;
    }
  }

  @Override
  public void setFutureLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    if (assignments.size() != 1)
      throw new IllegalArgumentException("There is only one root tablet");
    Assignment assignment = assignments.iterator().next();
    if (assignment.tablet.compareTo(Constants.ROOT_TABLET_EXTENT) != 0)
      throw new IllegalArgumentException("You can only store the root tablet location");
    String value = AddressUtil.toString(assignment.server.getLocation()) + "|" + assignment.server.getSession();
    Iterator<TabletLocationState> currentIter = iterator();
    TabletLocationState current = currentIter.next();
    if (current.current != null) {
      throw new IllegalDSException("Trying to set the root tablet location: it is already set to " + current.current);
    }
    store.put(Constants.ZROOT_TABLET_FUTURE_LOCATION, value.getBytes(UTF_8));
  }

  @Override
  public void setLocations(Collection<Assignment> assignments) throws DistributedStoreException {
    if (assignments.size() != 1)
      throw new IllegalArgumentException("There is only one root tablet");
    Assignment assignment = assignments.iterator().next();
    if (assignment.tablet.compareTo(Constants.ROOT_TABLET_EXTENT) != 0)
      throw new IllegalArgumentException("You can only store the root tablet location");
    String value = AddressUtil.toString(assignment.server.getLocation()) + "|" + assignment.server.getSession();
    Iterator<TabletLocationState> currentIter = iterator();
    TabletLocationState current = currentIter.next();
    if (current.current != null) {
      throw new IllegalDSException("Trying to set the root tablet location: it is already set to " + current.current);
    }
    if (!current.future.equals(assignment.server)) {
      throw new IllegalDSException("Root tablet is already assigned to " + current.future);
    }
    store.put(Constants.ZROOT_TABLET_LOCATION, value.getBytes(UTF_8));
    store.put(Constants.ZROOT_TABLET_LAST_LOCATION, value.getBytes(UTF_8));
    // Make the following unnecessary by making the entire update atomic
    store.remove(Constants.ZROOT_TABLET_FUTURE_LOCATION);
    log.debug("Put down root tablet location");
  }

  @Override
  public void unassign(Collection<TabletLocationState> tablets) throws DistributedStoreException {
    if (tablets.size() != 1)
      throw new IllegalArgumentException("There is only one root tablet");
    TabletLocationState tls = tablets.iterator().next();
    if (tls.extent.compareTo(Constants.ROOT_TABLET_EXTENT) != 0)
      throw new IllegalArgumentException("You can only store the root tablet location");
    store.remove(Constants.ZROOT_TABLET_LOCATION);
    store.remove(Constants.ZROOT_TABLET_FUTURE_LOCATION);
    log.debug("unassign root tablet location");
  }

  @Override
  public String name() {
    return "Root Tablet";
  }

}
