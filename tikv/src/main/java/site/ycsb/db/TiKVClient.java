/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import com.pingcap.tikv.common.TiSession;
import com.pingcap.tikv.raw.RawKVClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class TiKVClient extends DB {

  private RawKVClient rawClient;

  public static final String PD_ADDRESS = "pd.address";

  public void init() throws DBException {
    Properties props = getProperties();
    
    String pdAddress = props.getProperty(PD_ADDRESS);
    TiConfiguration conf = TiConfiguration.createRawDefault(pdAddress);
    TiSession session = TiSession.create(conf);

    rawClient = session.createRawKVClient();
  }

  public void cleanup() throws DBException {
    try {
      ((Closeable) rawClient).close();
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      rawClient.get(ByteString.copyFromUtf8(table + "_" + key));
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    System.err.printf("table, key, values = %s, %s, %s \n", table, key, StringByteIterator.getStringMap(values));
    try {
      rawClient.put(ByteString.copyFromUtf8(table + "_" + key), StringByteIterator.getStringMap(values));
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      rawClient.delete(ByteString.copyFromUtf8(table + "_" + key));
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.OK;
  }

}
