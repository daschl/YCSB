/**
 * Copyright (c) 2013 Yahoo! Inc. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.env.resources.IoPoolShutdownHook;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonFactory;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonGenerator;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.deps.io.netty.channel.DefaultSelectStrategyFactory;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.SelectStrategy;
import com.couchbase.client.deps.io.netty.channel.SelectStrategyFactory;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.couchbase.client.deps.io.netty.util.IntSupplier;
import com.couchbase.client.deps.io.netty.util.concurrent.DefaultThreadFactory;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.transcoder.JacksonTransformers;
import com.couchbase.client.java.util.Blocking;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import rx.Observable;
import rx.Subscriber;

import java.io.StringWriter;
import java.io.Writer;
import java.nio.channels.spi.SelectorProvider;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * A class that wraps the 2.x CouchbaseClient to allow it to be interfaced with YCSB.
 * This class extends {@link DB} and implements the database interface used by YCSB client.
 *
 * <p> The following options must be passed when using this database client.
 *
 * <ul>
 * <li><b>couchbase.host=127.0.0.1</b> The hostname from one server.</li>
 * <li><b>couchbase.bucket=default</b> The bucket name to use./li>
 * <li><b>couchbase.password=</b> The password of the bucket.</li>
 * <li><b>couchbase.syncMutationResponse=true</b> If mutations should wait for the response to complete.</li>
 * <li><b>couchbase.persistTo=0</b> Persistence durability requirement</li>
 * <li><b>couchbase.replicateTo=0</b> Replication durability requirement</li>
 * <li><b>couchbase.upsert=false</b> Use upsert instead of insert or replace.</li>
 * <li><b>couchbase.adhoc=false</b> If set to true, prepared statements are not used.</li>
 * <li><b>couchbase.kv=true</b> If set to false, mutation operations will also be performed through N1QL.</li>
 * <li><b>couchbase.maxParallelism=1</b> The server parallelism for all n1ql queries.</li>
 * <li><b>couchbase.kvEndpoints=1</b> The number of KV sockets to open per server.</li>
 * <li><b>couchbase.queryEndpoints=5</b> The number of N1QL Query sockets to open per server.</li>
 * </ul>
 *
 * @author Michael Nitschinger
 */
public class Couchbase2Client extends DB {

  private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Couchbase2Client.class);
  private static final Object INIT_COORDINATOR = new Object();

  private static volatile CouchbaseEnvironment env = null;

  private Cluster cluster;
  private Bucket bucket;
  private String bucketName;
  private boolean upsert;
  private PersistTo persistTo;
  private ReplicateTo replicateTo;
  private boolean syncMutResponse;
  private boolean epoll;
  private long kvTimeout;
  private boolean adhoc;
  private boolean kv;
  private int maxParallelism;
  private String host;
  private int kvEndpoints;
  private int queryEndpoints;
  private boolean boost;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    host = props.getProperty("couchbase.host", "127.0.0.1");
    bucketName = props.getProperty("couchbase.bucket", "default");
    String bucketPassword = props.getProperty("couchbase.password", "");

    upsert = props.getProperty("couchbase.upsert", "false").equals("true");
    persistTo = parsePersistTo(props.getProperty("couchbase.persistTo", "0"));
    replicateTo = parseReplicateTo(props.getProperty("couchbase.replicateTo", "0"));
    syncMutResponse = props.getProperty("couchbase.syncMutationResponse", "true").equals("true");
    adhoc = props.getProperty("couchbase.adhoc", "false").equals("true");
    kv = props.getProperty("couchbase.kv", "true").equals("true");
    maxParallelism = Integer.parseInt(props.getProperty("couchbase.maxParallelism", "1"));
    kvEndpoints = Integer.parseInt(props.getProperty("couchbase.kvEndpoints", "1"));
    queryEndpoints = Integer.parseInt(props.getProperty("couchbase.queryEndpoints", "5"));
    epoll = props.getProperty("couchbase.epoll", "false").equals("true");
    boost = props.getProperty("couchbase.boost", "false").equals("true");

    try {
      synchronized (INIT_COORDINATOR) {
        if (env == null) {
          DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment
              .builder()
              .queryEndpoints(queryEndpoints)
              .callbacksOnIoPool(true)
              .kvEndpoints(kvEndpoints);

          // allow to tune boosting and epoll down here
          // little work needs to be done to set all the other common defaults like thread name and pool
          // size to be sane and still configurable
          SelectStrategyFactory factory = boost ?
              new BackoffSelectStrategyFactory() : DefaultSelectStrategyFactory.INSTANCE;

          int poolSize = Integer.parseInt(
              System.getProperty("com.couchbase.ioPoolSize", Integer.toString(DefaultCoreEnvironment.IO_POOL_SIZE))
          );
          ThreadFactory threadFactory = new DefaultThreadFactory("cb-io", true);

          EventLoopGroup group = epoll ? new EpollEventLoopGroup(poolSize, threadFactory, factory)
              : new NioEventLoopGroup(poolSize, threadFactory, SelectorProvider.provider(), factory);
          builder.ioPool(group, new IoPoolShutdownHook(group));

          env = builder.build();
          logParams();
        }
      }

      cluster = CouchbaseCluster.create(env, host);
      bucket = cluster.openBucket(bucketName, bucketPassword);
      kvTimeout = env.kvTimeout();
    } catch (Exception ex) {
      throw new DBException("Could not connect to Couchbase Bucket.", ex);
    }

    if (!kv && !syncMutResponse) {
      throw new DBException("Not waiting for N1QL responses on mutations not yet implemented.");
    }
  }

  private void logParams() {
    StringBuilder sb = new StringBuilder();

    sb.append("host = ").append(host);
    sb.append(", bucket = ").append(bucketName);
    sb.append(", upsert = ").append(upsert);
    sb.append(", persistTo = ").append(persistTo);
    sb.append(", replicateTo = ").append(replicateTo);
    sb.append(", syncMutResponse = ").append(syncMutResponse);
    sb.append(", adhoc = ").append(adhoc);
    sb.append(", kv = ").append(kv);
    sb.append(", maxParallelism = ").append(maxParallelism);
    sb.append(", queryEndpoints = ").append(queryEndpoints);
    sb.append(", kvEndpoints = ").append(kvEndpoints);
    sb.append(", queryEndpoints = ").append(queryEndpoints);
    sb.append(", epoll = ").append(epoll);
    sb.append(", boost = ").append(boost);

    LOGGER.info("=== Using Params: " + sb.toString());
  }

  @Override
  public Status read(final String table, final String key, Set<String> fields,
      final HashMap<String, ByteIterator> result) {
    try {
      String docId = formatId(table, key);
      if (kv) {
        return readKv(docId, fields, result);
      } else {
        return readN1ql(docId, fields, result);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status readKv(String docId, Set<String> fields, final HashMap<String, ByteIterator> result)
    throws Exception {
    RawJsonDocument loaded = bucket.get(docId, RawJsonDocument.class);
    if (loaded == null) {
      return Status.NOT_FOUND;
    }
    decode(loaded.content(), fields, result);
    return Status.OK;
  }

  private Status readN1ql(String docId, Set<String> fields, final HashMap<String, ByteIterator> result)
    throws Exception {
    String readQuery = "SELECT " + joinFields(fields) + " FROM `" + bucketName + "` USE KEYS [$1]";
    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        readQuery,
        JsonArray.from(docId),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new DBException("Error while parsing N1QL Result. Query: " + readQuery
        + ", Errors: " + queryResult.errors());
    }

    N1qlQueryRow row;
    try {
      row = queryResult.rows().next();
    } catch (NoSuchElementException ex) {
      return Status.NOT_FOUND;
    }

    JsonObject content = row.value();
    if (fields == null) {
      content = content.getObject(bucketName); // n1ql result set scoped under *.bucketName
      fields = content.getNames();
    }

    for (String field : fields) {
      Object value = content.get(field);
      result.put(field, new StringByteIterator(value != null ? value.toString() : ""));
    }

    return Status.OK;
  }

  @Override
  public Status update(final String table, final String key, final HashMap<String, ByteIterator> values) {
    if (upsert) {
      return upsert(table, key, values);
    }

    try {
      String docId = formatId(table, key);
      if (kv) {
        return updateKv(docId, values);
      } else {
        return updateN1ql(docId, values);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status updateKv(final String docId, final HashMap<String, ByteIterator> values) {
    waitForMutationResponse(bucket.async().replace(
        RawJsonDocument.create(docId, encode(values)),
        persistTo,
        replicateTo
    ));
    return Status.OK;
  }

  private Status updateN1ql(final String docId, final HashMap<String, ByteIterator> values)
    throws Exception {
    String fields = encodeN1qlFields(values);
    String updateQuery = "UPDATE `" + bucketName + "` USE KEYS [$1] SET " + fields;

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        updateQuery,
        JsonArray.from(docId),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new DBException("Error while parsing N1QL Result. Query: " + updateQuery
        + ", Errors: " + queryResult.errors());
    }
    return Status.OK;
  }

  @Override
  public Status insert(final String table, final String key, final HashMap<String, ByteIterator> values) {
    if (upsert) {
      return upsert(table, key, values);
    }

    try {
      String docId = formatId(table, key);
      if (kv) {
        return insertKv(docId, values);
      } else {
        return insertN1ql(docId, values);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status insertKv(final String docId, final HashMap<String, ByteIterator> values) {
    // During "load" phase, make sure to throttle the insert load when its going too fast
    // for the server. All other errors still bubble up directly without further retries.
    // NOTE: only works when
    int tries = 60; // roughly 60 seconds with the 1 second sleep, not 100% accurate.

    for(int i = 0; i < tries; i++) {
      try {
        waitForMutationResponse(bucket.async().insert(
            RawJsonDocument.create(docId, encode(values)),
            persistTo,
            replicateTo
        ));
        return Status.OK;
      } catch (TemporaryFailureException ex) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while sleeping on TMPFAIL backoff.", ex);
        }
      }
    }

    throw new RuntimeException("Still receiving TMPFAIL from the server after trying " + tries + " times. " +
      "Check your server.");
  }

  private Status insertN1ql(final String docId, final HashMap<String, ByteIterator> values)
    throws Exception {
    String insertQuery = "INSERT INTO `" + bucketName + "`(KEY,VALUE) VALUES ($1,$2)";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        insertQuery,
        JsonArray.from(docId, encodeIntoJson(values)),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new DBException("Error while parsing N1QL Result. Query: " + insertQuery
        + ", Errors: " + queryResult.errors());
    }
    return Status.OK;
  }

  private Status upsert(String table, String key, HashMap<String, ByteIterator> values) {
    try {
      String docId = formatId(table, key);
      if (kv) {
        return upsertKv(docId, values);
      } else {
        return upsertN1ql(docId, values);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status upsertKv(String docId, HashMap<String, ByteIterator> values) {
    waitForMutationResponse(bucket.async().upsert(
        RawJsonDocument.create(docId, encode(values)),
        persistTo,
        replicateTo
    ));
    return Status.OK;
  }

  private Status upsertN1ql(String docId, HashMap<String, ByteIterator> values)
    throws Exception {
    String upsertQuery = "UPSERT INTO `" + bucketName + "`(KEY,VALUE) VALUES ($1,$2)";

    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        upsertQuery,
        JsonArray.from(docId, encodeIntoJson(values)),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new DBException("Error while parsing N1QL Result. Query: " + upsertQuery
        + ", Errors: " + queryResult.errors());
    }
    return Status.OK;
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      String docId = formatId(table, key);
      if (kv) {
        return deleteKv(docId);
      } else {
        return deleteN1ql(docId);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private Status deleteKv(String docId) {
    waitForMutationResponse(bucket.async().remove(
        docId,
        persistTo,
        replicateTo
    ));
    return Status.OK;
  }

  private Status deleteN1ql(String docId) throws Exception {
    String deleteQuery = "DELETE FROM `" + bucketName + "` USE KEYS [$1]";
    N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
        deleteQuery,
        JsonArray.from(docId),
        N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
    ));

    if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
      throw new DBException("Error while parsing N1QL Result. Query: " + deleteQuery
        + ", Errors: " + queryResult.errors());
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    try {
      String scanQuery = "SELECT " + joinFields(fields) + " FROM `" + bucketName + "` WHERE meta().id >= '$1' LIMIT $2";
      N1qlQueryResult queryResult = bucket.query(N1qlQuery.parameterized(
          scanQuery,
          JsonArray.from(formatId(table, startkey), recordcount),
          N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
      ));

      if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
        throw new DBException("Error while parsing N1QL Result. Query: " + scanQuery
          + ", Errors: " + queryResult.errors());
      }

      boolean allFields = fields == null || fields.isEmpty();
      result.ensureCapacity(recordcount);

      for (N1qlQueryRow row : queryResult) {
        JsonObject value = row.value();
        if (fields == null) {
          value = value.getObject(bucketName);
        }
        Set<String> f = allFields ? value.getNames() : fields;
        HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(f.size());
        for (String field : f) {
          tuple.put(field, new StringByteIterator(value.getString(field)));
        }
        result.add(tuple);
      }
      return Status.OK;
    } catch (Exception ex) {
      ex.printStackTrace();
      return Status.ERROR;
    }
  }

  private void waitForMutationResponse(final Observable<? extends Document<?>> input) {
    if (!syncMutResponse) {
      input.subscribe(new Subscriber<Document<?>>() {
        @Override
        public void onCompleted() {
        }

        @Override
        public void onError(Throwable e) {
        }

        @Override
        public void onNext(Document<?> document) {
        }
      });
    } else {
      Blocking.blockForSingle(input, kvTimeout, TimeUnit.MILLISECONDS);
    }
  }

  private static String encodeN1qlFields(final HashMap<String, ByteIterator> values) {
    if (values.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String raw = entry.getValue().toString();
      String escaped = raw.replace("\"", "\\\"").replace("\'", "\\\'");
      sb.append(entry.getKey()).append("=\"").append(escaped).append("\" ");
    }
    String toReturn = sb.toString();
    return toReturn.substring(0, toReturn.length() - 1);
  }

  private static JsonObject encodeIntoJson(final HashMap<String, ByteIterator> values) {
    JsonObject result = JsonObject.create();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      result.put(entry.getKey(), entry.getValue().toString());
    }
    return result;
  }

  private static String joinFields(final Set<String> fields) {
    if (fields == null || fields.isEmpty()) {
      return "*";
    }
    StringBuilder builder = new StringBuilder();
    for (String f : fields) {
      builder.append("`").append(f).append("`").append(",");
    }
    String toReturn = builder.toString();
    return toReturn.substring(0, toReturn.length() - 1);
  }

  private static String formatId(final String prefix, final String key) {
    return prefix + ":" + key;
  }

  private static ReplicateTo parseReplicateTo(final String property) throws DBException {
    int value = Integer.parseInt(property);

    switch (value) {
    case 0:
      return ReplicateTo.NONE;
    case 1:
      return ReplicateTo.ONE;
    case 2:
      return ReplicateTo.TWO;
    case 3:
      return ReplicateTo.THREE;
    default:
      throw new DBException("\"couchbase.replicateTo\" must be between 0 and 3");
    }
  }

  private static PersistTo parsePersistTo(final String property) throws DBException {
    int value = Integer.parseInt(property);

    switch (value) {
    case 0:
      return PersistTo.NONE;
    case 1:
      return PersistTo.ONE;
    case 2:
      return PersistTo.TWO;
    case 3:
      return PersistTo.THREE;
    case 4:
      return PersistTo.FOUR;
    default:
      throw new DBException("\"couchbase.persistTo\" must be between 0 and 4");
    }
  }

  /**
   * Decode the object from server into the storable result.
   *
   * @param source the loaded object.
   * @param fields the fields to check.
   * @param dest the result passed back to the ycsb core.
   */
  private void decode(final String source, final Set<String> fields,
                      final HashMap<String, ByteIterator> dest) {
    try {
      JsonNode json = JacksonTransformers.MAPPER.readTree(source);
      boolean checkFields = fields != null && !fields.isEmpty();
      for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields(); jsonFields.hasNext();) {
        Map.Entry<String, JsonNode> jsonField = jsonFields.next();
        String name = jsonField.getKey();
        if (checkFields && fields.contains(name)) {
          continue;
        }
        JsonNode jsonValue = jsonField.getValue();
        if (jsonValue != null && !jsonValue.isNull()) {
          dest.put(name, new StringByteIterator(jsonValue.asText()));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not decode JSON");
    }
  }

  /**
   * Encode the object for couchbase storage.
   *
   * @param source the source value.
   * @return the storable object.
   */
  private String encode(final HashMap<String, ByteIterator> source) {
    HashMap<String, String> stringMap = StringByteIterator.getStringMap(source);
    ObjectNode node = JacksonTransformers.MAPPER.createObjectNode();
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    try {
      JsonGenerator jsonGenerator = jsonFactory.createGenerator(writer);
      JacksonTransformers.MAPPER.writeTree(jsonGenerator, node);
    } catch (Exception e) {
      throw new RuntimeException("Could not encode JSON value");
    }
    return writer.toString();
  }
}

class BackoffSelectStrategyFactory implements SelectStrategyFactory {
  @Override
  public SelectStrategy newSelectStrategy() {
    return new BackoffSelectStrategy();
  }
}

class BackoffSelectStrategy implements SelectStrategy {

  private int counter = 0;

  @Override
  public int calculateStrategy(final IntSupplier supplier, final boolean hasTasks) throws Exception {
    int selectNowResult = supplier.get();
    if (hasTasks || selectNowResult != 0) {
      counter = 0;
      return selectNowResult;
    }
    counter++;

    if (counter > 2000) {
      LockSupport.parkNanos(1);
    } else if (counter > 3000) {
      Thread.yield();
    } else if (counter > 4000) {
      LockSupport.parkNanos(1000);
    } else if (counter > 5000) {
      // defer to blocking select
      counter = 0;
      return SelectStrategy.SELECT;
    }

    return SelectStrategy.CONTINUE;
  }
}
