/**
 * Copyright (c) 2013 Yahoo! Inc. All rights reserved.
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

package com.yahoo.ycsb.db;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.util.Blocking;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import rx.Observable;
import rx.Subscriber;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

/**
 * A class that wraps the 1.x CouchbaseClient to allow it to be interfaced with YCSB.
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
 * </ul>
 *
 * @author Michael Nitschinger
 */
public class Couchbase2Client extends DB {

    private static final Object INIT_COORDINATOR = new Object();
    private static volatile int NUM_THREADS = 0;
    private static volatile CouchbaseEnvironment ENV = null;
    private static volatile Cluster CLUSTER = null;
    private static volatile Bucket BUCKET = null;

    public static final String HOST_PROPERTY = "couchbase.host";
    public static final String BUCKET_PROPERTY = "couchbase.bucket";
    public static final String PASSWORD_PROPERTY = "couchbase.password";
    public static final String SYNC_MUT_PROPERTY = "couchbase.syncMutationResponse";
    public static final String PERSIST_PROPERTY = "couchbase.persistTo";
    public static final String REPLICATE_PROPERTY = "couchbase.replicateTo";
    public static final String UPSERT_PROPERTY = "couchbase.upsert";
    public static final String ADHOC_PROPOERTY = "couchbase.adhoc";
    public static final String KV_PROPERTY = "couchbase.kv";
    public static final String MAX_PARALLEL_PROPERTY = "couchbase.maxParallelism";

    private String bucketName;
    private boolean upsert;
    private PersistTo persistTo;
    private ReplicateTo replicateTo;
    private boolean syncMutResponse;
    private long kvTimeout;
    private boolean adhoc;
    private boolean kv;
    private int maxParallelism;

    @Override
    public void init() throws DBException {
        Properties props = getProperties();

        String host = props.getProperty(HOST_PROPERTY, "127.0.0.1");
        bucketName = props.getProperty(BUCKET_PROPERTY, "default");
        String bucketPassword = props.getProperty(PASSWORD_PROPERTY, "");

        upsert = props.getProperty(UPSERT_PROPERTY, "false").equals("true");
        persistTo = parsePersistTo(props.getProperty(PERSIST_PROPERTY, "0"));
        replicateTo = parseReplicateTo(props.getProperty(REPLICATE_PROPERTY, "0"));
        syncMutResponse = props.getProperty(SYNC_MUT_PROPERTY, "true").equals("true");
        adhoc = props.getProperty(ADHOC_PROPOERTY, "false").equals("true");
        kv = props.getProperty(KV_PROPERTY, "true").equals("true");
        maxParallelism = Integer.parseInt(props.getProperty(MAX_PARALLEL_PROPERTY, "1"));

        try {
            synchronized (INIT_COORDINATOR) {
                NUM_THREADS++;
                if (ENV == null) {
                    ENV = DefaultCouchbaseEnvironment
                        .builder()
                        .queryEndpoints(5)
                        .build();
                }
                if (CLUSTER == null) {
                    CLUSTER = CouchbaseCluster.create(ENV, host);
                }
                if (BUCKET == null) {
                    BUCKET = CLUSTER.openBucket(bucketName, bucketPassword);
                }
            }

            kvTimeout = BUCKET.environment().kvTimeout();
        } catch (Exception ex) {
            throw new DBException("Could not connect to Couchbase Bucket.", ex);

        }

        if (!kv && !syncMutResponse) {
            throw new DBException("Not waiting for n1ql responses on mutation is not yet implemented.");
        }
    }

    @Override
    public void cleanup() throws DBException {
        synchronized (INIT_COORDINATOR) {
            NUM_THREADS--;
            if (NUM_THREADS == 0) {
                CLUSTER.disconnect();
                ENV.shutdownAsync().toBlocking().single();
            }
        }
    }

    @Override
    public Status read(final String table, final String key, Set<String> fields,
        final HashMap<String, ByteIterator> result) {
        try {
            JsonObject content;
            if (kv) {
                JsonDocument loaded = BUCKET.get(formatId(table, key));
                if (loaded == null) {
                    return Status.NOT_FOUND;
                }
                content = loaded.content();
            } else {
                String readQuery = "SELECT " + joinSet(bucketName, fields) + " FROM `"
                  + bucketName + "` USE KEYS [$1]";
                N1qlQueryResult queryResult = BUCKET.query(N1qlQuery.parameterized(
                  readQuery,
                  JsonArray.from(formatId(table, key)),
                  N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
                ));

                if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
                    System.err.println(readQuery);
                    System.err.println(queryResult.errors());
                    return Status.ERROR;
                }

                List<N1qlQueryRow> rows = queryResult.allRows();
                if (rows.isEmpty()) {
                    return Status.NOT_FOUND;
                }

                content = rows.get(0).value();
            }

            fields = fields == null || fields.isEmpty() ? content.getNames() : fields;
            for (String field : fields) {
                result.put(field, new StringByteIterator(content.getString(field)));
            }
            return Status.OK;
        } catch (Exception ex) {
            ex.printStackTrace();
            return Status.ERROR;
        }
    }

    @Override
    public Status update(final String table, final String key,
    final HashMap<String, ByteIterator> values) {
        try {
            if (kv) {
                if (upsert) {
                    return upsert(table, key, values);
                }

                waitForMutationResponse(BUCKET.async().replace(
                  JsonDocument.create(formatId(table, key), encodeIntoJson(values)),
                  persistTo,
                  replicateTo
                ));
            } else {
                String fields = encodeN1qlFields(values);
                String updateQuery = "UPDATE `" + bucketName + "` USE KEYS [$1] SET " + fields;

                N1qlQueryResult queryResult = BUCKET.query(N1qlQuery.parameterized(
                  updateQuery,
                  JsonArray.from(formatId(table, key)),
                  N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
                ));

                if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
                    System.err.println(updateQuery);
                    System.err.println(queryResult.errors());
                    return Status.ERROR;
                }
            }
            return Status.OK;
        } catch (Exception ex) {
            ex.printStackTrace();
            return Status.ERROR;
        }
    }

    private static String encodeN1qlFields(final HashMap<String, ByteIterator> values) {
        if (values.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            sb.append(entry.getKey()).append("=\"").append(entry.getValue().toString()).append("\",");
        }
        String toReturn = sb.toString();
        return toReturn.substring(0, toReturn.length() - 1);
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            if (kv) {
                if (upsert) {
                    return upsert(table, key, values);
                }

                waitForMutationResponse(BUCKET.async().insert(
                  JsonDocument.create(formatId(table, key), encodeIntoJson(values)),
                  persistTo,
                  replicateTo
                ));
            } else {
                String insertQuery = "INSERT INTO `" + bucketName + "`(KEY,VALUE) VALUES ($1,$2)";

                N1qlQueryResult queryResult = BUCKET.query(N1qlQuery.parameterized(
                  insertQuery,
                  JsonArray.from(formatId(table, key), encodeIntoJson(values)),
                  N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
                ));

                if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
                    System.err.println(insertQuery);
                    System.err.println(queryResult.errors());
                    return Status.ERROR;
                }
            }
            return Status.OK;
        } catch (Exception ex) {
            ex.printStackTrace();
            return Status.ERROR;
        }
    }

    private Status upsert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            if (kv) {
                waitForMutationResponse(BUCKET.async().upsert(
                  JsonDocument.create(formatId(table, key), encodeIntoJson(values)),
                  persistTo,
                  replicateTo
                ));
            } else {
                String upsertQuery = "UPSERT INTO `" + bucketName + "`(KEY,VALUE) VALUES ($1,$2)";

                N1qlQueryResult queryResult = BUCKET.query(N1qlQuery.parameterized(
                  upsertQuery,
                  JsonArray.from(formatId(table, key), encodeIntoJson(values)),
                  N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
                ));

                if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
                    System.err.println(upsertQuery);
                    System.err.println(queryResult.errors());
                    return Status.ERROR;
                }
            }
            return Status.OK;
        } catch (Exception ex) {
            ex.printStackTrace();
            return Status.ERROR;
        }
    }

    private void waitForMutationResponse(final Observable<JsonDocument> input) {
        if (!syncMutResponse) {
            input.subscribe(new Subscriber<JsonDocument>() {
                @Override
                public void onCompleted() {}

                @Override
                public void onError(Throwable e) {}

                @Override
                public void onNext(JsonDocument jsonDocument) {}
            });
        } else {
            Blocking.blockForSingle(input, kvTimeout, TimeUnit.MILLISECONDS);
        }
    }

    private static JsonObject encodeIntoJson(final HashMap<String, ByteIterator> values) {
        JsonObject result = JsonObject.create();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            result.put(entry.getKey(), entry.getValue().toString());
        }
        return result;
    }

    @Override
    public Status delete(final String table, final String key) {
        try {
            if (kv) {
                BUCKET.remove(formatId(table, key));
            } else {
                String deleteQuery = "DELETE FROM `" + bucketName + "` USE KEYS [$1]";
                N1qlQueryResult queryResult = BUCKET.query(N1qlQuery.parameterized(
                  deleteQuery,
                  JsonArray.from(formatId(table, key)),
                  N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
                ));

                if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
                    System.err.println(deleteQuery);
                    System.err.println(queryResult.errors());
                    return Status.ERROR;
                }
            }
            return Status.OK;
        } catch (Exception ex) {
            ex.printStackTrace();
            return Status.ERROR;
        }
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields,
        Vector<HashMap<String, ByteIterator>> result) {
        try {
            String scanQuery = "SELECT " + joinSet(bucketName, fields) + " FROM `"
              + bucketName + "` WHERE meta().id >= '$1' LIMIT $2";
            N1qlQueryResult queryResult = BUCKET.query(N1qlQuery.parameterized(
                scanQuery,
                JsonArray.from(formatId(table, startkey), recordcount),
                N1qlParams.build().adhoc(adhoc).maxParallelism(maxParallelism)
            ));

            if (!queryResult.parseSuccess() || !queryResult.finalSuccess()) {
                System.err.println(scanQuery);
                System.err.println(queryResult.errors());
                return Status.ERROR;
            }

            boolean allFields = fields == null || fields.isEmpty();
            result.ensureCapacity(recordcount);

            for (N1qlQueryRow row : queryResult) {
                JsonObject value = row.value();
                fields = allFields ? value.getNames() : fields;
                HashMap<String, ByteIterator> tuple = new HashMap<String, ByteIterator>(fields.size());
                for (String field : fields) {
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

    private static String joinSet(final String bucket, final Set<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return "`" + bucket + "`.*";
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
            case 0: return ReplicateTo.NONE;
            case 1: return ReplicateTo.ONE;
            case 2: return ReplicateTo.TWO;
            case 3: return ReplicateTo.THREE;
            default:
                throw new DBException(REPLICATE_PROPERTY + " must be between 0 and 3");
        }
    }

    private static PersistTo parsePersistTo(final String property) throws DBException {
        int value = Integer.parseInt(property);

        switch (value) {
            case 0: return PersistTo.NONE;
            case 1: return PersistTo.ONE;
            case 2: return PersistTo.TWO;
            case 3: return PersistTo.THREE;
            case 4: return PersistTo.FOUR;
            default:
                throw new DBException(PERSIST_PROPERTY + " must be between 0 and 4");
        }
    }
}
