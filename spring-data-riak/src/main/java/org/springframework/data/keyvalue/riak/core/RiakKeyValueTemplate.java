/*
 * Copyright (c) 2010 by J. Brisbin <jon@jbrisbin.com>
 *     Portions (c) 2010 by NPC International, Inc. or the
 *     original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.keyvalue.riak.core;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.keyvalue.riak.mapreduce.MapReduceJob;
import org.springframework.data.keyvalue.riak.mapreduce.MapReduceOperations;
import org.springframework.data.keyvalue.riak.mapreduce.RiakMapReduceJob;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.util.Assert;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * An implementation of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations}
 * and {@link org.springframework.data.keyvalue.riak.mapreduce.MapReduceOperations} for the Riak
 * data store.
 * <p/>
 * To use the RiakTemplate, create a singleton in your Spring application-context.xml:
 * <pre><code>
 * &lt;bean id="riak" class="org.springframework.data.keyvalue.riak.core.RiakTemplate"
 *     p:defaultUri="http://localhost:8098/riak/{bucket}/{key}"
 *     p:mapReduceUri="http://localhost:8098/mapred"/>
 * </code></pre>
 * To store and retrieve objects in Riak, use the setXXX and getXXX methods (example in
 * Groovy):
 * <pre><code>
 * def obj = new TestObject(name: "My Name", age: 40)
 * riak.set([bucket: "mybucket", key: "mykey"], obj)
 * ...
 * def name = riak.get([bucket: "mybucket", key: "mykey"]).name
 * println "Hello $name!"
 * </code></pre>
 * You're key object should be one of: <ul><li>A <code>String</code> encoding the bucket and key
 * together, separated by a colon. e.g. "mybucket:mykey"</li> <li>An implementation of
 * BucketKeyPair (like {@link org.springframework.data.keyvalue.riak.core.SimpleBucketKeyPair})</li>
 * <li>A <code>Map</code> with both a "bucket" and a "key" specified.</li> <li>A
 * <code>String</code> of only the key name, but specifying a bucket by using the {@link
 * org.springframework.data.keyvalue.riak.convert.KeyValueStoreMetaData} annotation on the
 * object you're storing.</li></ul>
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakKeyValueTemplate extends AbstractRiakTemplate implements KeyValueStoreOperations, MapReduceOperations, InitializingBean {

  protected RiakTemplate riak;

  /**
   * Take all the defaults.
   */
  public RiakKeyValueTemplate() {
    super();
    riak = new RiakTemplate();
  }

  /**
   * Use the specified {@link org.springframework.http.client.ClientHttpRequestFactory}.
   *
   * @param requestFactory
   */
  public RiakKeyValueTemplate(ClientHttpRequestFactory requestFactory) {
    super(requestFactory);
    riak = new RiakTemplate(requestFactory);
  }

  /**
   * Use the specified defaultUri and mapReduceUri.
   *
   * @param defaultUri
   * @param mapReduceUri
   */
  public RiakKeyValueTemplate(String defaultUri, String mapReduceUri) {
    setRestTemplate(new RestTemplate());
    this.setDefaultUri(defaultUri);
    this.mapReduceUri = mapReduceUri;
    this.riak = new RiakTemplate(defaultUri, mapReduceUri);
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    super.afterPropertiesSet();
    riak.afterPropertiesSet();
  }

  /*----------------- Set Operations -----------------*/

  public <K, V> KeyValueStoreOperations set(K key, V value) {
    return setWithMetaData(key, value, null, null);
  }

  public <K, V> KeyValueStoreOperations set(K key, V value, QosParameters qosParams) {
    return setWithMetaData(key, value, null, qosParams);
  }

  public <K> KeyValueStoreOperations setAsBytes(K key, byte[] value) {
    return setAsBytes(key, value, null);
  }

  public <K> KeyValueStoreOperations setAsBytes(K key, byte[] value, QosParameters qosParams) {
    Assert.notNull(key, "Key cannot be null!");
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, value);
    riak.setAsBytes(bucketKeyPair.getBucket(), bucketKeyPair.getKey(), value, qosParams);
    return this;
  }

  public <K, V> KeyValueStoreOperations setWithMetaData(K key, V value, Map<String, String> metaData, QosParameters qosParams) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, value);
    riak.setWithMetaData(bucketKeyPair.getBucket(),
        bucketKeyPair.getKey(),
        value,
        metaData,
        qosParams);
    return this;
  }

  public <K, V> KeyValueStoreOperations setWithMetaData(K key, V value, Map<String, String> metaData) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, value);
    riak.setWithMetaData(bucketKeyPair.getBucket(),
        bucketKeyPair.getKey(),
        value,
        metaData,
        null);
    return this;
  }

  /*----------------- Get Operations -----------------*/

  public <K> RiakMetaData getMetaData(K key) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    return riak.getMetaData(bucketKeyPair.getBucket(), bucketKeyPair.getKey());
  }

  public <K, T> RiakValue<T> getWithMetaData(K key, Class<T> requiredType) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    return riak.getWithMetaData(bucketKeyPair.getBucket(),
        bucketKeyPair.getKey(),
        requiredType);
  }

  @SuppressWarnings({"unchecked"})
  public <K, V> V get(K key) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    return (V) riak.get(bucketKeyPair.getBucket(), bucketKeyPair.getKey());
  }

  public <K> byte[] getAsBytes(K key) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    RiakValue<byte[]> obj = riak.getAsBytesWithMetaData(bucketKeyPair.getBucket(),
        bucketKeyPair.getKey());
    return (null != obj ? obj.get() : null);
  }

  public <K> RiakValue<byte[]> getAsBytesWithMetaData(K key) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    return riak.getAsBytesWithMetaData(bucketKeyPair.getBucket(), bucketKeyPair.getKey());
  }

  public <K, T> T getAsType(K key, Class<T> requiredType) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    return riak.getAsType(bucketKeyPair.getBucket(), bucketKeyPair.getKey(), requiredType);
  }

  public <K, V> V getAndSet(K key, V value) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    return riak.getAndSet(bucketKeyPair.getBucket(), bucketKeyPair.getKey(), value);
  }

  public <K> byte[] getAndSetAsBytes(K key, byte[] value) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    return riak.getAndSetAsBytes(bucketKeyPair.getBucket(), bucketKeyPair.getKey(), value);
  }

  public <K, V, T> T getAndSetAsType(K key, V value, Class<T> requiredType) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    return riak.getAndSetAsType(bucketKeyPair.getBucket(),
        bucketKeyPair.getKey(),
        value,
        requiredType);
  }

  @SuppressWarnings({"unchecked"})
  public <K, V> List<V> getValues(List<K> keys) {
    List<V> results = new ArrayList<V>();
    for (K key : keys) {
      BucketKeyPair bkp = resolveBucketKeyPair(key, null);
      results.add((V) riak.get(bkp.getBucket(), bkp.getKey()));
    }
    return results;
  }

  public <K, V> List<V> getValues(K... keys) {
    return getValues(keys);
  }

  public <K, T> List<T> getValuesAsType(List<K> keys, Class<T> requiredType) {
    List<T> results = new ArrayList<T>();
    for (K key : keys) {
      BucketKeyPair bkp = resolveBucketKeyPair(key, null);
      results.add(riak.getAsType(bkp.getBucket(), bkp.getKey(), requiredType));
    }
    return results;
  }

  public <T, K> List<T> getValuesAsType(Class<T> requiredType, K... keys) {
    return riak.getValuesAsType(requiredType, keys);
  }

  /*----------------- Only-Set-Once Operations -----------------*/

  public <K, V> KeyValueStoreOperations setIfKeyNonExistent(K key, V value) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    riak.setIfKeyNonExistent(bucketKeyPair.getBucket(), bucketKeyPair.getKey(), value);
    return this;
  }

  public <K> KeyValueStoreOperations setIfKeyNonExistentAsBytes(K key, byte[] value) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    riak.setIfKeyNonExistent(bucketKeyPair.getBucket(), bucketKeyPair.getKey(), value);
    return this;
  }

  /*----------------- Multiple Item Operations -----------------*/

  public <K, V> KeyValueStoreOperations setMultiple(Map<K, V> keysAndValues) {
    for (Map.Entry<K, V> entry : keysAndValues.entrySet()) {
      set(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public <K> KeyValueStoreOperations setMultipleAsBytes(Map<K, byte[]> keysAndValues) {
    for (Map.Entry<K, byte[]> entry : keysAndValues.entrySet()) {
      setAsBytes(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public <K, V> KeyValueStoreOperations setMultipleIfKeysNonExistent(Map<K, V> keysAndValues) {
    for (Map.Entry<K, V> entry : keysAndValues.entrySet()) {
      setIfKeyNonExistent(entry.getKey(), entry.getValue());
    }
    return this;
  }

  public <K> KeyValueStoreOperations setMultipleAsBytesIfKeysNonExistent(Map<K, byte[]> keysAndValues) {
    for (Map.Entry<K, byte[]> entry : keysAndValues.entrySet()) {
      setIfKeyNonExistentAsBytes(entry.getKey(), entry.getValue());
    }
    return this;
  }

  /*----------------- Key Operations -----------------*/

  public <K> boolean containsKey(K key) {
    BucketKeyPair bucketKeyPair = resolveBucketKeyPair(key, null);
    return riak.containsKey(bucketKeyPair.getBucket(), bucketKeyPair.getKey());
  }

  public <K> boolean deleteKeys(K... keys) {
    return riak.deleteKeys(keys);
  }

  /*----------------- Map/Reduce Operations -----------------*/

  public RiakMapReduceJob createMapReduceJob() {
    return new RiakMapReduceJob(riak);
  }

  public Object execute(MapReduceJob job) {
    return execute(job, List.class);
  }

  public <T> T execute(MapReduceJob job, Class<T> targetType) {
    return riak.execute(job, targetType);
  }

  public <T> Future<List<T>> submit(MapReduceJob job) {
    // Run this job asynchronously.
    return riak.submit(job);
  }

  /*----------------- Link Operations -----------------*/

  /**
   * Use Riak's native Link mechanism to link two entries together.
   *
   * @param destination Key to the child object
   * @param source      Key to the parent object
   * @param tag         The tag for this relationship
   * @return This template interface
   */
  public <K1, K2> RiakKeyValueTemplate link(K1 destination, K2 source, String tag) {
    BucketKeyPair bkpFrom = resolveBucketKeyPair(source, null);
    BucketKeyPair bkpTo = resolveBucketKeyPair(destination, null);
    riak.link(bkpTo.getBucket(), bkpTo.getKey(), bkpFrom.getBucket(), bkpFrom.getKey(), tag);
    return this;
  }

  /**
   * Use Riak's link walking mechanism to retrieve a multipart message that will be decoded like
   * they were individual objects (e.g. using the built-in HttpMessageConverters of
   * RestTemplate).
   *
   * @param source
   * @param tag
   * @return
   */
  @SuppressWarnings({"unchecked"})
  public <T, K> T linkWalk(K source, String tag) {
    BucketKeyPair bkpSource = resolveBucketKeyPair(source, null);
    return (T) riak.linkWalk(bkpSource.getBucket(), bkpSource.getKey(), tag);
  }

  /*----------------- Bucket Operations -----------------*/

  public <B> Map<String, Object> getBucketSchema(B bucket) {
    return riak.getBucketSchema(bucket, false);
  }

  public <B> Map<String, Object> getBucketSchema(B bucket, boolean listKeys) {
    return riak.getBucketSchema(bucket, listKeys);
  }

  public <B> KeyValueStoreOperations updateBucketSchema(B bucket, Map<String, Object> props) {
    riak.updateBucketSchema(bucket, props);
    return this;
  }

}
