/*
 * Copyright (c) 2010 by J. Brisbin <jon@jbrisbin.com>
 * Portions (c) 2010 by NPC International, Inc. or the
 * original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.keyvalue.riak.core;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * An asynchronous version of {@link BucketKeyValueStoreOperations}.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface AsyncBucketKeyValueStoreOperations {

  /**
   * Put an object in Riak at a specific bucket and key and invoke callback with the value
   * pulled back out of Riak after the update, which contains full headers and metadata.
   *
   * @param bucket
   * @param key
   * @param value
   * @param callback Called with the update value pulled from Riak
   */
  <B, K, V, R> Future<?> set(B bucket, K key, V value, AsyncKeyValueStoreOperation<V, R> callback);

  /**
   * @param bucket
   * @param key
   * @param value
   * @param qosParams
   * @return
   */
  <B, K, V, R> Future<?> set(B bucket, K key, V value, QosParameters qosParams, AsyncKeyValueStoreOperation<V, R> callback);

  /**
   * @param bucket
   * @param key
   * @param value
   * @return
   */
  <B, K, R> Future<?> setAsBytes(B bucket, K key, byte[] value, AsyncKeyValueStoreOperation<byte[], R> callback);

  /**
   * @param bucket
   * @param key
   * @param value
   * @param qosParams
   * @return
   */
  <B, K, R> Future<?> setAsBytes(B bucket, K key, byte[] value, QosParameters qosParams, AsyncKeyValueStoreOperation<byte[], R> callback);

  /**
   * @param bucket
   * @param key
   * @param value
   * @param metaData
   * @return
   */
  <B, K, V, R> Future<?> setWithMetaData(B bucket, K key, V value, Map<String, String> metaData, AsyncKeyValueStoreOperation<V, R> callback);

  /**
   * @param bucket
   * @param key
   * @param value
   * @param metaData
   * @param qosParams
   * @return
   */
  <B, K, V, R> Future<?> setWithMetaData(B bucket, K key, V value, Map<String, String> metaData, QosParameters qosParams, AsyncKeyValueStoreOperation<V, R> callback);

  /**
   * @param bucket
   * @param key
   * @return
   */
  <B, K, V, R> Future<?> get(B bucket, K key, AsyncKeyValueStoreOperation<V, R> callback);

  /**
   * @param bucket
   * @param key
   * @return
   */
  <B, K, R> Future<?> getAsBytes(B bucket, K key, AsyncKeyValueStoreOperation<byte[], R> callback);

  /**
   * @param bucket
   * @param key
   * @param requiredType
   * @return
   */
  <B, K, T, R> Future<?> getAsType(B bucket, K key, Class<T> requiredType, AsyncKeyValueStoreOperation<T, R> callback);

  /**
   * @param bucket
   * @param key
   * @param value
   * @return
   */
  <B, K, V, R> Future<?> getAndSet(B bucket, K key, V value, AsyncKeyValueStoreOperation<V, R> callback);

  /**
   * @param bucket
   * @param key
   * @param value
   * @return
   */
  <B, K, R> Future<?> getAndSetAsBytes(B bucket, K key, byte[] value, AsyncKeyValueStoreOperation<byte[], R> callback);

  /**
   * @param bucket
   * @param key
   * @param value
   * @param requiredType
   * @return
   */
  <B, K, V, T, R> Future<?> getAndSetAsType(B bucket, K key, V value, Class<T> requiredType, AsyncKeyValueStoreOperation<T, R> callback);

  /**
   * @param bucket
   * @param key
   * @param value
   * @return
   */
  <B, K, V, R> Future<?> setIfKeyNonExistent(B bucket, K key, V value, AsyncKeyValueStoreOperation<V, R> callback);

  /**
   * @param bucket
   * @param key
   * @param value
   * @return
   */
  <B, K, R> Future<?> setIfKeyNonExistentAsBytes(B bucket, K key, byte[] value, AsyncKeyValueStoreOperation<byte[], R> callback);

  /**
   * @param bucket
   * @param key
   * @return
   */
  <B, K, R> Future<?> containsKey(B bucket, K key, AsyncKeyValueStoreOperation<Boolean, R> callback);

  /**
   * Delete a specific entry from this data store.
   *
   * @param bucket
   * @param key
   * @return
   */
  <B, K, R> Future<?> delete(B bucket, K key, AsyncKeyValueStoreOperation<Boolean, R> callback);

}
