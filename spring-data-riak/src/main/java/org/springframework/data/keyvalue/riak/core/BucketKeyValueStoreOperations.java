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

import java.util.Map;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface BucketKeyValueStoreOperations {

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#set(Object,
   * Object)} that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @param value
   * @return
   */
  <B, K, V> BucketKeyValueStoreOperations set(B bucket, K key, V value);

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#set(Object,
   * Object, QosParameters)} that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @param value
   * @param qosParams
   * @return
   */
  <B, K, V> BucketKeyValueStoreOperations set(B bucket, K key, V value, QosParameters qosParams);

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#setAsBytes(Object,
   * byte[])} that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @param value
   * @return
   */
  <B, K> BucketKeyValueStoreOperations setAsBytes(B bucket, K key, byte[] value);

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#setWithMetaData(Object,
   * Object, java.util.Map, QosParameters)} that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @param value
   * @param metaData
   * @param qosParams
   * @return
   */
  <B, K, V> BucketKeyValueStoreOperations setWithMetaData(B bucket, K key, V value, Map<String, String> metaData, QosParameters qosParams);

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#get(Object)}
   * that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @return
   */
  <B, K, V> V get(B bucket, K key);

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#getAsBytes(Object)}
   * that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @return
   */
  <B, K> byte[] getAsBytes(B bucket, K key);

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#getAsType(Object,
   * Class)} that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @param requiredType
   * @return
   */
  <B, K, T> T getAsType(B bucket, K key, Class<T> requiredType);

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#getAndSet(Object,
   * Object)} that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @param value
   * @return
   */
  <B, K, V> V getAndSet(B bucket, K key, V value);

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#getAndSetAsBytes(Object,
   * byte[])} that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @param value
   * @return
   */
  <B, K> byte[] getAndSetAsBytes(B bucket, K key, byte[] value);

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#getAndSetAsType(Object,
   * Object, Class)} that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @param value
   * @param requiredType
   * @return
   */
  <B, K, V, T> T getAndSetAsType(B bucket, K key, V value, Class<T> requiredType);

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#setIfKeyNonExistent(Object,
   * Object)} that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @param value
   * @return
   */
  <B, K, V> BucketKeyValueStoreOperations setIfKeyNonExistent(B bucket, K key, V value);

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#setIfKeyNonExistentAsBytes(Object,
   * byte[])} that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @param value
   * @return
   */
  <B, K> BucketKeyValueStoreOperations setIfKeyNonExistentAsBytes(B bucket, K key, byte[] value);

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#containsKey(Object)}
   * that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @return
   */
  <B, K> boolean containsKey(B bucket, K key);

  /**
   * Delete a specific entry from this data store.
   *
   * @param bucket
   * @param key
   * @return
   */
  <B, K> boolean delete(B bucket, K key);

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#setAsBytes(Object,
   * byte[], QosParameters)} that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @param value
   * @param qosParams
   * @return
   */
  <B, K> BucketKeyValueStoreOperations setAsBytes(B bucket, K key, byte[] value, QosParameters qosParams);

  /**
   * Variant of {@link org.springframework.data.keyvalue.riak.core.KeyValueStoreOperations#setWithMetaData(Object,
   * Object, java.util.Map)} that takes a discreet bucket and key pair.
   *
   * @param bucket
   * @param key
   * @param value
   * @param metaData
   * @return
   */
  <B, K, V> BucketKeyValueStoreOperations setWithMetaData(B bucket, K key, V value, Map<String, String> metaData);
}
