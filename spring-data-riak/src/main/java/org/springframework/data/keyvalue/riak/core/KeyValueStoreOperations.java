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

import java.util.List;
import java.util.Map;

/**
 * Generic abstraction for Key/Value stores. Contains most operations that generic K/V stores
 * might expose.
 */
public interface KeyValueStoreOperations {

  // Set operations

  /**
   * Set a value at a specified key.
   *
   * @param key
   * @param value
   * @return This template interface
   */
  <K, V> KeyValueStoreOperations set(K key, V value);

  /**
   * Variation on set() that allows the user to specify {@link org.springframework.data.keyvalue.riak.core.QosParameters}.
   *
   * @param key
   * @param value
   * @param qosParams
   * @param <K>
   * @param <V>
   * @return
   */
  <K, V> KeyValueStoreOperations set(K key, V value, QosParameters qosParams);

  /**
   * Set a value as a byte array at a specified key.
   *
   * @param key
   * @param value
   * @return This template interface
   */
  <K> KeyValueStoreOperations setAsBytes(K key, byte[] value);

  /**
   * Variation on setWithMetaData() that allows the user to pass {@link
   * org.springframework.data.keyvalue.riak.core.QosParameters}.
   *
   * @param key
   * @param value
   * @param metaData
   * @param qosParams
   * @param <K>
   * @param <V>
   * @return
   */
  <K, V> KeyValueStoreOperations setWithMetaData(K key, V value, Map<String, String> metaData, QosParameters qosParams);

  // Get operations

  /**
   * Get a value at the specified key, trying to infer the type from either the bucket in which
   * the value was stored, or (by default) as a <code>java.util.Map</code>.
   *
   * @param key
   * @return The converted value, or <code>null</code> if not found.
   */
  <K, V> V get(K key);

  /**
   * Get the value at the specified key as a byte array.
   *
   * @param key
   * @return The byte array of data, or <code>null</code> if not found.
   */
  <K> byte[] getAsBytes(K key);

  /**
   * Get the value at the specified key and convert it into an instance of the specified type.
   *
   * @param key
   * @param requiredType
   * @return The converted value, or <code>null</code> if not found.
   */
  <K, T> T getAsType(K key, Class<T> requiredType);

  // Get and Set operations

  /**
   * Get the old value at the specified key and replace it with the given value.
   *
   * @param key
   * @param value
   * @return The old value (before it was overwritten).
   */
  <K, V> V getAndSet(K key, V value);

  /**
   * Get the old value at the specified key as a byte array and replace it with the given
   * bytes.
   *
   * @param key
   * @param value
   * @return The old byte array (before it was overwritten).
   */
  <K> byte[] getAndSetAsBytes(K key, byte[] value);

  /**
   * Get the old value at the specified key and replace it with the given value, converting it
   * to an instance of the given type.
   *
   * @param key
   * @param value
   * @param requiredType The type to convert the value to.
   * @return The old value (before it was overwritten).
   */
  <K, V, T> T getAndSetAsType(K key, V value, Class<T> requiredType);

  // Multi-get operations

  /**
   * Get all the values at the specified keys.
   *
   * @param keys
   * @return A list of the values retrieved or an empty list if none were found.
   */
  <K, V> List<V> getValues(List<K> keys);

  /**
   * Variation on {@link KeyValueStoreOperations#getValues(java.util.List)} that uses varargs
   * instead of a <code>java.util.List</code>.
   *
   * @param keys
   * @return A list of the values retrieved or an empty list if none were found.
   */
  <K, V> List<V> getValues(K... keys);

  /**
   * Get all the values at the specified keys, converting the values into instances of the
   * specified type.
   *
   * @param keys
   * @param requiredType
   * @return A list of the values retrieved or an empty list if none were found.
   */
  <K, T> List<T> getValuesAsType(List<K> keys, Class<T> requiredType);

  /**
   * A variation on {@link KeyValueStoreOperations#getValuesAsType(java.util.List, Class)} that
   * takes uses varargs instead of a <code>java.util.List</code>.
   *
   * @param requiredType
   * @param keys
   * @return A list of the values retrieved or an empty list if none were found.
   */
  <T, K> List<T> getValuesAsType(Class<T> requiredType, K... keys);

  // Set if non-existent operations

  /**
   * Set the value at the given key only if that key doesn't already exist.
   *
   * @param key
   * @param value
   * @return This template interface
   */
  <K, V> KeyValueStoreOperations setIfKeyNonExistent(K key, V value);

  /**
   * Set the value at the given key as a byte array only if that key doesn't already exist.
   *
   * @param key
   * @param value
   * @return This template interface
   */
  <K> KeyValueStoreOperations setIfKeyNonExistentAsBytes(K key, byte[] value);

  // Multiple key-value set

  /**
   * Convenience method to set multiple values as Key/Value pairs.
   *
   * @param keysAndValues
   * @return This template interface
   */
  <K, V> KeyValueStoreOperations setMultiple(Map<K, V> keysAndValues);

  /**
   * Convenience method to set multiple values as Key/byte[] pairs.
   *
   * @param keysAndValues
   * @return This template interface
   */
  <K> KeyValueStoreOperations setMultipleAsBytes(Map<K, byte[]> keysAndValues);

  // Multiple key-value set if non-existent

  /**
   * Variation on setting multiple values only if the key doesn't already exist.
   *
   * @param keysAndValues
   * @return This template interface
   */
  <K, V> KeyValueStoreOperations setMultipleIfKeysNonExistent(Map<K, V> keysAndValues);

  /**
   * Variation on setting multiple values as byte arrays only if the key doesn't already exist.
   *
   * @param keysAndValues
   * @param <K>
   * @return
   */
  <K> KeyValueStoreOperations setMultipleAsBytesIfKeysNonExistent(Map<K, byte[]> keysAndValues);

  /**
   * Does the store contain this key?
   *
   * @param key
   * @return <code>true</code> if the key exists, <code>false</code> otherwise.
   */
  <K> boolean containsKey(K key);

  /**
   * Delete one or more keys from the store.
   *
   * @param keys
   * @return <code>true</code> if all keys were successfully deleted, <code>false</code>
   *         otherwise.
   */
  <K> boolean deleteKeys(K... keys);

  /**
   * Get the properties of the specified bucket.
   *
   * @param bucket
   * @return The bucket properties, without a list of keys in that bucket.
   */
  <B> Map<String, Object> getBucketSchema(B bucket);

  <B> KeyValueStoreOperations updateBucketSchema(B bucket, Map<String, Object> props);

  /**
   * Get the properties of the bucket and specify whether or not to list the keys in that
   * bucket.
   *
   * @param bucket
   * @param listKeys
   * @return The bucket properties, with or without a list of keys in that bucket.
   */
  <B> Map<String, Object> getBucketSchema(B bucket, boolean listKeys);

  <K> KeyValueStoreOperations setAsBytes(K key, byte[] value, QosParameters qosParams);

  <K, V> KeyValueStoreOperations setWithMetaData(K key, V value, Map<String, String> metaData);
}
