/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import java.io.Serializable;
import java.util.Set;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.convert.IndexedData;
import org.springframework.data.redis.core.convert.RedisConverter;
import org.springframework.data.redis.core.convert.SimpleIndexedPropertyValue;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 */
class IndexDataWriter {

	private final RedisConnection connection;
	private final RedisConverter converter;
	private final Serializable keyspace;

	public IndexDataWriter(Serializable keyspace, RedisConnection connection, RedisConverter converter) {

		Assert.notNull(keyspace, "Keyspace cannot be null!");
		Assert.notNull(connection, "RedisConnection cannot be null!");
		Assert.notNull(converter, "RedisConverter cannot be null!");

		this.connection = connection;
		this.converter = converter;
		this.keyspace = keyspace;
	}

	public void updateIndexes(Object key, Iterable<IndexedData> indexValues) {

		Assert.notNull(key, "Key must not be null!");
		if (indexValues == null) {
			return;
		}

		byte[] binKey = toBytes(key);

		removeKeyFromExistingIndexes(binKey, indexValues);
		addKeyToIndexes(binKey, indexValues);
	}

	public void removeKeyFromIndexes(Object key) {

		Assert.notNull(key, "Key must not be null!");
		byte[] binKey = toBytes(key);

		Set<byte[]> potentialIndex = connection.keys(toBytes(keyspace + ".*"));
		for (byte[] indexKey : potentialIndex) {
			connection.sRem(indexKey, binKey);
		}

	}

	public void removeAllIndexes() {

		Set<byte[]> potentialIndex = connection.keys(toBytes(keyspace + ".*"));

		if (!potentialIndex.isEmpty()) {
			connection.del(potentialIndex.toArray(new byte[potentialIndex.size()][]));
		}
	}

	private void removeKeyFromExistingIndexes(byte[] key, Iterable<IndexedData> indexValues) {

		for (IndexedData indexData : indexValues) {
			removeKeyFromExistingIndexes(key, indexData);
		}
	}

	private void removeKeyFromExistingIndexes(byte[] key, IndexedData indexedData) {

		Set<byte[]> existingKeys = connection.keys(toBytes(keyspace + "." + indexedData.getPath() + ":*"));
		for (byte[] existingKey : existingKeys) {
			connection.sRem(existingKey, key);
		}
	}

	private void addKeyToIndexes(byte[] key, Iterable<IndexedData> indexValues) {

		for (IndexedData indexData : indexValues) {
			addKeyToIndex(key, indexData);
		}
	}

	private void addKeyToIndex(byte[] key, IndexedData indexedData) {

		if (indexedData instanceof SimpleIndexedPropertyValue) {

			Object value = ((SimpleIndexedPropertyValue) indexedData).getValue();

			if (value == null) {
				return;
			}

			byte[] indexKey = toBytes(keyspace + "." + indexedData.getPath() + ":");
			indexKey = ByteUtils.concat(indexKey, toBytes(value));
			connection.sAdd(indexKey, key);
		} else {
			throw new IllegalArgumentException("Cannot index data");
		}
	}

	private byte[] toBytes(Object source) {

		if (source == null) {
			return new byte[] {};
		}

		if (source instanceof byte[]) {
			return (byte[]) source;
		}

		return converter.getConversionService().convert(source, byte[].class);
	}

}
