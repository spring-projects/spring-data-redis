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
package org.springframework.data.redis.core.convert;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.data.redis.connection.util.ByteArrayWrapper;
import org.springframework.data.redis.util.ByteUtils;

/**
 * Data object holding flat hash values, to be stored in Redis hash, representing the domain object. Index information
 * points to additional structures holding the objects is for searching.
 * 
 * @author Christoph Strobl
 */
public class RedisData {

	private static final Charset CHARSET = Charset.forName("UTF-8");

	private byte[] keyspace;
	private byte[] id;
	public static final byte[] ID_SEPERATOR = ":".getBytes(CHARSET);
	public static final byte[] PATH_SEPERATOR = ".".getBytes(CHARSET);

	private Map<ByteArrayWrapper, byte[]> data;
	private Set<ByteArrayWrapper> simpleIndexKeys;
	private Set<ByteArrayWrapper> indexPaths;

	public RedisData() {

		this.data = new LinkedHashMap<ByteArrayWrapper, byte[]>();
		this.simpleIndexKeys = new HashSet<ByteArrayWrapper>();
		this.indexPaths = new HashSet<ByteArrayWrapper>();
	}

	public RedisData(Map<byte[], byte[]> raw) {

		this();

		for (Entry<byte[], byte[]> entry : raw.entrySet()) {
			addDataEntry(entry.getKey(), entry.getValue());
		}
	}

	/**
	 * Create new {@link RedisData} from key/value pairs of given source {@link Map} by applying {@literal UTF-8}
	 * {@link String} to {@code byte[]} conversion.
	 * 
	 * @param source can be {@literal null}.
	 * @return
	 */
	public static RedisData newRedisDataFromStringMap(Map<String, String> source) {

		RedisData rdo = new RedisData();
		if (source == null) {
			return rdo;
		}

		for (Entry<String, String> entry : source.entrySet()) {
			rdo.addDataEntry(entry.getKey().getBytes(CHARSET), entry.getValue().getBytes(CHARSET));
		}
		return rdo;
	}

	/**
	 * Adds given key and value overriding existing values if the key matches an already existing one. {@literal null} key
	 * or values are be silently ignored.
	 * 
	 * @param key
	 * @param value
	 */
	public void addDataEntry(byte[] key, byte[] value) {

		if (hasValue(key) && hasValue(value)) {
			data.put(new ByteArrayWrapper(key), value);
		}
	}

	/**
	 * Get the key representing the {@link RedisData}.
	 * 
	 * @return never {@literal null}.
	 */
	public byte[] getKey() {
		return ByteUtils.concatAll(keyspace != null ? keyspace : new byte[] {}, ID_SEPERATOR, id != null ? id
				: new byte[] {});
	}

	public void setId(byte[] id) {
		this.id = id;
	}

	public byte[] getId() {
		return this.id;
	}

	public byte[] getDataForKey(byte[] key) {
		return data.get(new ByteArrayWrapper(key));
	}

	/**
	 * Get raw data wrapped in {@link RedisData}.
	 * 
	 * @return
	 */
	public Map<byte[], byte[]> getData() {

		Map<byte[], byte[]> map = new LinkedHashMap<byte[], byte[]>();
		for (Entry<ByteArrayWrapper, byte[]> entry : data.entrySet()) {
			map.put(entry.getKey().getArray(), entry.getValue());
		}
		return map;
	}

	/**
	 * Add keys the id of the current object is added to on save.
	 * 
	 * @param bytes
	 */
	public void addSimpleIndexKey(byte[] bytes) {
		this.simpleIndexKeys.add(new ByteArrayWrapper(bytes));
	}

	public void addIndexPath(byte[] path) {
		this.indexPaths.add(new ByteArrayWrapper(path));
	}

	public Set<byte[]> getSimpleIndexKeys() {

		Set<byte[]> target = new HashSet<byte[]>();
		for (ByteArrayWrapper wrapper : this.simpleIndexKeys) {
			target.add(ByteUtils.concatAll(keyspace, PATH_SEPERATOR, wrapper.getArray()));
		}
		return target;
	}

	public Set<byte[]> getIndexPaths() {

		Set<byte[]> target = new HashSet<byte[]>();
		for (ByteArrayWrapper wrapper : this.indexPaths) {
			target.add(ByteUtils.concatAll(keyspace, PATH_SEPERATOR, wrapper.getArray()));
		}
		return target;
	}

	/**
	 * @return
	 */
	public byte[] getKeyspace() {
		return keyspace;
	}

	/**
	 * @param keyspace
	 */
	public void setKeyspace(byte[] keyspace) {
		this.keyspace = keyspace;
	}

	/**
	 * @return
	 */
	public Map<String, String> getDataAsUtf8String() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		for (Entry<ByteArrayWrapper, byte[]> entry : data.entrySet()) {
			map.put(new String(entry.getKey().getArray(), CHARSET), new String(entry.getValue(), CHARSET));
		}
		return map;
	}

	/**
	 * @return
	 */
	public String getKeyAsUtf8String() {

		if (getKey() == null) {
			return null;
		}
		return new String(getKey(), CHARSET);
	}

	private boolean hasValue(byte[] source) {
		return source != null && source.length != 0;
	}

	@Override
	public String toString() {
		return "RedisDataObject [key=" + getKeyAsUtf8String() + ", hash=" + getDataAsUtf8String() + "]";
	}

}
