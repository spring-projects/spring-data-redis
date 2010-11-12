/*
 * Copyright 2010 the original author or authors.
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

package org.springframework.datastore.redis.connection.jredis;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jredis.RedisException;
import org.jredis.RedisType;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.datastore.redis.connection.DataType;
import org.springframework.datastore.redis.connection.DefaultEntry;
import org.springframework.datastore.redis.connection.RedisHashCommands.Entry;

/**
 * Helper class featuring methods for JRedis connection handling, providing support for exception translation. 
 * 
 * @author Costin Leau
 */
public abstract class JredisUtils {

	public static DataAccessException convertJredisAccessException(RedisException ex) {
		return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
	}

	static String convert(byte[] bytes) {
		return new String(bytes);
	}

	static String convert(String encoding, byte[] bytes) {
		try {
			return new String(bytes, encoding);
		} catch (UnsupportedEncodingException ex) {
			throw new RuntimeException(ex);
		}
	}

	static String[] convertMultiple(String encoding, byte[]... bytes) {
		String[] result = new String[bytes.length];
		try {
			for (int i = 0; i < bytes.length; i++) {
				result[i] = new String(bytes[i], encoding);
			}
		} catch (UnsupportedEncodingException ex) {
			throw new RuntimeException(ex);
		}
		return result;
	}

	static <T extends Collection<String>> T convertToStringCollection(List<byte[]> bytes, Class<T> collectionType) {

		Collection<String> col = (List.class.isAssignableFrom(collectionType) ? new ArrayList<String>(bytes.size())
				: new LinkedHashSet<String>(bytes.size()));

		for (byte[] bs : bytes) {
			col.add(new String(bs));
		}
		return (T) col;
	}

	static DataType convertDataType(RedisType type) {
		switch (type) {
		case NONE:
			return DataType.NONE;
		case string:
			return DataType.STRING;
		case list:
			return DataType.LIST;
		case set:
			return DataType.SET;
			//case zset:
			// return DataType.ZSET;
		case hash:
			return DataType.HASH;
		}

		return null;
	}

	static Set<Entry> convert(Map<String, byte[]> map) {
		Set<Entry> entries = new LinkedHashSet<Entry>(map.size());
		for (Map.Entry<String, byte[]> entry : map.entrySet()) {
			entries.add(new DefaultEntry(entry.getKey(), new String(entry.getValue())));
		}
		return entries;
	}

	static Map<String, byte[]> convert(String[] keys, String[] values) {
		Map<String, byte[]> result = new LinkedHashMap<String, byte[]>(keys.length);

		for (int i = 0; i < values.length; i++) {
			result.put(keys[i], values[i].getBytes());
		}
		return result;
	}

	static Collection<byte[]> convert(String charset, List<String> keys) {
		Collection<byte[]> list = new ArrayList<byte[]>(keys.size());

		try {
			for (String string : keys) {
				list.add(string.getBytes(charset));
			}
		} catch (UnsupportedEncodingException ex) {
			throw new RuntimeException(ex);
		}

		return list;
	}

	static byte[] convert(String charset, String string) {
		try {
			return string.getBytes(charset);
		} catch (UnsupportedEncodingException ex) {
			throw new RuntimeException(ex);
		}
	}

	static Map<String, byte[]> convert(String encoding, Map<byte[], byte[]> tuple) {
		Map<String, byte[]> result = new LinkedHashMap<String, byte[]>(tuple.size());
		try {

			for (Map.Entry<byte[], byte[]> entry : tuple.entrySet()) {
				result.put(new String(entry.getKey(), encoding), entry.getValue());
			}
		} catch (UnsupportedEncodingException ex) {
			throw new RuntimeException(ex);
		}

		return result;
	}
}