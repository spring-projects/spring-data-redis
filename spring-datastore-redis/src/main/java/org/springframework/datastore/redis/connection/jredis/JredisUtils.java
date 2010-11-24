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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jredis.RedisException;
import org.jredis.RedisType;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.datastore.redis.connection.DataType;

/**
 * Helper class featuring methods for JRedis connection handling, providing support for exception translation. 
 * 
 * @author Costin Leau
 */
public abstract class JredisUtils {

	public static DataAccessException convertJredisAccessException(RedisException ex) {
		return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
	}

	static String convert(Charset charset, byte[] bytes) {
		return new String(bytes, charset);
	}

	static String[] convertMultiple(Charset charset, byte[]... bytes) {
		String[] result = new String[bytes.length];
		for (int i = 0; i < bytes.length; i++) {
			result[i] = new String(bytes[i], charset);
		}
		return result;
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

	static Map<byte[], byte[]> convertMap(Charset charset, Map<String, byte[]> map) {
		Map<byte[], byte[]> result = new LinkedHashMap<byte[], byte[]>(map.size());
		for (Map.Entry<String, byte[]> entry : map.entrySet()) {
			result.put(entry.getKey().getBytes(charset), entry.getValue());
		}
		return result;
	}

	static Collection<byte[]> convert(Charset charset, List<String> keys) {
		Collection<byte[]> list = new ArrayList<byte[]>(keys.size());

		for (String string : keys) {
			list.add(string.getBytes(charset));
		}
		return list;
	}

	static byte[] convert(Charset charset, String string) {
		return string.getBytes(charset);
	}

	static Map<String, byte[]> convert(Charset charset, Map<byte[], byte[]> tuple) {
		Map<String, byte[]> result = new LinkedHashMap<String, byte[]>(tuple.size());
		for (Map.Entry<byte[], byte[]> entry : tuple.entrySet()) {
			result.put(new String(entry.getKey(), charset), entry.getValue());
		}
		return result;
	}
}