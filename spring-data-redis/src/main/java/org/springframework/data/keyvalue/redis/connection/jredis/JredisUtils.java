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

package org.springframework.data.keyvalue.redis.connection.jredis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jredis.RedisException;
import org.jredis.RedisType;
import org.jredis.Sort;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.keyvalue.redis.connection.DataType;
import org.springframework.data.keyvalue.redis.connection.SortParameters;
import org.springframework.data.keyvalue.redis.connection.SortParameters.Order;
import org.springframework.data.keyvalue.redis.connection.SortParameters.Range;

/**
 * Helper class featuring methods for JRedis connection handling, providing support for exception translation. 
 * 
 * @author Costin Leau
 */
public abstract class JredisUtils {

	public static DataAccessException convertJredisAccessException(RedisException ex) {
		return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
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

	static String decode(byte[] bytes) {
		return Base64.encodeToString(bytes, false);
	}

	static String[] decodeMultiple(byte[]... bytes) {
		String[] result = new String[bytes.length];
		for (int i = 0; i < bytes.length; i++) {
			result[i] = decode(bytes[i]);
		}
		return result;
	}

	static byte[] encode(String string) {
		return Base64.decode(string);
	}

	static Map<byte[], byte[]> encodeMap(Map<String, byte[]> map) {
		Map<byte[], byte[]> result = new LinkedHashMap<byte[], byte[]>(map.size());
		for (Map.Entry<String, byte[]> entry : map.entrySet()) {
			result.put(encode(entry.getKey()), entry.getValue());
		}
		return result;
	}

	static Collection<byte[]> convertCollection(Collection<String> keys) {
		Collection<byte[]> list = new ArrayList<byte[]>(keys.size());

		for (String string : keys) {
			list.add(Base64.decode(string));
		}
		return list;
	}


	static Map<String, byte[]> decodeMap(Map<byte[], byte[]> tuple) {
		Map<String, byte[]> result = new LinkedHashMap<String, byte[]>(tuple.size());
		for (Map.Entry<byte[], byte[]> entry : tuple.entrySet()) {
			result.put(decode(entry.getKey()), entry.getValue());
		}
		return result;
	}


	static Sort applySortingParams(Sort jredisSort, SortParameters params, byte[] storeKey) {
		if (params != null) {
			byte[] byPattern = params.getByPattern();
			if (byPattern != null) {
				jredisSort.BY(decode(byPattern));
			}
			byte[] getPattern = params.getGetPattern();
			if (getPattern != null) {
				jredisSort.GET(decode(getPattern));
			}
			Range limit = params.getLimit();
			if (limit != null) {
				jredisSort.LIMIT(limit.getStart(), limit.getCount());
			}
			Order order = params.getOrder();
			if (order != null && order.equals(Order.DESC)) {
				jredisSort.DESC();
			}
			Boolean isAlpha = params.isAlphabetic();
			if (isAlpha != null && isAlpha) {
				jredisSort.ALPHA();
			}
		}

		if (storeKey != null) {
			jredisSort.STORE(decode(storeKey));
		}


		return jredisSort;
	}
}