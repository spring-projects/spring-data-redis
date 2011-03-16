/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.connection.rjc;

import java.io.StringReader;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.idevlab.rjc.ElementScore;
import org.idevlab.rjc.RedisException;
import org.idevlab.rjc.SortingParams;
import org.idevlab.rjc.ZParams;
import org.idevlab.rjc.Client.LIST_POSITION;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.keyvalue.redis.UncategorizedRedisException;
import org.springframework.data.keyvalue.redis.connection.DataType;
import org.springframework.data.keyvalue.redis.connection.DefaultTuple;
import org.springframework.data.keyvalue.redis.connection.SortParameters;
import org.springframework.data.keyvalue.redis.connection.RedisListCommands.Position;
import org.springframework.data.keyvalue.redis.connection.RedisZSetCommands.Aggregate;
import org.springframework.data.keyvalue.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.keyvalue.redis.connection.SortParameters.Order;
import org.springframework.data.keyvalue.redis.connection.SortParameters.Range;
import org.springframework.data.keyvalue.redis.connection.util.DecodeUtils;


/**
 * Helper class featuring methods for RJC connection handling, providing support for exception translation.
 * 
 * @author Costin Leau
 */
public abstract class RjcUtils {

	private static final String ONE = "1";
	private static final String ZERO = "0";


	public static DataAccessException convertRjcAccessException(RuntimeException ex) {
		if (ex instanceof RedisException) {
			return convertRjcAccessException((RedisException) ex);
		}

		return new UncategorizedRedisException("Unknown exception", ex);
	}

	public static DataAccessException convertRjcAccessException(RedisException ex) {
		return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
	}

	static DataType convertDataType(String type) {
		if ("string".equals(type)) {
			return DataType.STRING;
		}
		else if ("list".equals(type)) {
			return DataType.LIST;
		}
		else if ("set".equals(type)) {
			return DataType.SET;
		}
		else if ("zset".equals(type)) {
			return DataType.ZSET;
		}
		else if ("hash".equals(type)) {
			return DataType.HASH;
		}
		else if ("none".equals(type)) {
			return DataType.NONE;
		}

		return null;
	}

	static String decode(byte[] bytes) {
		return DecodeUtils.decode(bytes);
	}

	static byte[] encode(String string) {
		return DecodeUtils.encode(string);
	}

	static String[] decodeMultiple(byte[]... bytes) {
		return DecodeUtils.decodeMultiple(bytes);
	}

	static String[] flatten(Map<byte[], byte[]> tuple) {
		String[] result = new String[tuple.size() * 2];
		int index = 0;
		for (Map.Entry<byte[], byte[]> entry : tuple.entrySet()) {
			result[index++] = decode(entry.getKey());
			result[index++] = decode(entry.getValue());
		}
		return result;

	}

	static Set<byte[]> convertToSet(Collection<String> keys) {
		if (keys == null) {
			return null;
		}

		return DecodeUtils.convertToSet(keys);
	}

	static List<byte[]> convertToList(Collection<String> keys) {
		if (keys == null) {
			return null;
		}
		return DecodeUtils.convertToList(keys);
	}

	static SortingParams convertSortParams(SortParameters params) {
		SortingParams rjcSort = null;

		if (params != null) {
			rjcSort = new SortingParams();

			byte[] byPattern = params.getByPattern();
			if (byPattern != null) {
				rjcSort.by(DecodeUtils.decode(byPattern));
			}
			byte[][] getPattern = params.getGetPattern();

			if (getPattern != null && getPattern.length > 0) {
				for (byte[] bs : getPattern) {
					rjcSort.get(DecodeUtils.decode(bs));
				}
			}
			Range limit = params.getLimit();
			if (limit != null) {
				rjcSort.limit((int) limit.getStart(), (int) limit.getCount());
			}
			Order order = params.getOrder();
			if (order != null && order.equals(Order.DESC)) {
				rjcSort.desc();
			}
			Boolean isAlpha = params.isAlphabetic();
			if (isAlpha != null && isAlpha) {
				rjcSort.alpha();
			}
		}
		return rjcSort;
	}

	static Properties info(String string) {
		Properties info = new Properties();
		StringReader stringReader = new StringReader(string);
		try {
			info.load(stringReader);
		} catch (Exception ex) {
			throw new UncategorizedRedisException("Cannot read Redis info", ex);
		} finally {
			stringReader.close();
		}
		return info;
	}

	static String asBit(boolean value) {
		return (value ? ONE : ZERO);
	}

	static LIST_POSITION convertPosition(Position where) {
		switch (where) {
		case BEFORE:
			return LIST_POSITION.BEFORE;

		case AFTER:
			return LIST_POSITION.AFTER;
		}
		return null;
	}

	static ZParams toZParams(Aggregate aggregate, int[] weights) {
		return new ZParams().weights(weights).aggregate(ZParams.Aggregate.valueOf(aggregate.name()));
	}

	static Set<Tuple> convertElementScore(List<ElementScore> tuples) {
		Set<Tuple> value = new LinkedHashSet<Tuple>(tuples.size());
		for (ElementScore tuple : tuples) {
			value.add(new DefaultTuple(encode(tuple.getElement()), Double.valueOf(tuple.getScore())));
		}

		return value;
	}

	static Map<byte[], byte[]> encodeMap(Map<String, String> map) {
		Map<byte[], byte[]> result = new LinkedHashMap<byte[], byte[]>(map.size());
		for (Map.Entry<String, String> entry : map.entrySet()) {
			result.put(encode(entry.getKey()), encode(entry.getValue()));
		}
		return result;
	}

	static Map<String, String> decodeMap(Map<byte[], byte[]> map) {
		Map<String, String> result = new LinkedHashMap<String, String>(map.size());
		for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
			result.put(decode(entry.getKey()), decode(entry.getValue()));
		}
		return result;
	}
}