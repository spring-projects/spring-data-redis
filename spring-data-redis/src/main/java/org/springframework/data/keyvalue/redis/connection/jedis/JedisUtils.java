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

package org.springframework.data.keyvalue.redis.connection.jedis;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.keyvalue.redis.RedisConnectionFailureException;
import org.springframework.data.keyvalue.redis.UncategorizedRedisException;
import org.springframework.data.keyvalue.redis.connection.DefaultTuple;
import org.springframework.data.keyvalue.redis.connection.RedisZSetCommands.Tuple;

import redis.clients.jedis.JedisException;

/**
 * Helper class featuring methods for Jedis connection handling, providing support for exception translation. 
 * 
 * @author Costin Leau
 */
public abstract class JedisUtils {

	private static final String OK_CODE = "OK";
	private static final String OK_MULTI_CODE = "+OK";

	public static DataAccessException convertJedisAccessException(JedisException ex) {
		return new InvalidDataAccessApiUsageException(ex.getMessage(), ex);
	}

	public static DataAccessException convertJedisAccessException(RuntimeException ex) {
		if (ex instanceof JedisException) {
			return convertJedisAccessException((JedisException) ex);
		}

		return new UncategorizedRedisException("Unknown exception", ex);
	}

	static DataAccessException convertJedisAccessException(IOException ex) {
		if (ex instanceof UnknownHostException) {
			return new RedisConnectionFailureException("Unknown host " + ex.getMessage(), ex);
		}
		return new RedisConnectionFailureException("Could not connect to Redis server", ex);
	}

	static DataAccessException convertJedisAccessException(TimeoutException ex) {
		throw new RedisConnectionFailureException("Jedis pool timed out. Could not get Redis Connection", ex);
	}

	static boolean isStatusOk(String status) {
		return status != null && (OK_CODE.equals(status) || OK_MULTI_CODE.equals(status));
	}

	static Boolean convertCodeReply(Number code) {
		return (code != null ? code.intValue() == 1 : null);
	}

	static Set<Tuple> convertJedisTuple(Set<redis.clients.jedis.Tuple> tuples) {
		Set<Tuple> value = new LinkedHashSet<Tuple>(tuples.size());
		for (redis.clients.jedis.Tuple tuple : tuples) {
			value.add(new DefaultTuple(tuple.getBinaryElement(), tuple.getScore()));
		}

		return value;
	}

	static byte[][] convert(Map<byte[], byte[]> hgetAll) {
		byte[][] result = new byte[hgetAll.size() * 2][];

		int index = 0;
		for (Map.Entry<byte[], byte[]> entry : hgetAll.entrySet()) {
			result[index++] = entry.getKey();
			result[index++] = entry.getValue();
		}
		return result;
	}

	static Map<String, String> convert(String[] fields, String[] values) {
		Map<String, String> result = new LinkedHashMap<String, String>(fields.length);

		for (int i = 0; i < values.length; i++) {
			result.put(fields[i], values[i]);
		}
		return result;
	}

	static String[] arrange(String[] keys, String[] values) {
		String[] result = new String[keys.length * 2];

		for (int i = 0; i < keys.length; i++) {
			int index = i << 1;
			result[index] = keys[i];
			result[index + 1] = values[i];

		}
		return result;
	}
}