/*
 * Copyright 2011-2013 the original author or authors.
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

package org.springframework.data.redis.connection.srp;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.util.Assert;

import redis.client.RedisException;
import redis.reply.BulkReply;
import redis.reply.MultiBulkReply;
import redis.reply.Reply;

import com.google.common.base.Charsets;

/**
 * Helper class featuring methods for SRedis connection handling, providing support for exception translation.
 * 
 * @author Costin Leau
 */
abstract class SrpUtils {

	private static final byte[] ONE = new byte[] { '1' };
	private static final byte[] ZERO = new byte[] { '0' };
	private static final byte[] BEFORE = "BEFORE".getBytes(Charsets.UTF_8);
	private static final byte[] AFTER = "AFTER".getBytes(Charsets.UTF_8);
	static final byte[] WITHSCORES = "WITHSCORES".getBytes(Charsets.UTF_8);
	private static final byte[] SPACE = "".getBytes(Charsets.UTF_8);
	private static final byte[] BY = "BY ".getBytes(Charsets.UTF_8);
	private static final byte[] GET = "GET ".getBytes(Charsets.UTF_8);
	private static final byte[] ALPHA = "ALPHA ".getBytes(Charsets.UTF_8);
	private static final byte[] STORE = "STORE ".getBytes(Charsets.UTF_8);


	static DataAccessException convertSRedisAccessException(RuntimeException ex) {
		if (ex instanceof RedisException) {
			return new RedisSystemException("redis exception", ex);
		}
		return null;
	}

	static Properties info(BulkReply reply) {
		Properties info = new Properties();
		// use the same charset as the library
		StringReader stringReader = new StringReader(new String(reply.data(), Charsets.UTF_8));
		try {
			info.load(stringReader);
		} catch (Exception ex) {
			throw new RedisSystemException("Cannot read Redis info", ex);
		} finally {
			stringReader.close();
		}
		return info;
	}

	static List<byte[]> toBytesList(Reply[] replies) {
		List<byte[]> list = new ArrayList<byte[]>(replies.length);
		for (Reply reply : replies) {
			Object data = reply.data();
			if (data == null) {
				list.add(null);
			}
			else if (data instanceof byte[])
				list.add((byte[]) data);
			else
				throw new IllegalArgumentException("array contains more then just nulls and bytes -> " + data);
		}

		return list;
	}

	static <T> List<T> toList(T[] byteArrays) {
		return Arrays.asList(byteArrays);
	}

	static Set<byte[]> toSet(Reply[] byteArrays) {
		return new LinkedHashSet<byte[]>(toBytesList(byteArrays));
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

	static byte[] asBit(boolean value) {
		return (value ? ONE : ZERO);
	}

	static byte[] convertPosition(Position where) {
		Assert.notNull("list positions are mandatory");
		return (Position.AFTER.equals(where) ? AFTER : BEFORE);
	}

	static Double toDouble(byte[] bytes) {
		return (bytes == null || bytes.length == 0 ? null : Double.valueOf(new String(bytes, Charsets.UTF_8)));
	}

	static Long toLong(Object[] bytes) {
		return (bytes == null || bytes.length == 0 ? null : Long.valueOf(new String((byte[]) bytes[0], Charsets.UTF_8)));
	}

	static Set<Tuple> convertTuple(MultiBulkReply zrange) {
		Reply[] byteArrays = zrange.data();
		Set<Tuple> tuples = new LinkedHashSet<Tuple>(byteArrays.length / 2 + 1);

		for (int i = 0; i < byteArrays.length; i++) {
			byte[] value = (byte[]) byteArrays[i].data();
			i++;
			Double score = toDouble((byte[]) byteArrays[i].data());
			tuples.add(new DefaultTuple(value, score));
		}

		return tuples;
	}

	static Map<byte[], byte[]> toMap(Reply[] byteArrays) {
		Map<byte[], byte[]> map = new LinkedHashMap<byte[], byte[]>(byteArrays.length / 2);
		for (int i = 0; i < byteArrays.length; i++) {
			map.put((byte[]) byteArrays[i++].data(), (byte[]) byteArrays[i].data());
		}
		return map;
	}

	static byte[] limit(long offset, long count) {
		return ("LIMIT " + offset + " " + count).getBytes(Charsets.UTF_8);
	}

	static byte[] sort(SortParameters params) {
		return sort(params, null);
	}

	static byte[] sort(SortParameters params, byte[] sortKey) {
		List<byte[]> arrays = new ArrayList<byte[]>();

		if (params.getByPattern() != null) {
			arrays.add(BY);
			arrays.add(params.getByPattern());
			arrays.add(SPACE);
		}

		if (params.getLimit() != null) {
			arrays.add(limit(params.getLimit().getStart(), params.getLimit().getCount()));
			arrays.add(SPACE);
		}

		if (params.getGetPattern() != null) {
			byte[][] pattern = params.getGetPattern();
			for (byte[] bs : pattern) {
				arrays.add(GET);
				arrays.add(bs);
				arrays.add(SPACE);
			}
		}

		if (params.getOrder() != null) {
			arrays.add(params.getOrder().name().getBytes(Charsets.UTF_8));
			arrays.add(SPACE);
		}

		if (params.isAlphabetic()) {
			arrays.add(ALPHA);
		}

		if (sortKey != null) {
			arrays.add(STORE);
			arrays.add(sortKey);
		}

		// concatenate array
		int size = 0;

		for (byte[] bs : arrays) {
			size += bs.length;
		}
		byte[] result = new byte[size];

		int index = 0;
		for (byte[] bs : arrays) {
			System.arraycopy(bs, 0, result, index, bs.length);
			index += bs.length;
		}

		return result;
	}
}