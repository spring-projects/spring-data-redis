/*
 * Copyright 2011-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.redis.connection.srp;

import com.google.common.base.Charsets;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.util.Assert;
import redis.client.RedisException;
import redis.reply.BulkReply;
import redis.reply.IntegerReply;
import redis.reply.MultiBulkReply;
import redis.reply.Reply;
import redis.reply.StatusReply;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Helper class featuring methods for SRedis connection handling, providing support for exception translation.
 * Deprecated. Use {@link SrpConverters} instead.
 * 
 * @author Costin Leau
 * @author Jennifer Hickey
 * @deprecated since 1.7. Will be removed in subsequent version.
 */
@Deprecated
abstract class SrpUtils {

	private static final byte[] ONE = new byte[] { '1' };
	private static final byte[] ZERO = new byte[] { '0' };
	private static final byte[] BEFORE = "BEFORE".getBytes(Charsets.UTF_8);
	private static final byte[] AFTER = "AFTER".getBytes(Charsets.UTF_8);
	static final byte[] WITHSCORES = "WITHSCORES".getBytes(Charsets.UTF_8);
	private static final byte[] SPACE = " ".getBytes(Charsets.UTF_8);
	private static final byte[] BY = "BY".getBytes(Charsets.UTF_8);
	private static final byte[] GET = "GET".getBytes(Charsets.UTF_8);
	private static final byte[] ALPHA = "ALPHA".getBytes(Charsets.UTF_8);
	private static final byte[] STORE = "STORE".getBytes(Charsets.UTF_8);

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

	@SuppressWarnings("rawtypes")
	static List<byte[]> toBytesList(Reply[] replies) {
		if (replies == null) {
			return null;
		}
		List<byte[]> list = new ArrayList<byte[]>(replies.length);
		for (Reply reply : replies) {
			Object data = reply.data();
			if (data == null) {
				list.add(null);
			} else if (data instanceof byte[])
				list.add((byte[]) data);
			else
				throw new IllegalArgumentException("array contains more then just nulls and bytes -> " + data);
		}

		return list;
	}

	static List<String> asStatusList(Reply[] replies) {
		List<String> statuses = new ArrayList<String>();
		for (Reply reply : replies) {
			statuses.add(((StatusReply) reply).data());
		}
		return statuses;
	}

	static <T> List<T> toList(T[] byteArrays) {
		return Arrays.asList(byteArrays);
	}

	@SuppressWarnings("rawtypes")
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
		return convertTuple(zrange.data());
	}

	@SuppressWarnings("rawtypes")
	static Set<Tuple> convertTuple(Reply[] byteArrays) {
		Set<Tuple> tuples = new LinkedHashSet<Tuple>(byteArrays.length / 2 + 1);

		for (int i = 0; i < byteArrays.length; i++) {
			byte[] value = (byte[]) byteArrays[i].data();
			i++;
			Double score = toDouble((byte[]) byteArrays[i].data());
			tuples.add(new DefaultTuple(value, score));
		}

		return tuples;
	}

	static Object[] convert(int timeout, byte[]... keys) {
		int length = (keys != null ? keys.length + 1 : 1);

		Object[] args = new Object[length];
		if (keys != null) {
			for (int i = 0; i < keys.length; i++) {
				args[i] = keys[i];
			}
		}
		args[length - 1] = String.valueOf(timeout).getBytes();
		return args;
	}

	@SuppressWarnings("rawtypes")
	static Map<byte[], byte[]> toMap(Reply[] byteArrays) {
		Map<byte[], byte[]> map = new LinkedHashMap<byte[], byte[]>(byteArrays.length / 2);
		for (int i = 0; i < byteArrays.length; i++) {
			map.put((byte[]) byteArrays[i++].data(), (byte[]) byteArrays[i].data());
		}
		return map;
	}

	static Boolean asBoolean(IntegerReply reply) {
		if (reply == null) {
			return null;
		}
		return (Long.valueOf(1).equals(reply.data()));
	}

	static byte[] limit(long offset, long count) {
		return ("LIMIT " + offset + " " + count).getBytes(Charsets.UTF_8);
	}

	static Object[] limitParams(long offset, long count) {
		return new Object[] { "LIMIT".getBytes(Charsets.UTF_8), String.valueOf(offset).getBytes(Charsets.UTF_8),
				String.valueOf(count).getBytes(Charsets.UTF_8) };
	}

	static byte[] sort(SortParameters params) {
		return sort(params, null);
	}

	static byte[] sort(SortParameters params, byte[] sortKey) {
		List<byte[]> arrays = new ArrayList<byte[]>();

		Object[] sortParams = sortParams(params, sortKey);
		for (Object param : sortParams) {
			arrays.add((byte[]) param);
			arrays.add(SPACE);
		}
		arrays.remove(arrays.size() - 1);

		// concatenate array
		int size = 0;

		for (Object bs : arrays) {
			size += ((byte[]) bs).length;
		}
		byte[] result = new byte[size];

		int index = 0;
		for (byte[] bs : arrays) {
			System.arraycopy(bs, 0, result, index, bs.length);
			index += bs.length;
		}

		return result;
	}

	static Object[] sortParams(SortParameters params) {
		return sortParams(params, null);
	}

	static Object[] sortParams(SortParameters params, byte[] sortKey) {
		List<byte[]> arrays = new ArrayList<byte[]>();

		if (params != null) {
			if (params.getByPattern() != null) {
				arrays.add(BY);
				arrays.add(params.getByPattern());
			}

			if (params.getLimit() != null) {
				arrays.add(limit(params.getLimit().getStart(), params.getLimit().getCount()));
			}

			if (params.getGetPattern() != null) {
				byte[][] pattern = params.getGetPattern();
				for (byte[] bs : pattern) {
					arrays.add(GET);
					arrays.add(bs);
				}
			}

			if (params.getOrder() != null) {
				arrays.add(params.getOrder().name().getBytes(Charsets.UTF_8));
			}

			Boolean isAlpha = params.isAlphabetic();
			if (isAlpha != null && isAlpha) {
				arrays.add(ALPHA);
			}
		}

		if (sortKey != null) {
			arrays.add(STORE);
			arrays.add(sortKey);
		}

		return arrays.toArray();
	}

	static byte[] bitOp(BitOperation op) {
		Assert.notNull(op, "The bit operation is required");
		return op.name().toUpperCase().getBytes(Charsets.UTF_8);
	}

	static String asShasum(Reply reply) {
		Object data = reply.data();
		return (data instanceof String ? (String) data : new String((byte[]) data, Charsets.UTF_8));
	}

	static List<Boolean> asBooleanList(Reply reply) {
		if (!(reply instanceof MultiBulkReply)) {
			throw new IllegalArgumentException();
		}
		List<Boolean> results = new ArrayList<Boolean>();
		for (Reply r : ((MultiBulkReply) reply).data()) {
			results.add(SrpUtils.asBoolean((IntegerReply) r));
		}
		return results;
	}

	static List<Long> asIntegerList(Reply[] replies) {
		List<Long> results = new ArrayList<Long>();
		for (Reply reply : replies) {
			results.add(((IntegerReply) reply).data());
		}
		return results;
	}

	static List<Object> asList(MultiBulkReply genericReply) {
		Reply[] replies = genericReply.data();
		List<Object> results = new ArrayList<Object>();
		for (Reply reply : replies) {
			results.add(reply.data());
		}
		return results;
	}

	static Object convertScriptReturn(ReturnType returnType, Reply reply) {
		if (reply instanceof MultiBulkReply) {
			return SrpUtils.asList((MultiBulkReply) reply);
		}
		if (returnType == ReturnType.BOOLEAN) {
			// Lua false comes back as a null bulk reply
			if (reply.data() == null) {
				return Boolean.FALSE;
			}
			return ((Long) reply.data() == 1);
		}
		return reply.data();
	}
}
