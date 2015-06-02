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

package org.springframework.data.redis.connection.lettuce;

import java.io.StringReader;
import java.util.*;

import com.lambdaworks.redis.*;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.protocol.LettuceCharsets;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.connection.RedisZSetCommands.Aggregate;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.SortParameters.Order;
import org.springframework.util.Assert;

/**
 * Helper class featuring methods for Lettuce connection handling, providing support for exception translation.
 * Deprecated in favor of {@link LettuceConverters}
 * 
 * @author Costin Leau
 */
@Deprecated
abstract class LettuceUtils {

	static final RedisCodec<byte[], byte[]> CODEC = new BytesRedisCodec();

	static DataAccessException convertRedisAccessException(RuntimeException ex) {
		if (ex instanceof RedisCommandInterruptedException) {
			return new RedisSystemException("Redis command interrupted", ex);
		}
		if (ex instanceof RedisException) {
			return new RedisSystemException("Redis exception", ex);
		}
		return null;
	}

	static Properties info(String reply) {
		if (reply == null) {
			return null;
		}
		Properties info = new Properties();
		StringReader stringReader = new StringReader(reply);
		try {
			info.load(stringReader);
		} catch (Exception ex) {
			throw new RedisSystemException("Cannot read Redis info", ex);
		} finally {
			stringReader.close();
		}
		return info;
	}

	static int asBit(boolean value) {
		return (value ? 1 : 0);
	}

	static boolean convertPosition(Position where) {
		Assert.notNull("list positions are mandatory");
		return (Position.AFTER.equals(where) ? false : true);
	}

	static Set<Tuple> convertTuple(List<ScoredValue<byte[]>> zrange) {
		if (zrange == null) {
			return null;
		}
		Set<Tuple> tuples = new LinkedHashSet<Tuple>(zrange.size());

		for (int i = 0; i < zrange.size(); i++) {
			tuples.add(new DefaultTuple(zrange.get(i).value, Double.valueOf(zrange.get(i).score)));
		}
		return tuples;
	}

	static SortArgs sort(SortParameters params) {
		SortArgs args = new SortArgs();

		if (params == null) {
			return args;
		}

		if (params.getByPattern() != null) {
			args.by(new String(params.getByPattern(), LettuceCharsets.ASCII));
		}

		if (params.getLimit() != null) {
			args.limit(params.getLimit().getStart(), params.getLimit().getCount());
		}

		if (params.getGetPattern() != null) {
			byte[][] pattern = params.getGetPattern();
			for (byte[] bs : pattern) {
				args.get(new String(bs, LettuceCharsets.ASCII));
			}
		}

		if (params.getOrder() != null) {
			if (params.getOrder() == Order.ASC) {
				args.asc();
			} else {
				args.desc();
			}
		}

		Boolean isAlpha = params.isAlphabetic();
		if (isAlpha != null && isAlpha) {
			args.alpha();
		}
		return args;
	}

	static ZStoreArgs zArgs(Aggregate aggregate, int[] weights) {
		ZStoreArgs args = new ZStoreArgs();

		if (aggregate != null) {
			switch (aggregate) {
				case MIN:
					args.min();
					break;
				case MAX:
					args.max();
					break;
				default:
					args.sum();
					break;
			}
		}

		long[] lg = new long[weights.length];
		for (int i = 0; i < lg.length; i++) {
			lg[i] = (long) weights[i];
		}
		args.weights(lg);
		return args;
	}

	static List<byte[]> toList(KeyValue<byte[], byte[]> blpop) {
		if (blpop == null) {
			return null;
		}
		List<byte[]> list = new ArrayList<byte[]>(2);
		list.add(blpop.key);
		list.add(blpop.value);
		return list;
	}

	static ScriptOutputType toScriptOutputType(ReturnType returnType) {
		switch (returnType) {
			case BOOLEAN:
				return ScriptOutputType.BOOLEAN;
			case MULTI:
				return ScriptOutputType.MULTI;
			case VALUE:
				return ScriptOutputType.VALUE;
			case INTEGER:
				return ScriptOutputType.INTEGER;
			case STATUS:
				return ScriptOutputType.STATUS;
			default:
				throw new IllegalArgumentException("Return type " + returnType + " is not a supported script output type");
		}
	}

	static byte[][] extractScriptKeys(int numKeys, byte[]... keysAndArgs) {
		if (numKeys > 0) {
			return Arrays.copyOfRange(keysAndArgs, 0, numKeys);
		}
		return new byte[0][0];
	}

	static byte[][] extractScriptArgs(int numKeys, byte[]... keysAndArgs) {
		if (keysAndArgs.length > numKeys) {
			return Arrays.copyOfRange(keysAndArgs, numKeys, keysAndArgs.length);
		}
		return new byte[0][0];
	}

}
