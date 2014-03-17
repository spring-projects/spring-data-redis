/*
 * Copyright 2013 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.connection.RedisListCommands.Position;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.SortParameters.Order;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.util.Assert;

import com.lambdaworks.redis.KeyValue;
import com.lambdaworks.redis.ScoredValue;
import com.lambdaworks.redis.ScriptOutputType;
import com.lambdaworks.redis.SortArgs;
import com.lambdaworks.redis.protocol.Charsets;

/**
 * Lettuce type converters
 * 
 * @author Jennifer Hickey
 */
abstract public class LettuceConverters extends Converters {

	private static final Converter<Date, Long> DATE_TO_LONG;
	private static final Converter<List<byte[]>, Set<byte[]>> BYTES_LIST_TO_BYTES_SET;
	private static final Converter<byte[], String> BYTES_TO_STRING;
	private static final Converter<Set<byte[]>, List<byte[]>> BYTES_SET_TO_BYTES_LIST;
	private static final Converter<KeyValue<byte[], byte[]>, List<byte[]>> KEY_VALUE_TO_BYTES_LIST;
	private static final Converter<List<ScoredValue<byte[]>>, Set<Tuple>> SCORED_VALUES_TO_TUPLE_SET;
	private static final Converter<ScoredValue<byte[]>, Tuple> SCORED_VALUE_TO_TUPLE;
	private static final Converter<Exception, DataAccessException> EXCEPTION_CONVERTER = new LettuceExceptionConverter();

	static {
		DATE_TO_LONG = new Converter<Date, Long>() {
			public Long convert(Date source) {
				return source != null ? source.getTime() : null;
			}

		};
		BYTES_LIST_TO_BYTES_SET = new Converter<List<byte[]>, Set<byte[]>>() {
			public Set<byte[]> convert(List<byte[]> results) {
				return results != null ? new LinkedHashSet<byte[]>(results) : null;
			}

		};
		BYTES_TO_STRING = new Converter<byte[], String>() {

			@Override
			public String convert(byte[] source) {
				if (source == null || Arrays.equals(source, new byte[0])) {
					return null;
				}
				return new String(source);
			}

		};
		BYTES_SET_TO_BYTES_LIST = new Converter<Set<byte[]>, List<byte[]>>() {
			public List<byte[]> convert(Set<byte[]> results) {
				return results != null ? new ArrayList<byte[]>(results) : null;
			}
		};
		KEY_VALUE_TO_BYTES_LIST = new Converter<KeyValue<byte[], byte[]>, List<byte[]>>() {
			public List<byte[]> convert(KeyValue<byte[], byte[]> source) {
				if (source == null) {
					return null;
				}
				List<byte[]> list = new ArrayList<byte[]>(2);
				list.add(source.key);
				list.add(source.value);
				return list;
			}
		};
		SCORED_VALUES_TO_TUPLE_SET = new Converter<List<ScoredValue<byte[]>>, Set<Tuple>>() {
			public Set<Tuple> convert(List<ScoredValue<byte[]>> source) {
				if (source == null) {
					return null;
				}
				Set<Tuple> tuples = new LinkedHashSet<Tuple>(source.size());
				for (ScoredValue<byte[]> value : source) {
					tuples.add(LettuceConverters.toTuple(value));
				}
				return tuples;
			}
		};
		SCORED_VALUE_TO_TUPLE = new Converter<ScoredValue<byte[]>, Tuple>() {
			public Tuple convert(ScoredValue<byte[]> source) {
				return source != null ? new DefaultTuple(source.value, Double.valueOf(source.score)) : null;
			}

		};
	}

	public static Converter<Date, Long> dateToLong() {
		return DATE_TO_LONG;
	}

	public static Converter<List<byte[]>, Set<byte[]>> bytesListToBytesSet() {
		return BYTES_LIST_TO_BYTES_SET;
	}

	public static Converter<byte[], String> bytesToString() {
		return BYTES_TO_STRING;
	}

	public static Converter<KeyValue<byte[], byte[]>, List<byte[]>> keyValueToBytesList() {
		return KEY_VALUE_TO_BYTES_LIST;
	}

	public static Converter<Set<byte[]>, List<byte[]>> bytesSetToBytesList() {
		return BYTES_SET_TO_BYTES_LIST;
	}

	public static Converter<List<ScoredValue<byte[]>>, Set<Tuple>> scoredValuesToTupleSet() {
		return SCORED_VALUES_TO_TUPLE_SET;
	}

	public static Converter<ScoredValue<byte[]>, Tuple> scoredValueToTuple() {
		return SCORED_VALUE_TO_TUPLE;
	}

	public static Converter<Exception, DataAccessException> exceptionConverter() {
		return EXCEPTION_CONVERTER;
	}

	public static Long toLong(Date source) {
		return DATE_TO_LONG.convert(source);
	}

	public static Set<byte[]> toBytesSet(List<byte[]> source) {
		return BYTES_LIST_TO_BYTES_SET.convert(source);
	}

	public static List<byte[]> toBytesList(KeyValue<byte[], byte[]> source) {
		return KEY_VALUE_TO_BYTES_LIST.convert(source);
	}

	public static List<byte[]> toBytesList(Set<byte[]> source) {
		return BYTES_SET_TO_BYTES_LIST.convert(source);
	}

	public static Set<Tuple> toTupleSet(List<ScoredValue<byte[]>> source) {
		return SCORED_VALUES_TO_TUPLE_SET.convert(source);
	}

	public static Tuple toTuple(ScoredValue<byte[]> source) {
		return SCORED_VALUE_TO_TUPLE.convert(source);
	}

	public static DataAccessException toDataAccessException(Exception ex) {
		return EXCEPTION_CONVERTER.convert(ex);
	}

	public static String toString(byte[] source) {
		return BYTES_TO_STRING.convert(source);
	}

	public static ScriptOutputType toScriptOutputType(ReturnType returnType) {
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

	public static boolean toBoolean(Position where) {
		Assert.notNull("list positions are mandatory");
		return (Position.AFTER.equals(where) ? false : true);
	}

	public static int toInt(boolean value) {
		return (value ? 1 : 0);
	}

	public static SortArgs toSortArgs(SortParameters params) {
		SortArgs args = new SortArgs();
		if (params == null) {
			return args;
		}
		if (params.getByPattern() != null) {
			args.by(new String(params.getByPattern(), Charsets.ASCII));
		}
		if (params.getLimit() != null) {
			args.limit(params.getLimit().getStart(), params.getLimit().getCount());
		}
		if (params.getGetPattern() != null) {
			byte[][] pattern = params.getGetPattern();
			for (byte[] bs : pattern) {
				args.get(new String(bs, Charsets.ASCII));
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
}
