/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.redis.core.script;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.data.redis.serializer.RedisElementReader;
import org.springframework.data.redis.serializer.RedisSerializer;

/**
 * Utilities for Lua script execution and result deserialization.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
class ScriptUtils {

	private ScriptUtils() {}

	/**
	 * Deserialize {@code result} using {@link RedisSerializer} to the serializer type. Collection types and intermediate
	 * collection elements are deserialized recursivly.
	 *
	 * @param resultSerializer must not be {@literal null}.
	 * @param result must not be {@literal null}.
	 * @return the deserialized result.
	 */
	@SuppressWarnings({ "unchecked" })
	static <T> T deserializeResult(RedisSerializer<T> resultSerializer, Object result) {

		if (result instanceof byte[]) {
			return resultSerializer.deserialize((byte[]) result);
		}

		if (result instanceof List) {

			List<Object> results = new ArrayList<>(((List) result).size());

			for (Object obj : (List) result) {
				results.add(deserializeResult(resultSerializer, obj));
			}

			return (T) results;
		}

		return (T) result;
	}

	/**
	 * Deserialize {@code result} using {@link RedisElementReader} to the reader type. Collection types and intermediate
	 * collection elements are deserialized recursively.
	 *
	 * @param reader must not be {@literal null}.
	 * @param result must not be {@literal null}.
	 * @return the deserialized result.
	 */
	@SuppressWarnings({ "unchecked" })
	static <T> T deserializeResult(RedisElementReader<T> reader, Object result) {

		if (result instanceof ByteBuffer) {
			return reader.read((ByteBuffer) result);
		}

		if (result instanceof List) {

			List<Object> results = new ArrayList<>(((List) result).size());

			for (Object obj : (List) result) {
				results.add(deserializeResult(reader, obj));
			}

			return (T) results;
		}
		return (T) result;
	}

	/**
	 * Checks whether given {@link Throwable} contains a {@code NOSCRIPT} error. {@code NOSCRIPT} is reported if a script
	 * was attempted to execute using {@code EVALSHA}.
	 *
	 * @param e the exception.
	 * @return {@literal true} if the exception or one of its causes contains a {@literal NOSCRIPT} error.
	 */
	static boolean exceptionContainsNoScriptError(Throwable e) {

		if (!(e instanceof NonTransientDataAccessException)) {
			return false;
		}

		Throwable current = e;
		while (current != null) {

			String exMessage = current.getMessage();
			if (exMessage != null && exMessage.contains("NOSCRIPT")) {
				return true;
			}

			current = current.getCause();
		}

		return false;
	}
}
