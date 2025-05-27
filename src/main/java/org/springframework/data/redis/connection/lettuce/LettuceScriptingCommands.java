/*
 * Copyright 2017-2025 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.api.async.RedisScriptingAsyncCommands;

import java.util.Arrays;
import java.util.List;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.core.convert.converter.Converter;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisScriptingCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 * @since 2.0
 */
@NullUnmarked
class LettuceScriptingCommands implements RedisScriptingCommands {

	private final LettuceConnection connection;

	LettuceScriptingCommands(@NonNull LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public void scriptFlush() {
		connection.invoke().just(RedisScriptingAsyncCommands::scriptFlush);
	}

	@Override
	public void scriptKill() {

		if (connection.isQueueing()) {
			throw new InvalidDataAccessApiUsageException("Script kill not permitted in a transaction");
		}

		connection.invoke().just(RedisScriptingAsyncCommands::scriptKill);
	}

	@Override
	public String scriptLoad(byte @NonNull [] script) {

		Assert.notNull(script, "Script must not be null");

		return connection.invoke().just(RedisScriptingAsyncCommands::scriptLoad, script);
	}

	@Override
	public List<Boolean> scriptExists(@NonNull String @NonNull... scriptSha1) {

		Assert.notNull(scriptSha1, "Script digests must not be null");
		Assert.noNullElements(scriptSha1, "Script digests must not contain null elements");

		return connection.invoke().just(RedisScriptingAsyncCommands::scriptExists, scriptSha1);
	}

	@Override
	public <T> T eval(byte @NonNull [] script, @NonNull ReturnType returnType, int numKeys,
			byte @NonNull [] @NonNull... keysAndArgs) {

		Assert.notNull(script, "Script must not be null");

		byte[][] keys = extractScriptKeys(numKeys, keysAndArgs);
		byte[][] args = extractScriptArgs(numKeys, keysAndArgs);
		String convertedScript = LettuceConverters.toString(script);

		return connection
				.invoke().from(RedisScriptingAsyncCommands::eval, convertedScript,
						LettuceConverters.toScriptOutputType(returnType), keys, args)
				.get(new LettuceEvalResultsConverter<T>(returnType));
	}

	@Override
	public <T> T evalSha(@NonNull String scriptSha1, @NonNull ReturnType returnType, int numKeys,
			byte @NonNull [] @NonNull... keysAndArgs) {

		Assert.notNull(scriptSha1, "Script digest must not be null");

		byte[][] keys = extractScriptKeys(numKeys, keysAndArgs);
		byte[][] args = extractScriptArgs(numKeys, keysAndArgs);

		return connection
				.invoke().from(RedisScriptingAsyncCommands::evalsha, scriptSha1,
						LettuceConverters.toScriptOutputType(returnType), keys, args)
				.get(new LettuceEvalResultsConverter<T>(returnType));
	}

	@Override
	public <T> T evalSha(byte @NonNull [] scriptSha1, @NonNull ReturnType returnType, int numKeys,
			byte @NonNull [] @NonNull... keysAndArgs) {

		Assert.notNull(scriptSha1, "Script digest must not be null");

		return evalSha(LettuceConverters.toString(scriptSha1), returnType, numKeys, keysAndArgs);
	}

	private static byte[][] extractScriptKeys(int numKeys, byte[]... keysAndArgs) {
		if (numKeys > 0) {
			return Arrays.copyOfRange(keysAndArgs, 0, numKeys);
		}
		return new byte[0][0];
	}

	private static byte[][] extractScriptArgs(int numKeys, byte[]... keysAndArgs) {
		if (keysAndArgs.length > numKeys) {
			return Arrays.copyOfRange(keysAndArgs, numKeys, keysAndArgs.length);
		}
		return new byte[0][0];
	}

	private class LettuceEvalResultsConverter<T> implements Converter<Object, T> {

		private final ReturnType returnType;

		public LettuceEvalResultsConverter(ReturnType returnType) {
			this.returnType = returnType;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public T convert(Object source) {
			if (returnType == ReturnType.MULTI) {
				List resultList = (List) source;
				for (Object obj : resultList) {
					if (obj instanceof Exception ex) {
						throw connection.convertLettuceAccessException(ex);
					}
				}
			}
			return (T) source;
		}
	}
}
