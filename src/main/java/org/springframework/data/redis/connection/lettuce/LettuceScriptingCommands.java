/*
 * Copyright 2017-2023 the original author or authors.
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

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.RedisScriptingCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 * @since 2.0
 */
class LettuceScriptingCommands implements RedisScriptingCommands {

	private final LettuceConnection connection;

	LettuceScriptingCommands(LettuceConnection connection) {
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptFlush()
	 */
	@Override
	public void scriptFlush() {
		connection.invoke().just(RedisScriptingAsyncCommands::scriptFlush);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptKill()
	 */
	@Override
	public void scriptKill() {

		if (connection.isQueueing()) {
			throw new UnsupportedOperationException("Script kill not permitted in a transaction");
		}

		connection.invoke().just(RedisScriptingAsyncCommands::scriptKill);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptLoad(byte[])
	 */
	@Override
	public String scriptLoad(byte[] script) {

		Assert.notNull(script, "Script must not be null!");

		return connection.invoke().just(RedisScriptingAsyncCommands::scriptLoad, script);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptExists(java.lang.String[])
	 */
	@Override
	public List<Boolean> scriptExists(String... scriptSha1) {

		Assert.notNull(scriptSha1, "Script digests must not be null!");
		Assert.noNullElements(scriptSha1, "Script digests must not contain null elements!");

		return connection.invoke().just(RedisScriptingAsyncCommands::scriptExists, scriptSha1);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#eval(byte[], org.springframework.data.redis.connection.ReturnType, int, byte[][])
	 */
	@Override
	public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {

		Assert.notNull(script, "Script must not be null!");

		byte[][] keys = extractScriptKeys(numKeys, keysAndArgs);
		byte[][] args = extractScriptArgs(numKeys, keysAndArgs);
		String convertedScript = LettuceConverters.toString(script);

		return connection
				.invoke().from(RedisScriptingAsyncCommands::eval, convertedScript,
						LettuceConverters.toScriptOutputType(returnType), keys, args)
				.get(new LettuceEvalResultsConverter<T>(returnType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#evalSha(java.lang.String, org.springframework.data.redis.connection.ReturnType, int, byte[][])
	 */
	@Override
	public <T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {

		Assert.notNull(scriptSha1, "Script digest must not be null!");

		byte[][] keys = extractScriptKeys(numKeys, keysAndArgs);
		byte[][] args = extractScriptArgs(numKeys, keysAndArgs);

		return connection
				.invoke().from(RedisScriptingAsyncCommands::evalsha, scriptSha1,
						LettuceConverters.toScriptOutputType(returnType), keys, args)
				.get(new LettuceEvalResultsConverter<T>(returnType));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#evalSha(byte[], org.springframework.data.redis.connection.ReturnType, int, byte[][])
	 */
	@Override
	public <T> T evalSha(byte[] scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {

		Assert.notNull(scriptSha1, "Script digest must not be null!");

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
					if (obj instanceof Exception) {
						throw connection.convertLettuceAccessException((Exception) obj);
					}
				}
			}
			return (T) source;
		}
	}
}
