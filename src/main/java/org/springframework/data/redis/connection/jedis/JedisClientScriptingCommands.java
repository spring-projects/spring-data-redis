/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import java.util.List;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.connection.RedisScriptingCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.util.Assert;

import redis.clients.jedis.UnifiedJedis;

/**
 * @author Tihomir Mateev
 * @since 4.1
 */
@NullUnmarked
class JedisClientScriptingCommands implements RedisScriptingCommands {

	private static final byte[] SAMPLE_KEY = new byte[0];
	private final JedisClientConnection connection;

	JedisClientScriptingCommands(@NonNull JedisClientConnection connection) {
		this.connection = connection;
	}

	@Override
	public void scriptFlush() {
		connection.execute(UnifiedJedis::scriptFlush, pipeline -> pipeline.scriptFlush(SAMPLE_KEY));
	}

	@Override
	public void scriptKill() {
		connection.execute(UnifiedJedis::scriptKill, pipeline -> pipeline.scriptKill(SAMPLE_KEY));
	}

	@Override
	public String scriptLoad(byte @NonNull [] script) {

		Assert.notNull(script, "Script must not be null");

		return connection.execute(client -> client.scriptLoad(script, SAMPLE_KEY),
				pipeline -> pipeline.scriptLoad(script, SAMPLE_KEY), JedisConverters::toString);
	}

	@Override
	public List<@NonNull Boolean> scriptExists(@NonNull String @NonNull... scriptSha1) {

		Assert.notNull(scriptSha1, "Script digests must not be null");
		Assert.noNullElements(scriptSha1, "Script digests must not contain null elements");

		byte[][] sha1 = new byte[scriptSha1.length][];
		for (int i = 0; i < scriptSha1.length; i++) {
			sha1[i] = JedisConverters.toBytes(scriptSha1[i]);
		}

		return connection.execute(client -> client.scriptExists(SAMPLE_KEY, sha1),
				pipeline -> pipeline.scriptExists(SAMPLE_KEY, sha1));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T eval(byte @NonNull [] script, @NonNull ReturnType returnType, int numKeys,
			byte @NonNull [] @NonNull... keysAndArgs) {

		Assert.notNull(script, "Script must not be null");

		JedisScriptReturnConverter converter = new JedisScriptReturnConverter(returnType);
		return (T) connection.execute(client -> client.eval(script, numKeys, keysAndArgs),
				pipeline -> pipeline.eval(script, numKeys, keysAndArgs), converter, () -> converter.convert(null));
	}

	@Override
	public <T> T evalSha(@NonNull String scriptSha1, @NonNull ReturnType returnType, int numKeys,
			byte @NonNull [] @NonNull... keysAndArgs) {
		return evalSha(JedisConverters.toBytes(scriptSha1), returnType, numKeys, keysAndArgs);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T evalSha(byte @NonNull [] scriptSha, @NonNull ReturnType returnType, int numKeys,
			byte @NonNull [] @NonNull... keysAndArgs) {

		Assert.notNull(scriptSha, "Script digest must not be null");

		JedisScriptReturnConverter converter = new JedisScriptReturnConverter(returnType);
		return (T) connection.execute(client -> client.evalsha(scriptSha, numKeys, keysAndArgs),
				pipeline -> pipeline.evalsha(scriptSha, numKeys, keysAndArgs), converter, () -> converter.convert(null));
	}

}
