/*
 * Copyright 2017-2022 the original author or authors.
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

import redis.clients.jedis.Jedis;

import java.util.List;

import org.springframework.data.redis.connection.RedisScriptingCommands;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.util.Assert;

/**
 * @author Mark Paluch
 * @since 2.0
 */
class JedisScriptingCommands implements RedisScriptingCommands {

	private final JedisConnection connection;

	JedisScriptingCommands(JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public void scriptFlush() {

		assertDirectMode();

		connection.invoke().just(Jedis::scriptFlush);
	}

	@Override
	public void scriptKill() {

		assertDirectMode();

		connection.invoke().just(Jedis::scriptKill);
	}

	@Override
	public String scriptLoad(byte[] script) {

		Assert.notNull(script, "Script must not be null!");
		assertDirectMode();

		return connection.invoke().from(it -> it.scriptLoad(script)).get(JedisConverters::toString);
	}

	@Override
	public List<Boolean> scriptExists(String... scriptSha1) {

		Assert.notNull(scriptSha1, "Script digests must not be null!");
		Assert.noNullElements(scriptSha1, "Script digests must not contain null elements!");
		assertDirectMode();

		return connection.invoke().just(it -> it.scriptExists(scriptSha1));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {

		Assert.notNull(script, "Script must not be null!");
		assertDirectMode();

		JedisScriptReturnConverter converter = new JedisScriptReturnConverter(returnType);
		return (T) connection.invoke().from(it -> it.eval(script, numKeys, keysAndArgs)).getOrElse(converter,
				() -> converter.convert(null));
	}

	@Override
	public <T> T evalSha(String scriptSha1, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		return evalSha(JedisConverters.toBytes(scriptSha1), returnType, numKeys, keysAndArgs);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T evalSha(byte[] scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {

		Assert.notNull(scriptSha, "Script digest must not be null!");
		assertDirectMode();

		JedisScriptReturnConverter converter = new JedisScriptReturnConverter(returnType);
		return (T) connection.invoke().from(it -> it.evalsha(scriptSha, numKeys, keysAndArgs)).getOrElse(converter,
				() -> converter.convert(null));
	}

	private void assertDirectMode() {
		if (connection.isQueueing() || connection.isPipelined()) {
			throw new UnsupportedOperationException("Scripting commands not supported in pipelining/transaction mode");
		}
	}

}
