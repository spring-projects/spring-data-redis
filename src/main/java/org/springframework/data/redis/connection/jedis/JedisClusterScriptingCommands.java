/*
 * Copyright 2017-2019 the original author or authors.
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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisScriptingCommands;
import org.springframework.data.redis.connection.ReturnType;
import redis.clients.jedis.JedisCluster;

/**
 * @author Mark Paluch
 * @since 2.0
 */
@RequiredArgsConstructor
class JedisClusterScriptingCommands implements RedisScriptingCommands {

	private final @NonNull JedisClusterConnection clusterConnection;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptFlush()
	 */
	@Override
	public void scriptFlush() {
		throw new InvalidDataAccessApiUsageException("ScriptFlush is not supported in cluster environment.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptKill()
	 */
	@Override
	public void scriptKill() {
		throw new InvalidDataAccessApiUsageException("ScriptKill is not supported in cluster environment.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptLoad(byte[])
	 */
	@Override
	public String scriptLoad(byte[] script) {
		throw new InvalidDataAccessApiUsageException("ScriptLoad is not supported in cluster environment.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#scriptExists(java.lang.String[])
	 */
	@Override
	public List<Boolean> scriptExists(String... scriptShas) {
		throw new InvalidDataAccessApiUsageException("ScriptExists is not supported in cluster environment.");
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#eval(byte[], org.springframework.data.redis.connection.ReturnType, int, byte[][])
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> T eval(byte[] script, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		checkConnection();
		try {
			return (T) new JedisScriptReturnConverter(returnType)
					.convert(getCluster().eval(script, JedisConverters.toBytes(numKeys), keysAndArgs));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#evalSha(java.lang.String, org.springframework.data.redis.connection.ReturnType, int, byte[][])
	 */
	@Override
	public <T> T evalSha(String scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		return evalSha(JedisConverters.toBytes(scriptSha), returnType, numKeys, keysAndArgs);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisScriptingCommands#evalSha(byte[], org.springframework.data.redis.connection.ReturnType, int, byte[][])
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> T evalSha(byte[] scriptSha, ReturnType returnType, int numKeys, byte[]... keysAndArgs) {
		checkConnection();
		try {
			return (T) new JedisScriptReturnConverter(returnType)
					.convert(getCluster().evalsha(scriptSha, numKeys, keysAndArgs));
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	public JedisClusterConnection getClusterConnection() {
		return clusterConnection;
	}

	protected RuntimeException convertJedisAccessException(Exception ex) {
		return clusterConnection.convertJedisAccessException(ex);
	}

	private JedisCluster getCluster() {
		return clusterConnection.getCluster();
	}

	protected void checkConnection() {
		if (isQueueing()) {
			throw new UnsupportedOperationException();
		}
		if (isPipelined()) {
			throw new UnsupportedOperationException();
		}
	}

	private boolean isPipelined() {
		return clusterConnection.isPipelined();
	}

	private boolean isQueueing() {
		return clusterConnection.isQueueing();
	}
}
