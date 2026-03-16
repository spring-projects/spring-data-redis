/*
 * Copyright 2017-present the original author or authors.
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

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ClusterCommandExecutor;
import org.springframework.data.redis.connection.RedisScriptingCommands;
import org.springframework.util.Assert;

/**
 * Cluster {@link RedisScriptingCommands} implementation for Jedis.
 * <p>
 * This class can be used to override only methods that require cluster-specific handling.
 * <p>
 * Pipeline and transaction modes are not supported in cluster mode.
 *
 * @author Mark Paluch
 * @author Pavel Khokhlov
 * @author Tihomir Mateev
 * @since 2.0
 */
@NullUnmarked
class JedisClusterScriptingCommands extends JedisScriptingCommands {

	private final JedisClusterConnection connection;

	JedisClusterScriptingCommands(@NonNull JedisClusterConnection connection) {
		super(connection);
		this.connection = connection;
	}

	@Override
	public void scriptFlush() {

		try {
			connection.getClusterCommandExecutor()
					.executeCommandOnAllNodes((JedisClusterConnection.JedisClusterCommandCallback<String>) Jedis::scriptFlush);
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	@Override
	public void scriptKill() {

		try {
			connection.getClusterCommandExecutor()
					.executeCommandOnAllNodes((JedisClusterConnection.JedisClusterCommandCallback<String>) Jedis::scriptKill);
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	@Override
	public String scriptLoad(byte @NonNull [] script) {

		Assert.notNull(script, "Script must not be null");

		try {
			ClusterCommandExecutor.MultiNodeResult<byte[]> multiNodeResult = connection.getClusterCommandExecutor()
					.executeCommandOnAllNodes(
							(JedisClusterConnection.JedisClusterCommandCallback<byte[]>) client -> client.scriptLoad(script));

			return JedisConverters.toString(multiNodeResult.getFirstNonNullNotEmptyOrDefault(new byte[0]));
		} catch (Exception ex) {
			throw connection.convertJedisAccessException(ex);
		}
	}

	@Override
	public List<Boolean> scriptExists(@NonNull String @NonNull... scriptShas) {
		throw new InvalidDataAccessApiUsageException("ScriptExists is not supported in cluster environment");
	}

	// eval() and evalSha() are inherited from JedisScriptingCommands
	// UnifiedJedis handles cluster routing automatically for these commands
}
