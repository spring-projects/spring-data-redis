/*
 * Copyright 2014-present the original author or authors.
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

import java.io.IOException;
import java.util.List;

import org.springframework.data.redis.connection.NamedNode;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelCommands;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisServer;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @since 1.4
 */
public class JedisSentinelConnection implements RedisSentinelConnection {

	private Jedis jedis;

	public JedisSentinelConnection(RedisNode sentinel) {
		this(sentinel.getHost(), sentinel.getPort());
	}

	public JedisSentinelConnection(String host, int port) {
		this(new Jedis(host, port));
	}

	public JedisSentinelConnection(Jedis jedis) {

		Assert.notNull(jedis, "Cannot created JedisSentinelConnection using 'null' as client");
		this.jedis = jedis;
		init();
	}

	@Override
	public void failover(NamedNode master) {

		Assert.notNull(master, "Redis node master must not be 'null' for failover");
		Assert.hasText(master.getName(), "Redis master name must not be 'null' or empty for failover");
		jedis.sentinelFailover(master.getName());
	}

	@Override
	public List<RedisServer> masters() {
		return JedisConverters.toListOfRedisServer(jedis.sentinelMasters());
	}

	@Override
	public List<RedisServer> replicas(NamedNode master) {

		Assert.notNull(master, "Master node cannot be 'null' when loading replicas");
		return replicas(master.getName());
	}

	/**
	 * @param masterName
	 * @see RedisSentinelCommands#replicas(NamedNode)
	 * @return
	 */
	public List<RedisServer> replicas(String masterName) {

		Assert.hasText(masterName, "Name of redis master cannot be 'null' or empty when loading replicas");
		return JedisConverters.toListOfRedisServer(jedis.sentinelReplicas(masterName));
	}

	@Override
	public void remove(NamedNode master) {

		Assert.notNull(master, "Master node cannot be 'null' when trying to remove");
		remove(master.getName());
	}

	/**
	 * @param masterName
	 * @see RedisSentinelCommands#remove(NamedNode)
	 */
	public void remove(String masterName) {

		Assert.hasText(masterName, "Name of redis master cannot be 'null' or empty when trying to remove");
		jedis.sentinelRemove(masterName);
	}

	@Override
	public void monitor(RedisServer server) {

		Assert.notNull(server, "Cannot monitor 'null' server");
		Assert.hasText(server.getName(), "Name of server to monitor must not be 'null' or empty");
		Assert.hasText(server.getHost(), "Host must not be 'null' for server to monitor");
		Assert.notNull(server.getPort(), "Port must not be 'null' for server to monitor");
		Assert.notNull(server.getQuorum(), "Quorum must not be 'null' for server to monitor");
		jedis.sentinelMonitor(server.getName(), server.getHost(), server.getPort().intValue(),
				server.getQuorum().intValue());
	}

	@Override
	public void close() throws IOException {
		jedis.close();
	}

	private void init() {
		if (!jedis.isConnected()) {
			doInit(jedis);
		}
	}

	/**
	 * Do what ever is required to establish the connection to redis.
	 *
	 * @param jedis
	 */
	protected void doInit(Jedis jedis) {
		jedis.connect();
	}

	@Override
	public boolean isOpen() {
		return jedis.isConnected();
	}

}
