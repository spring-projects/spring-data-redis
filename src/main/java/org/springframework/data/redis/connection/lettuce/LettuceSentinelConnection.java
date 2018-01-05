/*
 * Copyright 2014-2018 the original author or authors.
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

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI.Builder;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.sentinel.api.StatefulRedisSentinelConnection;
import io.lettuce.core.sentinel.api.sync.RedisSentinelCommands;

import java.io.IOException;
import java.util.List;

import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.connection.NamedNode;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisServer;
import org.springframework.util.Assert;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.5
 */
public class LettuceSentinelConnection implements RedisSentinelConnection {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			LettuceConverters.exceptionConverter());

	private final LettuceConnectionProvider provider;
	private StatefulRedisSentinelConnection<String, String> connection; // no that should not be null

	/**
	 * Creates a {@link LettuceSentinelConnection} with a dedicated client for a supplied {@link RedisNode}.
	 *
	 * @param sentinel The sentinel to connect to.
	 */
	public LettuceSentinelConnection(RedisNode sentinel) {
		this(sentinel.getHost(), sentinel.getPort());
	}

	/**
	 * Creates a {@link LettuceSentinelConnection} with a client for the supplied {@code host} and {@code port}.
	 *
	 * @param host must not be {@literal null}.
	 * @param port sentinel port.
	 */
	public LettuceSentinelConnection(String host, int port) {

		Assert.notNull(host, "Cannot create LettuceSentinelConnection using 'null' as host.");

		this.provider = new DedicatedClientConnectionProvider(host, port);
		init();
	}

	/**
	 * Creates a {@link LettuceSentinelConnection} with a client for the supplied {@code host} and {@code port} and reuse
	 * existing {@link ClientResources}.
	 *
	 * @param host must not be {@literal null}.
	 * @param port sentinel port.
	 * @param clientResources must not be {@literal null}.
	 */
	public LettuceSentinelConnection(String host, int port, ClientResources clientResources) {

		Assert.notNull(clientResources, "Cannot create LettuceSentinelConnection using 'null' as ClientResources.");
		Assert.notNull(host, "Cannot create LettuceSentinelConnection using 'null' as host.");

		this.provider = new DedicatedClientConnectionProvider(host, port, clientResources);
		init();
	}

	/**
	 * Creates a {@link LettuceSentinelConnection} using a supplied {@link RedisClient}.
	 *
	 * @param redisClient must not be {@literal null}.
	 */
	public LettuceSentinelConnection(RedisClient redisClient) {

		Assert.notNull(redisClient, "Cannot create LettuceSentinelConnection using 'null' as client.");
		this.provider = new LettuceConnectionProvider() {
			@Override
			public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> t) {
				return t.cast(redisClient.connectSentinel());
			}
		};
		init();
	}

	/**
	 * Creates a {@link LettuceSentinelConnection} using a supplied redis connection.
	 *
	 * @param connection native Lettuce connection, must not be {@literal null}
	 */
	protected LettuceSentinelConnection(StatefulRedisSentinelConnection<String, String> connection) {

		Assert.notNull(connection, "Cannot create LettuceSentinelConnection using 'null' as connection.");
		this.provider = new LettuceConnectionProvider() {
			@Override
			public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> t) {
				return t.cast(connection);
			}
		};
		init();
	}

	/**
	 * Creates a {@link LettuceSentinelConnection} using a {@link LettuceConnectionProvider}.
	 *
	 * @param connectionProvider must not be {@literal null}.
	 * @since 2.0
	 */
	public LettuceSentinelConnection(LettuceConnectionProvider connectionProvider) {

		Assert.notNull(connectionProvider, "LettuceConnectionProvider must not be null!");
		this.provider = connectionProvider;
		init();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSentinelCommands#failover(org.springframework.data.redis.connection.NamedNode)
	 */
	@Override
	public void failover(NamedNode master) {

		Assert.notNull(master, "Redis node master must not be 'null' for failover.");
		Assert.hasText(master.getName(), "Redis master name must not be 'null' or empty for failover.");
		getSentinelCommands().failover(master.getName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSentinelCommands#masters()
	 */
	@Override
	public List<RedisServer> masters() {
		try {
			return LettuceConverters.toListOfRedisServer(getSentinelCommands().masters());
		} catch (Exception e) {
			throw EXCEPTION_TRANSLATION.translate(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSentinelCommands#slaves(org.springframework.data.redis.connection.NamedNode)
	 */
	@Override
	public List<RedisServer> slaves(NamedNode master) {

		Assert.notNull(master, "Master node cannot be 'null' when loading slaves.");
		return slaves(master.getName());
	}

	/**
	 * @param masterName
	 * @see org.springframework.data.redis.connection.RedisSentinelCommands#slaves(org.springframework.data.redis.connection.NamedNode)
	 * @return
	 */
	public List<RedisServer> slaves(String masterName) {

		Assert.hasText(masterName, "Name of redis master cannot be 'null' or empty when loading slaves.");
		try {
			return LettuceConverters.toListOfRedisServer(getSentinelCommands().slaves(masterName));
		} catch (Exception e) {
			throw EXCEPTION_TRANSLATION.translate(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSentinelCommands#remove(org.springframework.data.redis.connection.NamedNode)
	 */
	@Override
	public void remove(NamedNode master) {

		Assert.notNull(master, "Master node cannot be 'null' when trying to remove.");
		remove(master.getName());
	}

	/**
	 * @param masterName
	 * @see org.springframework.data.redis.connection.RedisSentinelCommands#remove(org.springframework.data.redis.connection.NamedNode)
	 */
	public void remove(String masterName) {

		Assert.hasText(masterName, "Name of redis master cannot be 'null' or empty when trying to remove.");
		getSentinelCommands().remove(masterName);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.RedisSentinelCommands#monitor(org.springframework.data.redis.connection.RedisServer)
	 */
	@Override
	public void monitor(RedisServer server) {

		Assert.notNull(server, "Cannot monitor 'null' server.");
		Assert.hasText(server.getName(), "Name of server to monitor must not be 'null' or empty.");
		Assert.hasText(server.getHost(), "Host must not be 'null' for server to monitor.");
		Assert.notNull(server.getPort(), "Port must not be 'null' for server to monitor.");
		Assert.notNull(server.getQuorum(), "Quorum must not be 'null' for server to monitor.");
		getSentinelCommands().monitor(server.getName(), server.getHost(), server.getPort().intValue(),
				server.getQuorum().intValue());
	}

	/*
	 * (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		provider.release(connection);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void init() {

		if (connection == null) {
			connection = provider.getConnection(StatefulRedisSentinelConnection.class);
		}
	}

	private RedisSentinelCommands<String, String> getSentinelCommands() {
		return connection.sync();
	}

	@Override
	public boolean isOpen() {
		return connection != null && connection.isOpen();
	}

	/**
	 * {@link LettuceConnectionProvider} for a dedicated client instance.
	 */
	private static class DedicatedClientConnectionProvider implements LettuceConnectionProvider {

		private final RedisClient redisClient;

		DedicatedClientConnectionProvider(String host, int port) {

			Assert.notNull(host, "Cannot create LettuceSentinelConnection using 'null' as host.");

			redisClient = RedisClient.create(Builder.redis(host, port).build());
		}

		DedicatedClientConnectionProvider(String host, int port, ClientResources clientResources) {

			Assert.notNull(clientResources, "Cannot create LettuceSentinelConnection using 'null' as ClientResources.");
			Assert.notNull(host, "Cannot create LettuceSentinelConnection using 'null' as host.");

			redisClient = RedisClient.create(clientResources, Builder.redis(host, port).build());
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider#getConnection(java.lang.Class)
		 */
		@Override
		public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType) {
			return connectionType.cast(redisClient.connectSentinel());
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider#release(io.lettuce.core.api.StatefulConnection)
		 */
		@Override
		public void release(StatefulConnection<?, ?> connection) {

			connection.close();
			redisClient.shutdown();
		}
	}
}
