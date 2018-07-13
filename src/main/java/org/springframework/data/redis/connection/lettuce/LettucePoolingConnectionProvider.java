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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.support.ConnectionPoolSupport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.PoolException;
import org.springframework.util.Assert;

/**
 * {@link LettuceConnectionProvider} with connection pooling support. This connection provider holds multiple pools (one
 * per connection type) for contextualized connection allocation.
 * <p />
 * Each allocated connection is tracked and to be returned into the pool which created the connection. Instances of this
 * class require {@link #destroy() disposal} to de-allocate lingering connections that were not returned to the pool and
 * to close the pools.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 * @see #getConnection(Class)
 */
class LettucePoolingConnectionProvider implements LettuceConnectionProvider, RedisClientProvider, DisposableBean {

	private final static Log log = LogFactory.getLog(LettucePoolingConnectionProvider.class);

	private final LettuceConnectionProvider connectionProvider;
	private final GenericObjectPoolConfig poolConfig;
	private final Map<StatefulConnection<?, ?>, GenericObjectPool<StatefulConnection<?, ?>>> poolRef = new ConcurrentHashMap<>(
			32);
	private final Map<Class<?>, GenericObjectPool<StatefulConnection<?, ?>>> pools = new ConcurrentHashMap<>(32);

	LettucePoolingConnectionProvider(LettuceConnectionProvider connectionProvider,
			LettucePoolingClientConfiguration clientConfiguration) {

		Assert.notNull(connectionProvider, "ConnectionProvider must not be null!");
		Assert.notNull(clientConfiguration, "ClientConfiguration must not be null!");

		this.connectionProvider = connectionProvider;
		this.poolConfig = clientConfiguration.getPoolConfig();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider#getConnection(java.lang.Class)
	 */
	@Override
	public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType) {

		GenericObjectPool<StatefulConnection<?, ?>> pool = pools.computeIfAbsent(connectionType, poolType -> {
			return ConnectionPoolSupport.createGenericObjectPool(() -> connectionProvider.getConnection(connectionType),
					poolConfig, false);
		});

		try {

			StatefulConnection<?, ?> connection = pool.borrowObject();

			poolRef.put(connection, pool);

			return connectionType.cast(connection);
		} catch (Exception e) {
			throw new PoolException("Could not get a resource from the pool", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.RedisClientProvider#getRedisClient()
	 */
	@Override
	public AbstractRedisClient getRedisClient() {

		if (connectionProvider instanceof RedisClientProvider) {
			return ((RedisClientProvider) connectionProvider).getRedisClient();
		}

		throw new IllegalStateException(
				String.format("Underlying connection provider %s does not implement RedisClientProvider!",
						connectionProvider.getClass().getName()));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.lettuce.LettuceConnectionProvider#release(io.lettuce.core.api.StatefulConnection)
	 */
	@Override
	public void release(StatefulConnection<?, ?> connection) {

		GenericObjectPool<StatefulConnection<?, ?>> pool = poolRef.remove(connection);

		if (pool == null) {
			throw new PoolException("Returned connection " + connection
					+ " was either previously returned or does not belong to this connection provider");
		}

		pool.returnObject(connection);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() throws Exception {

		if (!poolRef.isEmpty()) {

			log.warn("LettucePoolingConnectionProvider contains unreleased connections");

			poolRef.forEach((connection, pool) -> pool.returnObject(connection));
			poolRef.clear();
		}

		pools.forEach((type, pool) -> pool.close());
		pools.clear();
	}
}
