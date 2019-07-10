/*
 * Copyright 2014-2019 the original author or authors.
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
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.instrument.classloading.ShadowingClassLoader;
import org.springframework.lang.Nullable;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Unit tests for {@link RedisTemplate}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisTemplateUnitTests {

	private RedisTemplate<Object, Object> template;
	private @Mock RedisConnectionFactory connectionFactoryMock;
	private @Mock RedisConnection redisConnectionMock;

	@Before
	public void setUp() {

		TransactionSynchronizationManager.clear();

		template = new RedisTemplate<>();
		template.setConnectionFactory(connectionFactoryMock);
		when(connectionFactoryMock.getConnection()).thenReturn(redisConnectionMock);

		template.afterPropertiesSet();
	}

	@Test // DATAREDIS-277
	public void slaveOfIsDelegatedToConnectionCorrectly() {

		template.slaveOf("127.0.0.1", 1001);
		verify(redisConnectionMock, times(1)).slaveOf(eq("127.0.0.1"), eq(1001));
	}

	@Test // DATAREDIS-277
	public void slaveOfNoOneIsDelegatedToConnectionCorrectly() {

		template.slaveOfNoOne();
		verify(redisConnectionMock, times(1)).slaveOfNoOne();
	}

	@Test // DATAREDIS-501
	public void templateShouldPassOnAndUseResoureLoaderClassLoaderToDefaultJdkSerializerWhenNotAlreadySet() {

		ShadowingClassLoader scl = new ShadowingClassLoader(ClassLoader.getSystemClassLoader());

		template = new RedisTemplate<>();
		template.setConnectionFactory(connectionFactoryMock);
		template.setBeanClassLoader(scl);
		template.afterPropertiesSet();

		when(redisConnectionMock.get(any(byte[].class)))
				.thenReturn(new JdkSerializationRedisSerializer().serialize(new SomeArbitrarySerializableObject()));

		Object deserialized = template.opsForValue().get("spring");
		assertThat(deserialized).isNotNull();
		assertThat(deserialized.getClass().getClassLoader()).isEqualTo((ClassLoader) scl);
	}

	@Test // DATAREDIS-531
	public void executeWithStickyConnectionShouldNotCloseConnectionWhenDone() {

		CapturingCallback callback = new CapturingCallback();
		template.executeWithStickyConnection(callback);

		assertThat(callback.getConnection()).isSameAs(redisConnectionMock);
		verify(redisConnectionMock, never()).close();
	}

	@Test // DATAREDIS-988
	public void executeSessionShouldReuseConnection() {

		template.execute(new SessionCallback<Object>() {
			@Nullable
			@Override
			public <K, V> Object execute(RedisOperations<K, V> operations) throws DataAccessException {

				template.multi();
				template.multi();
				return null;
			}
		});

		verify(connectionFactoryMock).getConnection();
		verify(redisConnectionMock).close();
	}

	@Test // DATAREDIS-988
	public void executeSessionInTransactionShouldReuseConnection() {

		TransactionSynchronizationManager.setCurrentTransactionReadOnly(true);

		template.execute(new SessionCallback<Object>() {
			@Override
			public <K, V> Object execute(RedisOperations<K, V> operations) throws DataAccessException {

				template.multi();
				template.multi();
				return null;
			}
		});

		verify(connectionFactoryMock).getConnection();
		verify(redisConnectionMock).close();
	}

	@Test // DATAREDIS-988
	public void transactionAwareTemplateShouldReleaseConnection() {

		template.setEnableTransactionSupport(true);
		TransactionSynchronizationManager.setCurrentTransactionReadOnly(true);

		template.execute(new SessionCallback<Object>() {

			@Override
			public <K, V> Object execute(RedisOperations<K, V> operations) throws DataAccessException {

				template.multi();
				template.multi();
				return null;
			}
		});

		verify(connectionFactoryMock, times(2)).getConnection();
		verify(redisConnectionMock, times(2)).close();
	}

	static class SomeArbitrarySerializableObject implements Serializable {
		private static final long serialVersionUID = -5973659324040506423L;
	}

	static class CapturingCallback implements RedisCallback<Cursor<Object>> {

		private RedisConnection connection;

		@Override
		public Cursor<Object> doInRedis(RedisConnection connection) throws DataAccessException {
			this.connection = connection;
			return null;
		}

		public RedisConnection getConnection() {
			return connection;
		}
	}
}
