/*
 * Copyright 2020-2025 the original author or authors.
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisKeyCommands;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Unit tests for {@link RedisConnectionUtils}.
 *
 * @author Mark Paluch
 */
class RedisConnectionUtilsUnitTests {

	RedisConnection connectionMock1 = mock(RedisConnection.class);
	RedisConnection connectionMock2 = mock(RedisConnection.class);
	RedisConnectionFactory factoryMock = mock(RedisConnectionFactory.class);

	@BeforeEach
	void setUp() {

		// cleanup to avoid lingering resources
		TransactionSynchronizationManager.clear();

		if (TransactionSynchronizationManager.isSynchronizationActive()) {
			TransactionSynchronizationManager.clearSynchronization();
		}

		when(factoryMock.getConnection()).thenReturn(connectionMock1, connectionMock2);
	}

	@Test // DATAREDIS-1104
	void shouldSilentlyCloseRedisConnection() {

		Mockito.reset(connectionMock1);
		doThrow(new IllegalStateException()).when(connectionMock1).close();
		RedisConnectionUtils.releaseConnection(connectionMock1, factoryMock);

		verify(connectionMock1).close();
	}

	@Test // DATAREDIS-891
	void bindConnectionShouldBindConnectionToClosureScope() {

		assertThat(RedisConnectionUtils.bindConnection(factoryMock)).isSameAs(connectionMock1);
		assertThat(TransactionSynchronizationManager.hasResource(factoryMock)).isTrue();

		assertThat(RedisConnectionUtils.getConnection(factoryMock)).isSameAs(connectionMock1);

		RedisConnectionUtils.unbindConnection(factoryMock);
		verifyNoInteractions(connectionMock1);

		RedisConnectionUtils.unbindConnection(factoryMock);
		verify(connectionMock1).close();

		assertThat(TransactionSynchronizationManager.hasResource(factoryMock)).isFalse();
	}

	@Test // DATAREDIS-891
	void getConnectionShouldBindConnectionToTransactionScopeWithReadOnlyTransaction() {

		TransactionTemplate template = new TransactionTemplate(new DummyTransactionManager());
		template.setReadOnly(true);

		template.executeWithoutResult(status -> {

			assertThat(RedisConnectionUtils.getConnection(factoryMock, true)).isSameAs(connectionMock1);
			assertThat(TransactionSynchronizationManager.hasResource(factoryMock)).isTrue();
			assertThat(RedisConnectionUtils.getConnection(factoryMock)).isSameAs(connectionMock1);

			RedisConnectionUtils.releaseConnection(connectionMock1, factoryMock);
			RedisConnectionUtils.releaseConnection(connectionMock1, factoryMock);

			verifyNoInteractions(connectionMock1);
		});

		verify(connectionMock1).close();
		assertThat(TransactionSynchronizationManager.hasResource(factoryMock)).isFalse();
	}

	@Test // DATAREDIS-891, GH-2016
	void bindConnectionShouldBindConnectionToOngoingTransactionScope() {

		TransactionTemplate template = new TransactionTemplate(new DummyTransactionManager());

		template.executeWithoutResult(status -> {

			assertThat(RedisConnectionUtils.bindConnection(factoryMock, true))
					.isInstanceOf(RedisConnectionUtils.RedisConnectionProxy.class);
			assertThat(TransactionSynchronizationManager.hasResource(factoryMock)).isTrue();
			assertThat(RedisConnectionUtils.getConnection(factoryMock)).isNotNull();

			RedisConnectionUtils.releaseConnection(connectionMock1, factoryMock);
			RedisConnectionUtils.releaseConnection(connectionMock1, factoryMock);

			verify(connectionMock1).multi();
			verifyNoMoreInteractions(connectionMock1);
		});

		verify(connectionMock1).close();
		assertThat(TransactionSynchronizationManager.hasResource(factoryMock)).isFalse();
	}

	@Test // DATAREDIS-891
	void bindConnectionShouldNotBindConnectionToTransactionWithoutTransaction() {

		assertThat(RedisConnectionUtils.bindConnection(factoryMock, true)).isNotNull();
		assertThat(TransactionSynchronizationManager.hasResource(factoryMock)).isTrue();
		assertThat(RedisConnectionUtils.getConnection(factoryMock)).isNotNull();

		RedisConnectionUtils.releaseConnection(connectionMock1, factoryMock);
		RedisConnectionUtils.releaseConnection(connectionMock1, factoryMock);

		verify(connectionMock1).close();
		assertThat(TransactionSynchronizationManager.hasResource(factoryMock)).isFalse();
	}

	@Test // DATAREDIS-891
	void getConnectionShouldBindConnectionToTransactionScopeWithReadWriteTransaction() {

		TransactionTemplate template = new TransactionTemplate(new DummyTransactionManager());

		template.executeWithoutResult(status -> {

			assertThat(RedisConnectionUtils.getConnection(factoryMock, true)).isNotNull();
			assertThat(TransactionSynchronizationManager.hasResource(factoryMock)).isTrue();

			RedisConnectionUtils.releaseConnection(connectionMock1, factoryMock);

			verify(connectionMock1).multi();
			verify(factoryMock, times(1)).getConnection();
		});

		verify(connectionMock1).close();
		assertThat(TransactionSynchronizationManager.hasResource(factoryMock)).isFalse();
	}

	@Test // DATAREDIS-891
	void getConnectionShouldNotBindConnectionToTransaction() {

		TransactionTemplate template = new TransactionTemplate(new DummyTransactionManager());

		template.executeWithoutResult(status -> {

			assertThat(RedisConnectionUtils.getConnection(factoryMock)).isSameAs(connectionMock1);
			assertThat(TransactionSynchronizationManager.hasResource(factoryMock)).isFalse();

			RedisConnectionUtils.releaseConnection(connectionMock1, factoryMock);

			verify(factoryMock).getConnection();
			verify(connectionMock1).close();
		});

		verifyNoMoreInteractions(factoryMock);
		verifyNoMoreInteractions(connectionMock1);
	}

	@Test // DATAREDIS-891
	void bindConnectionShouldNotBindConnectionToTransaction() {

		TransactionTemplate template = new TransactionTemplate(new DummyTransactionManager());

		template.executeWithoutResult(status -> {

			assertThat(RedisConnectionUtils.bindConnection(factoryMock)).isSameAs(connectionMock1);
			assertThat(TransactionSynchronizationManager.hasResource(factoryMock)).isTrue();

			RedisConnectionUtils.unbindConnection(factoryMock);

			verify(connectionMock1).close();
			verify(factoryMock, times(1)).getConnection();
		});

		verifyNoMoreInteractions(factoryMock);
		assertThat(TransactionSynchronizationManager.hasResource(factoryMock)).isFalse();
	}

	@Test // GH-2886
	void connectionProxyShouldInvokeReadOnlyMethods() {

		TransactionTemplate template = new TransactionTemplate(new DummyTransactionManager());

		byte[] anyBytes = new byte[] { 1, 2, 3 };
		when(connectionMock2.exists(anyBytes)).thenReturn(true);

		template.executeWithoutResult(status -> {

			RedisConnection connection = RedisConnectionUtils.getConnection(factoryMock, true);

			assertThat(connection.exists(anyBytes)).isEqualTo(true);
		});
	}

	@Test // GH-2886
	void connectionProxyShouldConsiderCommandInterfaces() {

		TransactionTemplate template = new TransactionTemplate(new DummyTransactionManager());

		byte[] anyBytes = new byte[] { 1, 2, 3 };

		RedisKeyCommands commandsMock = mock(RedisKeyCommands.class);

		when(connectionMock1.keyCommands()).thenReturn(commandsMock);
		when(connectionMock2.keyCommands()).thenReturn(commandsMock);
		when(commandsMock.exists(anyBytes)).thenReturn(true);
		when(commandsMock.del(anyBytes)).thenReturn(42L);

		template.executeWithoutResult(status -> {

			RedisConnection connection = RedisConnectionUtils.getConnection(factoryMock, true);

			assertThat(connection.keyCommands().exists(anyBytes)).isEqualTo(true);
			assertThat(connection.keyCommands().del(anyBytes)).isEqualTo(42L);
		});
	}

	static class DummyTransactionManager extends AbstractPlatformTransactionManager {

		@Override
		protected Object doGetTransaction() throws TransactionException {
			return new Object();
		}

		@Override
		protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {

		}

		@Override
		protected void doCommit(DefaultTransactionStatus status) throws TransactionException {

		}

		@Override
		protected void doRollback(DefaultTransactionStatus status) throws TransactionException {

		}
	}
}
