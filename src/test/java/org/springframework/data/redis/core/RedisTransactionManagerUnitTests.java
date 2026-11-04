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
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.transaction.InvalidIsolationLevelException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Unit tests for {@link RedisTransactionManager}.
 *
 * @author Alpha
 */
class RedisTransactionManagerUnitTests {

	RedisConnection connection = mock(RedisConnection.class);

	RedisConnectionFactory connectionFactory = mock(RedisConnectionFactory.class);

	@BeforeEach
	void setUp() {
		TransactionSynchronizationManager.clear();
		when(this.connectionFactory.getConnection()).thenReturn(this.connection);
	}

	@AfterEach
	void tearDown() {
		TransactionSynchronizationManager.clear();
	}

	@Test // GH-3205
	void shouldCommitRedisTransaction() {

		TransactionTemplate transactionTemplate = new TransactionTemplate(new RedisTransactionManager(this.connectionFactory));

		transactionTemplate.executeWithoutResult(status -> {

			RedisConnection connection = RedisConnectionUtils.getConnection(this.connectionFactory);
			assertThat(RedisConnectionUtils.isConnectionTransactional(connection, this.connectionFactory)).isTrue();
			RedisConnectionUtils.releaseConnection(connection, this.connectionFactory);
		});

		verify(this.connection).multi();
		verify(this.connection).exec();
		verify(this.connection, never()).discard();
		verify(this.connection).close();
		verify(this.connectionFactory).getConnection();
	}

	@Test // GH-3205
	void shouldDiscardRedisTransactionOnRollback() {

		TransactionTemplate transactionTemplate = new TransactionTemplate(new RedisTransactionManager(this.connectionFactory));

		transactionTemplate.executeWithoutResult(status -> {
			status.setRollbackOnly();
		});

		verify(this.connection).multi();
		verify(this.connection).discard();
		verify(this.connection, never()).exec();
		verify(this.connection).close();
	}

	@Test // GH-3205
	void shouldNotStartRedisTransactionForReadOnlyDefinition() {

		TransactionTemplate transactionTemplate = new TransactionTemplate(new RedisTransactionManager(this.connectionFactory));
		transactionTemplate.setReadOnly(true);

		transactionTemplate.executeWithoutResult(status -> {
			RedisConnection connection = RedisConnectionUtils.getConnection(this.connectionFactory);
			RedisConnectionUtils.releaseConnection(connection, this.connectionFactory);
		});

		verify(this.connection, never()).multi();
		verify(this.connection, never()).exec();
		verify(this.connection, never()).discard();
		verify(this.connection).close();
	}

	@Test // GH-3205
	void shouldRejectCustomIsolationLevel() {

		TransactionTemplate transactionTemplate = new TransactionTemplate(new RedisTransactionManager(this.connectionFactory));
		transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE);

		assertThatExceptionOfType(InvalidIsolationLevelException.class)
				.isThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {}));

		verifyNoInteractions(this.connectionFactory);
	}

	@Test // GH-3205
	void shouldRollBackOuterTransactionWhenParticipatingTransactionIsMarkedRollbackOnly() {

		RedisTransactionManager transactionManager = new RedisTransactionManager(this.connectionFactory);
		TransactionTemplate outer = new TransactionTemplate(transactionManager);
		TransactionTemplate inner = new TransactionTemplate(transactionManager);

		assertThatExceptionOfType(UnexpectedRollbackException.class).isThrownBy(() -> outer.executeWithoutResult(status ->
				inner.executeWithoutResult(innerStatus -> innerStatus.setRollbackOnly())));

		verify(this.connection).multi();
		verify(this.connection).discard();
		verify(this.connection, never()).exec();
		verify(this.connection).close();
	}

	@Test // GH-3205
	void shouldSuspendAndResumeConnectionForRequiresNewTransaction() {

		RedisConnection innerConnection = mock(RedisConnection.class);
		when(this.connectionFactory.getConnection()).thenReturn(this.connection, innerConnection);

		RedisTransactionManager transactionManager = new RedisTransactionManager(this.connectionFactory);
		TransactionTemplate outer = new TransactionTemplate(transactionManager);
		TransactionTemplate inner = new TransactionTemplate(transactionManager);
		inner.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

		outer.executeWithoutResult(status -> inner.executeWithoutResult(innerStatus -> {
			RedisConnection connection = RedisConnectionUtils.getConnection(this.connectionFactory);
			RedisConnectionUtils.releaseConnection(connection, this.connectionFactory);
		}));

		verify(this.connection).multi();
		verify(this.connection).exec();
		verify(this.connection).close();
		verify(innerConnection).multi();
		verify(innerConnection).exec();
		verify(innerConnection).close();
		verify(this.connectionFactory, times(2)).getConnection();
	}

}
