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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConfiguration;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientConfiguration;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.InvalidIsolationLevelException;
import org.springframework.transaction.InvalidTimeoutException;
import org.springframework.transaction.NestedTransactionNotSupportedException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Integration tests for {@link RedisTransactionManager}.
 *
 * @author Moritz Halbritter
 */
@ParameterizedClass
@EnumSource(RedisTransactionManagerIntegrationTests.Client.class)
class RedisTransactionManagerIntegrationTests {

	private static final String KEY = "key";

	private static final String OTHER_KEY = "other-key";

	private static final String VALUE = "value";

	// Rejected by Redis while queueing, so EXEC aborts.
	private static final String UNKNOWN_COMMAND = "NOSUCHCOMMAND";

	@Parameter Client client;

	private final List<DisposableBean> factories = new ArrayList<>();

	private CountingConnectionFactory factory;

	private RedisTransactionManager txManager;

	private TransactionTemplate txTemplate;

	private StringRedisTemplate template;

	private StringRedisTemplate plainTemplate;

	@BeforeEach
	void setUp() {

		factory = new CountingConnectionFactory(register(client.standalone(ResultConversion.CONVERT)));
		txManager = new RedisTransactionManager(factory);
		txTemplate = new TransactionTemplate(txManager);
		template = template(factory, TransactionSupport.ENABLED);
		plainTemplate = template(factory, TransactionSupport.DISABLED);

		plainTemplate.execute((RedisCallback<Void>) connection -> {
			connection.serverCommands().flushDb();
			return null;
		});
	}

	@AfterEach
	void tearDown() throws Exception {

		assertThat(factory.getActiveConnections()).as("active connections").isZero();

		for (DisposableBean bean : factories) {
			bean.destroy();
		}
	}

	@Test // GH-3205
	void commitsWithTransactionTemplate() {

		txTemplate.executeWithoutResult(status -> {
			template.opsForValue().set(KEY, VALUE);
			template.opsForValue().set(OTHER_KEY, VALUE);
		});

		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo(VALUE);
		assertThat(plainTemplate.opsForValue().get(OTHER_KEY)).isEqualTo(VALUE);
	}

	@Test // GH-3205
	void commitsWithTransactional() {

		try (AnnotationConfigApplicationContext context = context()) {
			context.getBean(WriteService.class).write(KEY, VALUE);
		}

		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo(VALUE);
	}

	@Test // GH-3205
	void rollsBackWithTransactionalOnException() {

		try (AnnotationConfigApplicationContext context = context()) {

			WriteService service = context.getBean(WriteService.class);

			assertThatIllegalStateException().isThrownBy(() -> service.writeAndFail(KEY, VALUE));
		}

		assertThat(plainTemplate.hasKey(KEY)).isFalse();
	}

	@Test // GH-3205
	void queuedCommandsReturnNull() {

		Long result = txTemplate.execute(status -> template.opsForValue().increment(KEY));

		assertThat(result).isNull();
		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo("1");
	}

	@Test // GH-3205
	void rollsBackOnException() {

		assertThatIllegalStateException().isThrownBy(() -> txTemplate.executeWithoutResult(status -> {
			template.opsForValue().set(KEY, VALUE);
			throw new IllegalStateException("Boom");
		}));

		assertThat(plainTemplate.hasKey(KEY)).isFalse();
	}

	@Test // GH-3205
	void rollsBackOnSetRollbackOnly() {

		txTemplate.executeWithoutResult(status -> {
			template.opsForValue().set(KEY, VALUE);
			status.setRollbackOnly();
		});

		assertThat(plainTemplate.hasKey(KEY)).isFalse();
	}

	@Test // GH-3205
	void innerRollbackOnlyRollsBackOuter() {

		assertThatExceptionOfType(UnexpectedRollbackException.class)
				.isThrownBy(() -> txTemplate.executeWithoutResult(outer -> {

					template.opsForValue().set(KEY, VALUE);

					txTemplate.executeWithoutResult(inner -> {
						template.opsForValue().set(OTHER_KEY, VALUE);
						inner.setRollbackOnly();
					});
				}));

		assertThat(plainTemplate.hasKey(KEY)).isFalse();
		assertThat(plainTemplate.hasKey(OTHER_KEY)).isFalse();
	}

	@Test // GH-3205
	void readOnlyRunsCommandsImmediately() {

		txTemplate.setReadOnly(true);

		Map<String, Object> result = txTemplate.execute(status -> Map.of( //
				"queueing", template.execute(RedisConnection::isQueueing), //
				"increment", template.opsForValue().increment(KEY), //
				"visible", plainTemplate.opsForValue().get(KEY)));

		assertThat(result).containsEntry("queueing", false).containsEntry("increment", 1L).containsEntry("visible", "1");
	}

	@Test // GH-3205
	void readOnlyRejectsPendingMulti() {

		txTemplate.setReadOnly(true);

		assertThatExceptionOfType(IllegalTransactionStateException.class)
				.isThrownBy(() -> txTemplate.executeWithoutResult(status -> {
					template.multi();
					template.opsForValue().set(KEY, VALUE);
				}));

		assertThat(plainTemplate.hasKey(KEY)).isFalse();
	}

	@Test // GH-3205
	void readOnlyAllowsManualMultiAndExec() {

		// Read-only only skips the managed MULTI; writes aren't rejected. So the template can run its own MULTI/EXEC.
		txTemplate.setReadOnly(true);

		List<Object> results = txTemplate.execute(status -> {
			template.multi();
			template.opsForValue().increment(KEY);
			return template.exec();
		});

		assertThat(results).containsExactly(1L);
		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo("1");
	}

	@Test // GH-3205
	void readsDontSeeQueuedWrites() {

		plainTemplate.opsForValue().set(KEY, "before");

		String read = txTemplate.execute(status -> {
			template.opsForValue().set(KEY, VALUE);
			return template.opsForValue().get(KEY);
		});

		assertThat(read).isEqualTo("before");
		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo(VALUE);
	}

	@Test // GH-3205
	void requiresNewCommitsIndependently() {

		TransactionTemplate requiresNew = new TransactionTemplate(txManager);
		requiresNew.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

		assertThatIllegalStateException().isThrownBy(() -> txTemplate.executeWithoutResult(status -> {

			template.opsForValue().set(KEY, VALUE);
			requiresNew.executeWithoutResult(inner -> template.opsForValue().set(OTHER_KEY, VALUE));

			throw new IllegalStateException("Boom");
		}));

		assertThat(plainTemplate.hasKey(KEY)).isFalse();
		assertThat(plainTemplate.opsForValue().get(OTHER_KEY)).isEqualTo(VALUE);
	}

	@Test // GH-3205
	void notSupportedRunsCommandsImmediately() {

		TransactionTemplate notSupported = new TransactionTemplate(txManager);
		notSupported.setPropagationBehavior(TransactionDefinition.PROPAGATION_NOT_SUPPORTED);

		txTemplate.executeWithoutResult(status -> {

			Long increment = notSupported.execute(inner -> template.opsForValue().increment(OTHER_KEY));
			assertThat(increment).isEqualTo(1L);

			status.setRollbackOnly();
		});

		assertThat(plainTemplate.opsForValue().get(OTHER_KEY)).isEqualTo("1");
	}

	@Test // GH-3205
	void supportsWithoutTransactionRunsCommandsImmediately() {

		txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_SUPPORTS);

		Long increment = txTemplate.execute(status -> template.opsForValue().increment(KEY));

		assertThat(increment).isEqualTo(1L);
	}

	@Test // GH-3205
	void templateWithoutTransactionSupportDoesNotParticipate() {

		txTemplate.executeWithoutResult(status -> {
			plainTemplate.opsForValue().set(KEY, VALUE);
			status.setRollbackOnly();
		});

		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo(VALUE);
	}

	@Test // GH-3205
	void templateOnOtherFactoryFollowsTransactionOutcome() {

		CountingConnectionFactory otherFactory = new CountingConnectionFactory(
				register(client.standalone(ResultConversion.CONVERT)));
		StringRedisTemplate otherTemplate = template(otherFactory, TransactionSupport.ENABLED);

		Long queued = txTemplate.execute(status -> otherTemplate.opsForValue().increment(KEY));

		txTemplate.executeWithoutResult(status -> {
			otherTemplate.opsForValue().set(OTHER_KEY, VALUE);
			status.setRollbackOnly();
		});

		assertThat(queued).isNull();
		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo("1");
		assertThat(plainTemplate.hasKey(OTHER_KEY)).isFalse();
		assertThat(otherFactory.getActiveConnections()).isZero();
	}

	@Test // GH-3205
	void sessionCallbackQueuesOnSharedConnection() {

		Long result = txTemplate.execute(status -> template.execute(new SessionCallback<>() {

			@Override
			public <K, V> Long execute(RedisOperations<K, V> operations) {
				template.opsForValue().set(KEY, VALUE);
				return template.opsForValue().increment(OTHER_KEY);
			}
		}));

		assertThat(result).isNull();
		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo(VALUE);
		assertThat(plainTemplate.opsForValue().get(OTHER_KEY)).isEqualTo("1");
	}

	@Test // GH-3205
	void sessionCallbackWithoutTransactionSupportRunsOwnTransaction() {

		List<Object> results = txTemplate.execute(status -> {

			List<Object> exec = plainTemplate.execute(new SessionCallback<>() {

				@Override
				public <K, V> List<Object> execute(RedisOperations<K, V> operations) {
					plainTemplate.multi();
					plainTemplate.opsForValue().increment(KEY);
					return plainTemplate.exec();
				}
			});

			status.setRollbackOnly();
			return exec;
		});

		assertThat(results).containsExactly(1L);
		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo("1");
	}

	@Test // GH-3205
	void nestedSessionCallbacksKeepOuterConnection() {

		List<Object> results = txTemplate.execute(status -> plainTemplate.execute(new SessionCallback<>() {

			@Override
			public <K, V> List<Object> execute(RedisOperations<K, V> operations) {

				plainTemplate.multi();
				template.execute(new SessionCallback<>() {

					@Override
					public <K2, V2> Object execute(RedisOperations<K2, V2> inner) {
						template.opsForValue().set(OTHER_KEY, VALUE);
						return null;
					}
				});
				plainTemplate.opsForValue().increment(KEY);

				return plainTemplate.exec();
			}
		}));

		assertThat(results).containsExactly(1L);
		assertThat(plainTemplate.opsForValue().get(OTHER_KEY)).isEqualTo(VALUE);
	}

	@Test // GH-3205
	void executePipelinedInsideTransaction() {

		if (client == Client.JEDIS) {

			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> txTemplate.executeWithoutResult(status -> template.executePipelined(
							(RedisCallback<Object>) connection -> {
								connection.stringCommands().set(bytes(KEY), bytes(VALUE));
								return null;
							})))
					.withMessageContaining("Cannot use Pipelining while a transaction is active");
			return;
		}

		List<Object> results = txTemplate.execute(status -> template.executePipelined(
				(RedisCallback<Object>) connection -> {
					connection.stringCommands().set(bytes(KEY), bytes(VALUE));
					return null;
				}));

		assertThat(results).isEmpty();
		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo(VALUE);
	}

	@Test // GH-3205
	void execErrorThrowsAndKeepsAppliedCommands() {

		plainTemplate.opsForValue().set(OTHER_KEY, "not-a-number");

		assertThatExceptionOfType(TransactionSystemException.class)
				.isThrownBy(() -> txTemplate.executeWithoutResult(status -> {
					template.opsForValue().set(KEY, VALUE);
					template.opsForValue().increment(OTHER_KEY);
				}));

		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo(VALUE);
	}

	@Test // GH-3205
	void execErrorThrowsWithUnconvertedResults() {

		CountingConnectionFactory rawFactory = new CountingConnectionFactory(
				register(client.standalone(ResultConversion.RAW)));
		StringRedisTemplate rawTemplate = template(rawFactory, TransactionSupport.ENABLED);
		TransactionTemplate rawTxTemplate = new TransactionTemplate(new RedisTransactionManager(rawFactory));

		plainTemplate.opsForValue().set(OTHER_KEY, "not-a-number");

		assertThatExceptionOfType(TransactionSystemException.class)
				.isThrownBy(() -> rawTxTemplate.executeWithoutResult(status -> {
					rawTemplate.opsForValue().set(KEY, VALUE);
					rawTemplate.opsForValue().increment(OTHER_KEY);
				}));

		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo(VALUE);
		assertThat(rawFactory.getActiveConnections()).isZero();
	}

	@Test // GH-3205
	void queueTimeErrorAbortsTransaction() {

		assertThatExceptionOfType(TransactionSystemException.class)
				.isThrownBy(() -> txTemplate.executeWithoutResult(status -> {
					template.opsForValue().set(KEY, VALUE);
					template.execute((RedisCallback<Object>) connection -> connection.execute(UNKNOWN_COMMAND));
				}));

		assertThat(plainTemplate.hasKey(KEY)).isFalse();
	}

	@Test // GH-3205
	void commitsEmptyTransaction() {
		assertThatNoException().isThrownBy(() -> txTemplate.executeWithoutResult(status -> {}));
	}

	@Test // GH-3205
	void rejectsIsolationLevel() {

		txTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE);

		assertThatExceptionOfType(InvalidIsolationLevelException.class)
				.isThrownBy(() -> txTemplate.executeWithoutResult(status -> {}));
	}

	@Test // GH-3205
	void rejectsTimeout() {

		txTemplate.setTimeout(10);

		assertThatExceptionOfType(InvalidTimeoutException.class)
				.isThrownBy(() -> txTemplate.executeWithoutResult(status -> {}));
	}

	@Test // GH-3205
	void ignoresDefaultTimeout() {

		// E.g. applied to all transaction managers by Spring Boot's spring.transaction.default-timeout.
		txManager.setDefaultTimeout(10);

		txTemplate.executeWithoutResult(status -> template.opsForValue().set(KEY, VALUE));

		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo(VALUE);
	}

	@Test // GH-3205
	void flushTriggersSynchronizations() {

		AtomicBoolean flushed = new AtomicBoolean();

		txTemplate.executeWithoutResult(status -> {

			TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {

				@Override
				public void flush() {
					flushed.set(true);
				}
			});

			status.flush();
		});

		assertThat(flushed).isTrue();
	}

	@Test // GH-3205
	void rejectsReadWriteJoiningReadOnly() {

		TransactionTemplate readOnly = new TransactionTemplate(txManager);
		readOnly.setReadOnly(true);

		assertThatExceptionOfType(IllegalTransactionStateException.class).isThrownBy(() -> readOnly
				.executeWithoutResult(outer -> txTemplate.executeWithoutResult(inner -> template.opsForValue().set(KEY, VALUE))));

		assertThat(plainTemplate.hasKey(KEY)).isFalse();
	}

	@Test // GH-3205
	void readWriteJoinsReadOnlyWithoutValidation() {

		txManager.setValidateExistingTransaction(false);

		TransactionTemplate readOnly = new TransactionTemplate(txManager);
		readOnly.setReadOnly(true);

		// Joins the read-only transaction, so the write runs immediately and survives the rollback.
		Long increment = readOnly.execute(outer -> {

			Long result = txTemplate.execute(inner -> template.opsForValue().increment(KEY));
			outer.setRollbackOnly();

			return result;
		});

		assertThat(increment).isEqualTo(1L);
		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo("1");
	}

	@Test // GH-3205
	void manualExecFailsCommit() {

		assertThatExceptionOfType(TransactionSystemException.class)
				.isThrownBy(() -> txTemplate.executeWithoutResult(status -> {
					template.opsForValue().set(KEY, VALUE);
					template.exec();
					template.opsForValue().set(OTHER_KEY, VALUE);
				}));

		// EXEC ended the MULTI early: queued write applied, later write ran immediately.
		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo(VALUE);
		assertThat(plainTemplate.opsForValue().get(OTHER_KEY)).isEqualTo(VALUE);
	}

	@Test // GH-3205
	void manualDiscardFailsCommit() {

		assertThatExceptionOfType(TransactionSystemException.class)
				.isThrownBy(() -> txTemplate.executeWithoutResult(status -> {
					template.opsForValue().set(KEY, VALUE);
					template.discard();
					template.opsForValue().set(OTHER_KEY, VALUE);
				}));

		// DISCARD ended the MULTI early: queued write dropped, later write ran immediately.
		assertThat(plainTemplate.hasKey(KEY)).isFalse();
		assertThat(plainTemplate.opsForValue().get(OTHER_KEY)).isEqualTo(VALUE);
	}

	@Test // GH-3205
	void manualMultiDoesNothing() {

		Long result = txTemplate.execute(status -> {
			template.multi();
			return template.opsForValue().increment(KEY);
		});

		assertThat(result).isNull();
		assertThat(plainTemplate.opsForValue().get(KEY)).isEqualTo("1");
	}

	@Test // GH-3205
	void rejectsIsolationLevelWhenJoining() {

		TransactionTemplate serializable = new TransactionTemplate(txManager);
		serializable.setIsolationLevel(TransactionDefinition.ISOLATION_SERIALIZABLE);

		assertThatExceptionOfType(IllegalTransactionStateException.class).isThrownBy(
				() -> txTemplate.executeWithoutResult(status -> serializable.executeWithoutResult(inner -> {})));
	}

	@Test // GH-3205
	void rejectsNested() {

		TransactionTemplate nested = new TransactionTemplate(txManager);
		nested.setPropagationBehavior(TransactionDefinition.PROPAGATION_NESTED);

		assertThatExceptionOfType(NestedTransactionNotSupportedException.class).isThrownBy(
				() -> txTemplate.executeWithoutResult(status -> nested.executeWithoutResult(inner -> {})));
	}

	@Test // GH-3205
	@EnabledOnRedisClusterAvailable
	void clusterFailsAtBegin() {

		CountingConnectionFactory clusterFactory = new CountingConnectionFactory(register(client.cluster()));
		TransactionTemplate clusterTxTemplate = new TransactionTemplate(new RedisTransactionManager(clusterFactory));
		AtomicBoolean invoked = new AtomicBoolean();

		assertThatExceptionOfType(CannotCreateTransactionException.class)
				.isThrownBy(() -> clusterTxTemplate.executeWithoutResult(status -> invoked.set(true)));

		clusterTxTemplate.setReadOnly(true);

		assertThatExceptionOfType(CannotCreateTransactionException.class)
				.isThrownBy(() -> clusterTxTemplate.executeWithoutResult(status -> invoked.set(true)));

		assertThat(invoked).isFalse();
		assertThat(clusterFactory.getActiveConnections()).isZero();
	}

	@Test // GH-3205
	void rollbackOnCommitFailureKeepsExecError() {

		txManager.setRollbackOnCommitFailure(true);
		plainTemplate.opsForValue().set(OTHER_KEY, "not-a-number");

		assertThatExceptionOfType(TransactionSystemException.class)
				.isThrownBy(() -> txTemplate.executeWithoutResult(status -> template.opsForValue().increment(OTHER_KEY)))
				.withMessage("Could not commit Redis transaction")
				.withStackTraceContaining("not an integer");
	}

	@Test // GH-3205
	void connectionOfParticipatingTemplateIsTransactional() {

		Map<String, Boolean> result = txTemplate.execute(status -> {

			RedisConnection participating = RedisConnectionUtils.getConnection(factory, true);
			RedisConnection plain = RedisConnectionUtils.getConnection(factory, false);

			try {
				return Map.of( //
						"participating", RedisConnectionUtils.isConnectionTransactional(participating, factory), //
						"plain", RedisConnectionUtils.isConnectionTransactional(plain, factory));
			} finally {
				RedisConnectionUtils.releaseConnection(participating, factory);
				RedisConnectionUtils.releaseConnection(plain, factory);
			}
		});

		assertThat(result).containsEntry("participating", true).containsEntry("plain", false);
	}

	@Test // GH-3205
	void participatingTemplateSeesExistingConnection() {

		List<Boolean> existing = new ArrayList<>();
		RedisTemplate<String, String> recording = new RedisTemplate<>() {

			@Override
			protected RedisConnection preProcessConnection(RedisConnection connection, boolean existingConnection) {
				existing.add(existingConnection);
				return connection;
			}
		};
		recording.setConnectionFactory(factory);
		recording.setEnableTransactionSupport(true);
		recording.afterPropertiesSet();

		txTemplate.executeWithoutResult(status -> recording.execute(RedisConnection::isQueueing));

		assertThat(existing).containsExactly(true);
	}

	// Destroyed after each test.
	private RedisConnectionFactory register(RedisConnectionFactory factory) {

		factories.add((DisposableBean) factory);

		return factory;
	}

	private AnnotationConfigApplicationContext context() {

		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.registerBean(RedisConnectionFactory.class, () -> factory);
		context.register(TransactionalConfig.class);
		context.refresh();

		return context;
	}

	private static StringRedisTemplate template(RedisConnectionFactory factory, TransactionSupport transactionSupport) {

		StringRedisTemplate template = new StringRedisTemplate(factory);
		template.setEnableTransactionSupport(transactionSupport == TransactionSupport.ENABLED);
		template.afterPropertiesSet();

		return template;
	}

	private static byte[] bytes(String value) {
		return value.getBytes(StandardCharsets.UTF_8);
	}

	enum TransactionSupport {
		ENABLED, DISABLED
	}

	enum ResultConversion {
		CONVERT, RAW
	}

	enum Client {

		LETTUCE_SHARED {
			@Override
			RedisConnectionFactory create(RedisConfiguration configuration, ResultConversion conversion) {

				LettuceConnectionFactory factory = lettuce(configuration, conversion);
				factory.setShareNativeConnection(true);

				return start(factory);
			}
		},

		LETTUCE_DEDICATED {
			@Override
			RedisConnectionFactory create(RedisConfiguration configuration, ResultConversion conversion) {

				LettuceConnectionFactory factory = lettuce(configuration, conversion);
				factory.setShareNativeConnection(false);

				return start(factory);
			}
		},

		JEDIS {
			@Override
			RedisConnectionFactory create(RedisConfiguration configuration, ResultConversion conversion) {

				JedisConnectionFactory factory = configuration instanceof RedisClusterConfiguration cluster
						? new JedisConnectionFactory(cluster)
						: new JedisConnectionFactory((RedisStandaloneConfiguration) configuration);
				factory.setConvertPipelineAndTxResults(conversion == ResultConversion.CONVERT);

				return start(factory);
			}
		};

		abstract RedisConnectionFactory create(RedisConfiguration configuration, ResultConversion conversion);

		RedisConnectionFactory standalone(ResultConversion conversion) {
			return create(SettingsUtils.standaloneConfiguration(), conversion);
		}

		RedisConnectionFactory cluster() {
			return create(SettingsUtils.clusterConfiguration(), ResultConversion.CONVERT);
		}

		private static LettuceConnectionFactory lettuce(RedisConfiguration configuration, ResultConversion conversion) {

			LettuceConnectionFactory factory = new LettuceConnectionFactory(configuration,
					LettuceTestClientConfiguration.defaultConfiguration());
			factory.setConvertPipelineAndTxResults(conversion == ResultConversion.CONVERT);

			return factory;
		}

		private static RedisConnectionFactory start(LettuceConnectionFactory factory) {

			factory.afterPropertiesSet();
			factory.start();

			return factory;
		}

		private static RedisConnectionFactory start(JedisConnectionFactory factory) {

			factory.afterPropertiesSet();
			factory.start();

			return factory;
		}

	}

	@Configuration(proxyBeanMethods = false)
	@EnableTransactionManagement
	static class TransactionalConfig {

		@Bean
		RedisTransactionManager transactionManager(RedisConnectionFactory factory) {
			return new RedisTransactionManager(factory);
		}

		@Bean
		WriteService writeService(RedisConnectionFactory factory) {
			return new WriteService(template(factory, TransactionSupport.ENABLED));
		}

	}

	static class WriteService {

		private final StringRedisTemplate template;

		WriteService(StringRedisTemplate template) {
			this.template = template;
		}

		@Transactional
		public void write(String key, String value) {
			template.opsForValue().set(key, value);
		}

		@Transactional
		public void writeAndFail(String key, String value) {
			template.opsForValue().set(key, value);
			throw new IllegalStateException("Boom");
		}

	}

}
