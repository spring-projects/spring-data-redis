/*
 * Copyright 2022 the original author or authors.
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
package org.springframework.data.redis.aot;

import java.util.Arrays;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.keyvalue.core.AbstractKeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueAdapter;
import org.springframework.data.keyvalue.core.KeyValueOperations;
import org.springframework.data.keyvalue.core.KeyValueTemplate;
import org.springframework.data.keyvalue.core.mapping.context.KeyValueMappingContext;
import org.springframework.data.keyvalue.repository.KeyValueRepository;
import org.springframework.data.keyvalue.repository.config.QueryCreatorType;
import org.springframework.data.keyvalue.repository.query.KeyValuePartTreeQuery;
import org.springframework.data.keyvalue.repository.support.KeyValueRepositoryFactoryBean;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.convert.MappingConfiguration;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.convert.RedisConverter;
import org.springframework.data.redis.core.convert.RedisCustomConversions;
import org.springframework.data.redis.core.convert.ReferenceResolver;
import org.springframework.data.redis.core.convert.ReferenceResolverImpl;
import org.springframework.data.redis.core.index.ConfigurableIndexDefinitionProvider;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.repository.query.RedisQueryCreator;
import org.springframework.data.redis.repository.support.RedisRepositoryFactoryBean;
import org.springframework.lang.Nullable;

/**
 * {@link RuntimeHintsRegistrar} for Redis operations and repository support.
 *
 * @author Christoph Strobl
 * @since 3.0
 */
class RedisRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {

		// CACHE
		hints.serialization().registerType(org.springframework.cache.support.NullValue.class,
				hint -> hint.onReachableType(TypeReference.of(RedisCacheManager.class)));

		// REFLECTION
		hints.reflection().registerTypes(Arrays.asList(TypeReference.of(RedisConnection.class),
				TypeReference.of(StringRedisConnection.class), TypeReference.of(DefaultedRedisConnection.class),
				TypeReference.of(DefaultedRedisClusterConnection.class), TypeReference.of(RedisKeyCommands.class),
				TypeReference.of(RedisStringCommands.class), TypeReference.of(RedisListCommands.class),
				TypeReference.of(RedisSetCommands.class), TypeReference.of(RedisZSetCommands.class),
				TypeReference.of(RedisHashCommands.class), TypeReference.of(RedisTxCommands.class),
				TypeReference.of(RedisPubSubCommands.class), TypeReference.of(RedisConnectionCommands.class),
				TypeReference.of(RedisServerCommands.class), TypeReference.of(RedisStreamCommands.class),
				TypeReference.of(RedisScriptingCommands.class), TypeReference.of(RedisGeoCommands.class),
				TypeReference.of(RedisHyperLogLogCommands.class), TypeReference.of(RedisClusterCommands.class),
				TypeReference.of(ReactiveRedisConnection.class), TypeReference.of(ReactiveKeyCommands.class),
				TypeReference.of(ReactiveStringCommands.class), TypeReference.of(ReactiveListCommands.class),
				TypeReference.of(ReactiveSetCommands.class), TypeReference.of(ReactiveZSetCommands.class),
				TypeReference.of(ReactiveHashCommands.class), TypeReference.of(ReactivePubSubCommands.class),
				TypeReference.of(ReactiveServerCommands.class), TypeReference.of(ReactiveStreamCommands.class),
				TypeReference.of(ReactiveScriptingCommands.class), TypeReference.of(ReactiveGeoCommands.class),
				TypeReference.of(ReactiveHyperLogLogCommands.class), TypeReference.of(ReactiveClusterKeyCommands.class),
				TypeReference.of(ReactiveClusterStringCommands.class), TypeReference.of(ReactiveClusterListCommands.class),
				TypeReference.of(ReactiveClusterSetCommands.class), TypeReference.of(ReactiveClusterZSetCommands.class),
				TypeReference.of(ReactiveClusterHashCommands.class), TypeReference.of(ReactiveClusterServerCommands.class),
				TypeReference.of(ReactiveClusterStreamCommands.class), TypeReference.of(ReactiveClusterScriptingCommands.class),
				TypeReference.of(ReactiveClusterGeoCommands.class), TypeReference.of(ReactiveClusterHyperLogLogCommands.class),
				TypeReference.of(ReactiveRedisOperations.class), TypeReference.of(ReactiveRedisTemplate.class),
				TypeReference.of(RedisOperations.class), TypeReference.of(RedisTemplate.class),
				TypeReference.of(StringRedisTemplate.class), TypeReference.of(KeyspaceConfiguration.class),
				TypeReference.of(MappingConfiguration.class), TypeReference.of(MappingRedisConverter.class),
				TypeReference.of(RedisConverter.class), TypeReference.of(RedisCustomConversions.class),
				TypeReference.of(ReferenceResolver.class), TypeReference.of(ReferenceResolverImpl.class),
				TypeReference.of(IndexConfiguration.class), TypeReference.of(ConfigurableIndexDefinitionProvider.class),
				TypeReference.of(RedisMappingContext.class), TypeReference.of(RedisRepositoryFactoryBean.class),
				TypeReference.of(RedisQueryCreator.class), TypeReference.of(MessageListener.class),
				TypeReference.of(RedisMessageListenerContainer.class),

				TypeReference.of(RedisKeyValueAdapter.class), TypeReference.of(RedisKeyValueTemplate.class),

				// Key-Value
				TypeReference.of(KeySpace.class), TypeReference.of(AbstractKeyValueAdapter.class),
				TypeReference.of(KeyValueAdapter.class), TypeReference.of(KeyValueOperations.class),
				TypeReference.of(KeyValueTemplate.class), TypeReference.of(KeyValueMappingContext.class),
				TypeReference.of(KeyValueRepository.class), TypeReference.of(KeyValueRepositoryFactoryBean.class),
				TypeReference.of(QueryCreatorType.class), TypeReference.of(KeyValuePartTreeQuery.class)),

				hint -> hint.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS, MemberCategory.INVOKE_PUBLIC_METHODS));

		// PROXIES
		hints.proxies().registerJdkProxy(TypeReference.of(RedisConnection.class));
		hints.proxies().registerJdkProxy(TypeReference.of(DefaultedRedisConnection.class));
		hints.proxies().registerJdkProxy(TypeReference.of(ReactiveRedisConnection.class));
		hints.proxies().registerJdkProxy(TypeReference.of(StringRedisConnection.class),
				TypeReference.of(DecoratedRedisConnection.class));

		// keys are bound by a proxy
		boundOperationsProxy(BoundGeoOperations.class, hints);
		boundOperationsProxy(BoundHashOperations.class, hints);
		boundOperationsProxy(BoundKeyOperations.class, hints);
		boundOperationsProxy(BoundListOperations.class, hints);
		boundOperationsProxy(BoundSetOperations.class, hints);
		boundOperationsProxy(BoundStreamOperations.class, hints);
		boundOperationsProxy(BoundValueOperations.class, hints);
		boundOperationsProxy(BoundZSetOperations.class, hints);
		boundOperationsProxy(
				TypeReference.of("org.springframework.data.redis.core.BoundOperationsProxyFactory$DefaultBoundKeyOperations"),
				hints);
	}

	private void boundOperationsProxy(Class<?> type, RuntimeHints hints) {
		boundOperationsProxy(TypeReference.of(type), hints);
	}

	private void boundOperationsProxy(TypeReference typeReference, RuntimeHints hints) {
		hints.proxies().registerJdkProxy(typeReference, //
				TypeReference.of("org.springframework.aop.SpringProxy"), //
				TypeReference.of("org.springframework.aop.framework.Advised"), //
				TypeReference.of("org.springframework.core.DecoratingProxy"));
	}
}
