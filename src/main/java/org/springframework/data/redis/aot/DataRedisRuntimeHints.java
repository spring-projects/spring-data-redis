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
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 * @since 3.0
 */
public class DataRedisRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {

		// REFLECTION
		hints.reflection()
				.registerTypes(
						Arrays.asList(TypeReference.of(org.springframework.data.redis.connection.RedisConnection.class),
								TypeReference.of(org.springframework.data.redis.connection.StringRedisConnection.class),
								TypeReference.of(org.springframework.data.redis.connection.DefaultedRedisConnection.class),
								TypeReference.of(org.springframework.data.redis.connection.DefaultedRedisClusterConnection.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisKeyCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisStringCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisListCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisSetCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisZSetCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisHashCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisTxCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisPubSubCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisConnectionCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisServerCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisStreamCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisScriptingCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisGeoCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisHyperLogLogCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.RedisClusterCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveRedisConnection.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveKeyCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveStringCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveListCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveSetCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveZSetCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveHashCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactivePubSubCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveServerCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveStreamCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveScriptingCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveGeoCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveHyperLogLogCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveClusterKeyCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveClusterStringCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveClusterListCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveClusterSetCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveClusterZSetCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveClusterHashCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveClusterServerCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveClusterStreamCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveClusterScriptingCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveClusterGeoCommands.class),
								TypeReference.of(org.springframework.data.redis.connection.ReactiveClusterHyperLogLogCommands.class),
								TypeReference.of(org.springframework.data.redis.core.ReactiveRedisOperations.class),
								TypeReference.of(org.springframework.data.redis.core.ReactiveRedisTemplate.class),
								TypeReference.of(org.springframework.data.redis.core.RedisOperations.class),
								TypeReference.of(org.springframework.data.redis.core.RedisTemplate.class),
								TypeReference.of(org.springframework.data.redis.core.StringRedisTemplate.class),
								TypeReference.of(org.springframework.data.keyvalue.annotation.KeySpace.class),
								TypeReference.of(org.springframework.data.keyvalue.core.AbstractKeyValueAdapter.class),
								TypeReference.of(org.springframework.data.keyvalue.core.KeyValueAdapter.class),
								TypeReference.of(org.springframework.data.keyvalue.core.KeyValueOperations.class),
								TypeReference.of(org.springframework.data.keyvalue.core.KeyValueTemplate.class),
								TypeReference.of(org.springframework.data.keyvalue.core.mapping.context.KeyValueMappingContext.class),
								TypeReference.of(org.springframework.data.keyvalue.repository.KeyValueRepository.class),
								TypeReference
										.of(org.springframework.data.keyvalue.repository.support.KeyValueRepositoryFactoryBean.class),
								TypeReference.of(org.springframework.data.keyvalue.repository.config.QueryCreatorType.class),
								TypeReference.of(org.springframework.data.keyvalue.repository.query.KeyValuePartTreeQuery.class),
								TypeReference.of(org.springframework.data.redis.core.RedisKeyValueAdapter.class),
								TypeReference.of(org.springframework.data.redis.core.RedisKeyValueTemplate.class),
								TypeReference.of(org.springframework.data.redis.core.convert.KeyspaceConfiguration.class),
								TypeReference.of(org.springframework.data.redis.core.convert.MappingConfiguration.class),
								TypeReference.of(org.springframework.data.redis.core.convert.MappingRedisConverter.class),
								TypeReference.of(org.springframework.data.redis.core.convert.RedisConverter.class),
								TypeReference.of(org.springframework.data.redis.core.convert.RedisCustomConversions.class),
								TypeReference.of(org.springframework.data.redis.core.convert.ReferenceResolver.class),
								TypeReference.of(org.springframework.data.redis.core.convert.ReferenceResolverImpl.class),
								TypeReference.of(org.springframework.data.redis.core.index.IndexConfiguration.class),
								TypeReference.of(org.springframework.data.redis.core.index.ConfigurableIndexDefinitionProvider.class),
								TypeReference.of(org.springframework.data.redis.core.mapping.RedisMappingContext.class),
								TypeReference.of(org.springframework.data.redis.repository.support.RedisRepositoryFactoryBean.class),
								TypeReference.of(org.springframework.data.redis.repository.query.RedisQueryCreator.class)),
						hint -> hint.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
								MemberCategory.INVOKE_PUBLIC_METHODS));

		// PROXIES
		hints.proxies().registerJdkProxy(TypeReference.of(org.springframework.data.redis.connection.RedisConnection.class));
		hints.proxies()
				.registerJdkProxy(TypeReference.of(org.springframework.data.redis.connection.DefaultedRedisConnection.class));
		hints.proxies()
				.registerJdkProxy(TypeReference.of(org.springframework.data.redis.connection.ReactiveRedisConnection.class));
		hints.proxies().registerJdkProxy(
				TypeReference.of(org.springframework.data.redis.connection.StringRedisConnection.class),
				TypeReference.of(org.springframework.data.redis.connection.DecoratedRedisConnection.class));

	}
}
