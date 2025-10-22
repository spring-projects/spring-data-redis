/*
 * Copyright 2016-2025 the original author or authors.
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
package org.springframework.data.redis.repository.configuration;

import static org.assertj.core.api.Assertions.*;

import java.util.Collection;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.SimpleBeanDefinitionRegistry;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.StandardAnnotationMetadata;
import org.springframework.data.annotation.Id;
import org.springframework.data.keyvalue.repository.KeyValueRepository;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.RedisKeyValueAdapter.DeletionStrategy;
import org.springframework.data.redis.core.RedisKeyValueAdapter.EnableKeyspaceEvents;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfiguration;
import org.springframework.data.repository.config.RepositoryConfigurationSource;

/**
 * Unit tests for {@link RedisRepositoryConfigurationExtension}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Kim Sumin
 */
class RedisRepositoryConfigurationExtensionUnitTests {

	private AnnotationMetadata metadata = AnnotationMetadata.introspect(Config.class);
	private ResourceLoader loader = new PathMatchingResourcePatternResolver();
	private Environment environment = new StandardEnvironment();
	private BeanDefinitionRegistry registry = new DefaultListableBeanFactory();
	private RepositoryConfigurationSource configurationSource = new AnnotationRepositoryConfigurationSource(metadata,
			EnableRedisRepositories.class, loader, environment, registry, null);

	private RedisRepositoryConfigurationExtension extension = new RedisRepositoryConfigurationExtension();

	@Test // DATAREDIS-425
	void isStrictMatchIfDomainTypeIsAnnotatedWithDocument() {
		assertHasRepo(SampleRepository.class, extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	@Test // DATAREDIS-425
	void isStrictMatchIfRepositoryExtendsStoreSpecificBase() {
		assertHasRepo(StoreRepository.class, extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	@Test // DATAREDIS-425
	void isNotStrictMatchIfDomainTypeIsNotAnnotatedWithDocument() {

		assertDoesNotHaveRepo(UnannotatedRepository.class,
				extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	@Test // DATAREDIS-491
	void picksUpEnableKeyspaceEventsOnStartupCorrectly() {

		metadata = AnnotationMetadata.introspect(Config.class);
		BeanDefinitionRegistry bdr = getBeanDefinitionRegistry();

		assertThat(getEnableKeyspaceEvents(bdr)).isEqualTo((Object) EnableKeyspaceEvents.ON_STARTUP);
	}

	@Test // DATAREDIS-491
	void picksUpEnableKeyspaceEventsDefaultCorrectly() {

		metadata = AnnotationMetadata.introspect(ConfigWithKeyspaceEventsDisabled.class);
		BeanDefinitionRegistry bdr = getBeanDefinitionRegistry();

		assertThat(getEnableKeyspaceEvents(bdr)).isEqualTo((Object) EnableKeyspaceEvents.OFF);
	}

	@Test // DATAREDIS-505
	void picksUpDefaultKeyspaceNotificationsConfigParameterCorrectly() {

		metadata = new StandardAnnotationMetadata(Config.class, true);
		BeanDefinitionRegistry bdr = getBeanDefinitionRegistry();

		assertThat(getKeyspaceNotificationsConfigParameter(bdr)).isEqualTo((Object) "Ex");
	}

	@Test // DATAREDIS-505
	void picksUpCustomKeyspaceNotificationsConfigParameterCorrectly() {

		metadata = new StandardAnnotationMetadata(ConfigWithKeyspaceEventsEnabledAndCustomEventConfig.class, true);
		BeanDefinitionRegistry bdr = getBeanDefinitionRegistry();

		assertThat(getKeyspaceNotificationsConfigParameter(bdr)).isEqualTo((Object) "KEA");
	}

	@Test // DATAREDIS-1049
	void explicitlyEmptyKeyspaceNotificationsConfigParameterShouldBeCapturedCorrectly() {

		metadata = new StandardAnnotationMetadata(ConfigWithEmptyConfigParameter.class, true);
		BeanDefinitionRegistry bdr = getBeanDefinitionRegistry();

		assertThat(getKeyspaceNotificationsConfigParameter(bdr)).isEqualTo("");
	}

	@Test // GH-2294
	void picksUpDeletionStrategyDefaultCorrectly() {
		metadata = new StandardAnnotationMetadata(ConfigWithDefaultDeletionStrategy.class, true);
		BeanDefinitionRegistry beanDefinitionRegistry = getBeanDefinitionRegistry();

		assertThat(getDeletionStrategy(beanDefinitionRegistry)).isEqualTo(DeletionStrategy.DEL);
	}

	@Test // GH-2294
	void picksUpDeletionStrategyUnlinkCorrectly() {
		metadata = new StandardAnnotationMetadata(ConfigWithUnlinkDeletionStrategy.class, true);
		BeanDefinitionRegistry beanDefinitionRegistry = getBeanDefinitionRegistry();

		assertThat(getDeletionStrategy(beanDefinitionRegistry)).isEqualTo(DeletionStrategy.UNLINK);
	}

	private static void assertDoesNotHaveRepo(Class<?> repositoryInterface,
			Collection<RepositoryConfiguration<RepositoryConfigurationSource>> configs) {

		try {

			assertHasRepo(repositoryInterface, configs);
			fail("Expected not to find config for repository interface ".concat(repositoryInterface.getName()));
		} catch (AssertionError error) {
			// repo not there. we're fine.
		}
	}

	private static void assertHasRepo(Class<?> repositoryInterface,
			Collection<RepositoryConfiguration<RepositoryConfigurationSource>> configs) {

		for (RepositoryConfiguration<?> config : configs) {
			if (config.getRepositoryInterface().equals(repositoryInterface.getName())) {
				return;
			}
		}

		fail("Expected to find config for repository interface ".concat(repositoryInterface.getName()).concat(" but got ")
				.concat(configs.toString()));
	}

	private BeanDefinitionRegistry getBeanDefinitionRegistry() {

		BeanDefinitionRegistry registry = new SimpleBeanDefinitionRegistry();

		RepositoryConfigurationSource configurationSource = new AnnotationRepositoryConfigurationSource(metadata,
				EnableRedisRepositories.class, loader, environment, registry, null);

		RedisRepositoryConfigurationExtension extension = new RedisRepositoryConfigurationExtension();

		extension.registerBeansForRoot(registry, configurationSource);

		return registry;
	}

	private Object getEnableKeyspaceEvents(BeanDefinitionRegistry bdr) {
		return bdr.getBeanDefinition("redisKeyValueAdapter").getPropertyValues()
				.getPropertyValue("enableKeyspaceEvents").getValue();
	}

	private Object getKeyspaceNotificationsConfigParameter(BeanDefinitionRegistry bdr) {
		return bdr.getBeanDefinition("redisKeyValueAdapter").getPropertyValues()
				.getPropertyValue("keyspaceNotificationsConfigParameter").getValue();
	}

	private Object getDeletionStrategy(BeanDefinitionRegistry beanDefinitionRegistry) {
		return Objects.requireNonNull(beanDefinitionRegistry.getBeanDefinition("redisKeyValueAdapter").getPropertyValues()
				.getPropertyValue("deletionStrategy").getValue());
	}

	@EnableRedisRepositories(considerNestedRepositories = true, enableKeyspaceEvents = EnableKeyspaceEvents.ON_STARTUP)
	private static class Config {

	}

	@EnableRedisRepositories(considerNestedRepositories = true)
	private static class ConfigWithKeyspaceEventsDisabled {

	}

	@EnableRedisRepositories(considerNestedRepositories = true, enableKeyspaceEvents = EnableKeyspaceEvents.ON_STARTUP,
			keyspaceNotificationsConfigParameter = "KEA")
	private static class ConfigWithKeyspaceEventsEnabledAndCustomEventConfig {

	}

	@EnableRedisRepositories(considerNestedRepositories = true, enableKeyspaceEvents = EnableKeyspaceEvents.ON_STARTUP,
			keyspaceNotificationsConfigParameter = "")
	private static class ConfigWithEmptyConfigParameter {

	}

	@EnableRedisRepositories(considerNestedRepositories = true)
	private static class ConfigWithDefaultDeletionStrategy {}

	@EnableRedisRepositories(considerNestedRepositories = true, deletionStrategy = DeletionStrategy.UNLINK)
	private static class ConfigWithUnlinkDeletionStrategy {}

	@RedisHash
	static class Sample {
		@Id String id;
	}

	interface SampleRepository extends Repository<Sample, Long> {}

	interface UnannotatedRepository extends Repository<Object, Long> {}

	interface StoreRepository extends KeyValueRepository<Object, Long> {}
}
