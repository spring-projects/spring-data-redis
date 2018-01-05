/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.redis.repository.configuration;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.convert.ReferenceResolver;
import org.springframework.data.redis.repository.configuration.RedisRepositoryConfigurationUnitTests.ContextWithCustomReferenceResolver;
import org.springframework.data.redis.repository.configuration.RedisRepositoryConfigurationUnitTests.ContextWithoutCustomization;
import org.springframework.data.repository.Repository;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * @author Christoph Strobl
 */
@RunWith(Suite.class)
@SuiteClasses({ ContextWithCustomReferenceResolver.class, ContextWithoutCustomization.class })
public class RedisRepositoryConfigurationUnitTests {

	static RedisTemplate<?, ?> createTemplateMock() {

		RedisTemplate<?, ?> template = mock(RedisTemplate.class);
		RedisConnectionFactory connectionFactory = mock(RedisConnectionFactory.class);
		RedisConnection connection = mock(RedisConnection.class);

		when(template.getConnectionFactory()).thenReturn(connectionFactory);
		when(connectionFactory.getConnection()).thenReturn(connection);

		return template;
	}

	@RunWith(SpringJUnit4ClassRunner.class)
	@DirtiesContext
	@ContextConfiguration(classes = { ContextWithCustomReferenceResolver.Config.class })
	public static class ContextWithCustomReferenceResolver {

		@EnableRedisRepositories(considerNestedRepositories = true,
				includeFilters = { @ComponentScan.Filter(type = FilterType.REGEX, pattern = { ".*ContextSampleRepository" }) })
		static class Config {

			@Bean
			RedisTemplate<?, ?> redisTemplate() {
				return createTemplateMock();
			}

			@Bean
			ReferenceResolver redisReferenceResolver() {
				return mock(ReferenceResolver.class);
			}

		}

		@Autowired ApplicationContext ctx;

		@Test // DATAREDIS-425
		public void shouldPickUpReferenceResolver() {

			RedisKeyValueAdapter adapter = (RedisKeyValueAdapter) ctx.getBean("redisKeyValueAdapter");

			Object referenceResolver = ReflectionTestUtils.getField(adapter.getConverter(), "referenceResolver");

			assertThat(referenceResolver, is(equalTo(ctx.getBean("redisReferenceResolver"))));
			assertThat(mockingDetails(referenceResolver).isMock(), is(true));
		}
	}

	@RunWith(SpringJUnit4ClassRunner.class)
	@DirtiesContext
	@ContextConfiguration(classes = { ContextWithoutCustomization.Config.class })
	public static class ContextWithoutCustomization {

		@EnableRedisRepositories(considerNestedRepositories = true,
				includeFilters = { @ComponentScan.Filter(type = FilterType.REGEX, pattern = { ".*ContextSampleRepository" }) })
		static class Config {

			@Bean
			RedisTemplate<?, ?> redisTemplate() {
				return createTemplateMock();
			}
		}

		@Autowired ApplicationContext ctx;

		@Test // DATAREDIS-425
		public void shouldInitWithDefaults() {
			assertThat(ctx.getBean(ContextSampleRepository.class), is(notNullValue()));
		}

		@Test // DATAREDIS-425
		public void shouldRegisterDefaultBeans() {

			assertThat(ctx.getBean(ContextSampleRepository.class), is(notNullValue()));
			assertThat(ctx.getBean("redisKeyValueAdapter"), is(notNullValue()));
			assertThat(ctx.getBean("redisCustomConversions"), is(notNullValue()));
			assertThat(ctx.getBean("redisReferenceResolver"), is(notNullValue()));
		}
	}

	@RedisHash
	static class Sample {
		String id;
	}

	interface ContextSampleRepository extends Repository<Sample, Long> {}
}
