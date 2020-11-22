/*
 * Copyright 2015-2020 the original author or authors.
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
package org.springframework.data.redis.repository;

import static org.assertj.core.api.Assertions.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.convert.ConfigurableTypeInformationMapper;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.convert.DefaultRedisTypeMapper;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.convert.RedisCustomConversions;
import org.springframework.data.redis.core.convert.RedisTypeMapper;
import org.springframework.data.redis.core.convert.ReferenceResolver;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Takahiro Shigemoto
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class RedisRepositoryIntegrationTests extends RedisRepositoryIntegrationTestBase {

	@Configuration
	@EnableRedisRepositories(considerNestedRepositories = true, indexConfiguration = MyIndexConfiguration.class,
			keyspaceConfiguration = MyKeyspaceConfiguration.class,
			includeFilters = { @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE,
					classes = { PersonRepository.class, CityRepository.class, ImmutableObjectRepository.class }) })

	static class Config {

		@Bean
		RedisTemplate<?, ?> redisTemplate() {

			JedisConnectionFactory connectionFactory = new JedisConnectionFactory();
			connectionFactory.afterPropertiesSet();

			RedisTemplate<String, String> template = new RedisTemplate<>();
			template.setDefaultSerializer(StringRedisSerializer.UTF_8);
			template.setConnectionFactory(connectionFactory);

			return template;
		}

		@Bean
		public MappingRedisConverter redisConverter(RedisMappingContext mappingContext,
				RedisCustomConversions customConversions, ReferenceResolver referenceResolver) {

			MappingRedisConverter mappingRedisConverter = new MappingRedisConverter(mappingContext, null, referenceResolver,
					customTypeMapper());

			mappingRedisConverter.setCustomConversions(customConversions);

			return mappingRedisConverter;
		}

		private RedisTypeMapper customTypeMapper() {

			Map<Class<?>, String> mapping = new HashMap<>();

			mapping.put(Person.class, "person");
			mapping.put(City.class, "city");

			ConfigurableTypeInformationMapper mapper = new ConfigurableTypeInformationMapper(mapping);

			return new DefaultRedisTypeMapper(DefaultRedisTypeMapper.DEFAULT_TYPE_KEY, Collections.singletonList(mapper));
		}
	}

	@Autowired RedisOperations<String, String> operations;

	@Test // DATAREDIS-543
	public void shouldConsiderCustomTypeMapper() {

		Person rand = new Person();
		rand.id = "rand";
		rand.firstname = "rand";
		rand.lastname = "al'thor";

		repo.save(rand);

		Map<String, String> entries = operations.<String, String> opsForHash().entries("persons:rand");

		assertThat(entries.get("_class")).isEqualTo("person");
	}

	@Test // DATAREDIS-1253
	public void shouldNotDeleteKeyRegistryPut() throws Exception {
		// Save the record in advance.
		Person rand = new Person();
		rand.id = "rand";
		rand.firstname = "rand";
		rand.lastname = "al'thor";

		repo.save(rand);

		ExecutorService executorService = Executors.newCachedThreadPool();

		// Put thread
		Callable<String> callablePut = () -> {
			try {
				for (int i = 0; i < 100; i++) {
					repo.save(rand);
					Thread.sleep(1);
				}
				return "OK";
			} catch(InterruptedException e) {
				return "ERROR";
			}
		};

		// Get thread
		Callable<String> callableGet = () -> {
			try {
				for (int i = 0; i < 100; i++) {
					Optional<Person> person = repo.findById("rand");
					if (!person.isPresent()) {
						return "EMPTY";
					}
					Thread.sleep(1);
				}
				return "OK";
			} catch(InterruptedException e) {
				return "ERROR";
			}
		};

		// Run threads at the same time.
		Future<String> futurePut = executorService.submit(callablePut);
		Future<String> futureGet = executorService.submit(callableGet);

		assertThat(futurePut.get()).isEqualTo("OK");
		assertThat(futureGet.get()).isEqualTo("OK");

		executorService.shutdown();
	}
}
