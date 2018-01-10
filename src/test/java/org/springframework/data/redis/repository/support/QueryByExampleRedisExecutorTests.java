/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.redis.repository.support;

import static org.assertj.core.api.Assertions.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.KeyValueTemplate;
import org.springframework.data.keyvalue.repository.support.SimpleKeyValueRepository;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.core.mapping.RedisPersistentEntity;
import org.springframework.data.redis.repository.core.MappingRedisEntityInformation;

/**
 * Integration tests for {@link QueryByExampleRedisExecutor}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class QueryByExampleRedisExecutorTests {

	JedisConnectionFactory connectionFactory;
	RedisMappingContext mappingContext = new RedisMappingContext();
	RedisKeyValueTemplate kvTemplate;

	Person walt, hank, gus;

	@Before
	public void before() {

		connectionFactory = new JedisConnectionFactory();
		connectionFactory.afterPropertiesSet();

		RedisTemplate<byte[], byte[]> template = new RedisTemplate<>();
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		kvTemplate = new RedisKeyValueTemplate(new RedisKeyValueAdapter(template, mappingContext), mappingContext);

		SimpleKeyValueRepository<Person, String> repository = new SimpleKeyValueRepository<>(
				getEntityInformation(Person.class), new KeyValueTemplate(new RedisKeyValueAdapter(template)));
		repository.deleteAll();

		walt = new Person("Walter", "White");
		walt.setHometown(new City("Albuquerqe"));

		hank = new Person("Hank", "Schrader");
		hank.setHometown(new City("Albuquerqe"));

		gus = new Person("Gus", "Fring");
		gus.setHometown(new City("Albuquerqe"));

		repository.saveAll(Arrays.asList(walt, hank, gus));
	}

	@After
	public void tearDown() {
		connectionFactory.destroy();
	}

	@Test // DATAREDIS-605
	public void shouldFindOneByExample() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Optional<Person> result = executor.findOne(Example.of(walt));

		assertThat(result).contains(walt);
	}

	@Test // DATAREDIS-605
	public void shouldThrowExceptionWhenFindOneByExampleReturnsNonUniqueResult() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		assertThatThrownBy(() -> executor.findOne(Example.of(person)))
				.isInstanceOf(IncorrectResultSizeDataAccessException.class);
	}

	@Test // DATAREDIS-605
	public void shouldNotFindOneByExample() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Optional<Person> result = executor.findOne(Example.of(new Person("Skyler", "White")));
		assertThat(result).isEmpty();
	}

	@Test // DATAREDIS-605
	public void shouldFindAllByExample() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		Iterable<Person> result = executor.findAll(Example.of(person));
		assertThat(result).contains(walt, gus, hank);
	}

	@Test // DATAREDIS-605
	public void shouldNotSupportFindAllOrdered() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		assertThatThrownBy(() -> executor.findAll(Example.of(person), Sort.by("foo")))
				.isInstanceOf(UnsupportedOperationException.class);
	}

	@Test // DATAREDIS-605
	public void shouldFindAllPagedByExample() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		Page<Person> result = executor.findAll(Example.of(person), PageRequest.of(0, 2));
		assertThat(result).hasSize(2);
		assertThat(result.getTotalElements()).isEqualTo(3);
	}

	@Test // DATAREDIS-605
	public void shouldCountCorrectly() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		assertThat(executor.count(Example.of(person))).isEqualTo(3);
		assertThat(executor.count(Example.of(walt))).isEqualTo(1);
		assertThat(executor.count(Example.of(new Person()))).isEqualTo(3);
		assertThat(executor.count(Example.of(new Person("Foo", "Bar")))).isZero();
	}

	@Test // DATAREDIS-605
	public void shouldReportExistenceCorrectly() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		assertThat(executor.exists(Example.of(person))).isTrue();
		assertThat(executor.exists(Example.of(walt))).isTrue();
		assertThat(executor.exists(Example.of(new Person()))).isTrue();
		assertThat(executor.exists(Example.of(new Person("Foo", "Bar")))).isFalse();
	}

	@SuppressWarnings("unchecked")
	private <T> MappingRedisEntityInformation<T, String> getEntityInformation(Class<T> entityClass) {
		return new MappingRedisEntityInformation<>(
				(RedisPersistentEntity) mappingContext.getRequiredPersistentEntity(entityClass));
	}

	@RedisHash("persons")
	@Data
	static class Person {

		@Id String id;
		@Indexed String firstname;
		String lastname;
		City hometown;

		public Person() {}

		public Person(String firstname, String lastname) {

			this.firstname = firstname;
			this.lastname = lastname;
		}
	}

	@Data
	@AllArgsConstructor
	@NoArgsConstructor
	static class City {
		@Indexed String name;
	}
}
