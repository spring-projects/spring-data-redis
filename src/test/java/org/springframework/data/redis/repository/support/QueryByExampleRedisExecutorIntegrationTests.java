/*
 * Copyright 2018-present the original author or authors.
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
package org.springframework.data.redis.repository.support;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.KeyValueTemplate;
import org.springframework.data.keyvalue.repository.support.SimpleKeyValueRepository;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.core.mapping.RedisPersistentEntity;
import org.springframework.data.redis.repository.core.MappingRedisEntityInformation;
import org.springframework.data.redis.test.extension.RedisStandalone;
import org.springframework.data.repository.query.FluentQuery;

/**
 * Integration tests for {@link QueryByExampleRedisExecutor}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author John Blum
 */
class QueryByExampleRedisExecutorIntegrationTests {

	private static JedisConnectionFactory connectionFactory;
	private RedisMappingContext mappingContext = new RedisMappingContext();
	private RedisKeyValueTemplate kvTemplate;

	private Person walt, hank, gus;

	@BeforeAll
	static void beforeAll() {
		connectionFactory = JedisConnectionFactoryExtension.getConnectionFactory(RedisStandalone.class);
	}

	@BeforeEach
	void before() {

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

	@Test // DATAREDIS-605
	void shouldFindOneByExample() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Optional<Person> result = executor.findOne(Example.of(walt));

		assertThat(result).contains(walt);
	}

	@Test // DATAREDIS-605
	void shouldThrowExceptionWhenFindOneByExampleReturnsNonUniqueResult() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		assertThatThrownBy(() -> executor.findOne(Example.of(person)))
				.isInstanceOf(IncorrectResultSizeDataAccessException.class);
	}

	@Test // DATAREDIS-605
	void shouldNotFindOneByExample() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Optional<Person> result = executor.findOne(Example.of(new Person("Skyler", "White")));
		assertThat(result).isEmpty();
	}

	@Test // DATAREDIS-605, GH-2880
	void shouldFindAllByExample() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		List<Person> result = executor.findAll(Example.of(person));
		assertThat(result).contains(walt, gus, hank);
	}

	@Test // DATAREDIS-605
	void shouldNotSupportFindAllOrdered() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		assertThatThrownBy(() -> executor.findAll(Example.of(person), Sort.by("foo")))
				.isInstanceOf(UnsupportedOperationException.class);
	}

	@Test // DATAREDIS-605
	void shouldFindAllPagedByExample() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		Page<Person> result = executor.findAll(Example.of(person), PageRequest.of(0, 2));
		assertThat(result).hasSize(2);
		assertThat(result.getTotalElements()).isEqualTo(3);
	}

	@Test // DATAREDIS-605
	void shouldCountCorrectly() {

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
	void shouldReportExistenceCorrectly() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		assertThat(executor.exists(Example.of(person))).isTrue();
		assertThat(executor.exists(Example.of(walt))).isTrue();
		assertThat(executor.exists(Example.of(new Person()))).isTrue();
		assertThat(executor.exists(Example.of(new Person("Foo", "Bar")))).isFalse();
	}

	@Test // GH-2150
	void findByShouldFindFirst() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		assertThat((Object) executor.findBy(Example.of(person), FluentQuery.FetchableFluentQuery::first)).isNotNull();
		assertThat(executor.findBy(Example.of(walt), it -> it.as(PersonProjection.class).firstValue()).getFirstname())
				.isEqualTo(walt.getFirstname());
	}

	@Test // GH-2150
	void findByShouldFindFirstAsDto() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		assertThat(executor.findBy(Example.of(walt), it -> it.as(PersonDto.class).firstValue()).getFirstname())
				.isEqualTo(walt.getFirstname());
	}

	@Test // GH-2150
	void findByShouldFindOne() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		assertThatExceptionOfType(IncorrectResultSizeDataAccessException.class)
				.isThrownBy(() -> executor.findBy(Example.of(person), FluentQuery.FetchableFluentQuery::one));
		assertThat(executor.findBy(Example.of(walt), it -> it.as(PersonProjection.class).oneValue()).getFirstname())
				.isEqualTo(walt.getFirstname());
	}

	@Test // GH-2150
	void findByShouldFindAll() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		assertThat((List<Person>) executor.findBy(Example.of(person), FluentQuery.FetchableFluentQuery::all)).hasSize(3);
		List<PersonProjection> people = executor.findBy(Example.of(walt), it -> it.as(PersonProjection.class).all());
		assertThat(people).hasOnlyElementsOfType(PersonProjection.class);
	}

	@Test // GH-2150
	void findByShouldFindPage() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		Page<Person> result = executor.findBy(Example.of(person), it -> it.page(PageRequest.of(0, 2)));
		assertThat(result).hasSize(2);
		assertThat(result.getTotalElements()).isEqualTo(3);
	}

	@Test // GH-2150
	void findByShouldFindStream() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		Stream<Person> result = executor.findBy(Example.of(person), FluentQuery.FetchableFluentQuery::stream);
		assertThat(result).hasSize(3);
	}

	@Test // GH-2150
	void findByShouldCount() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		assertThat((Long) executor.findBy(Example.of(person), FluentQuery.FetchableFluentQuery::count)).isEqualTo(3);
	}

	@Test // GH-2150
	void findByShouldExists() {

		QueryByExampleRedisExecutor<Person> executor = new QueryByExampleRedisExecutor<>(getEntityInformation(Person.class),
				kvTemplate);

		Person person = new Person();
		person.setHometown(walt.getHometown());

		assertThat((Boolean) executor.findBy(Example.of(person), FluentQuery.FetchableFluentQuery::exists)).isTrue();
	}

	@SuppressWarnings("unchecked")
	private <T> MappingRedisEntityInformation<T, String> getEntityInformation(Class<T> entityClass) {
		return new MappingRedisEntityInformation<>(
				(RedisPersistentEntity) mappingContext.getRequiredPersistentEntity(entityClass));
	}

	@RedisHash("persons")
	static class Person {

		private @Id String id;
		private @Indexed String firstname;
		private String lastname;
		private City hometown;

		Person() {}

		Person(String firstname, String lastname) {
			this.firstname = firstname;
			this.lastname = lastname;
		}

		public String getId() {
			return this.id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getFirstname() {
			return this.firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public String getLastname() {
			return this.lastname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		public City getHometown() {
			return this.hometown;
		}

		public void setHometown(City hometown) {
			this.hometown = hometown;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof Person that)) {
				return false;
			}

			return Objects.equals(this.getId(), that.getId()) && Objects.equals(this.getFirstname(), that.getFirstname())
					&& Objects.equals(this.getLastname(), that.getLastname())
					&& Objects.equals(this.getHometown(), that.getHometown());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getId(), getFirstname(), getLastname(), getHometown());
		}
	}

	static class City {

		private @Indexed String name;

		public City() {}

		public City(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof City that)) {
				return false;
			}

			return Objects.equals(this.getName(), that.getName());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getName());
		}
	}

	static class PersonDto {

		private String firstname;

		public String getFirstname() {
			return this.firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof Person that)) {
				return false;
			}

			return Objects.equals(this.getFirstname(), that.getLastname());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getFirstname());
		}

		@Override
		public String toString() {
			return getFirstname();
		}
	}

	interface PersonProjection {
		String getFirstname();
	}
}
