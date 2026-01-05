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
package org.springframework.data.redis.repository.query;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.ExampleMatcher;
import org.springframework.data.domain.ExampleMatcher.StringMatcher;
import org.springframework.data.redis.core.convert.PathIndexResolver;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.repository.query.RedisOperationChain.PathAndValue;

/**
 * Unit tests for {@link ExampleQueryMapper}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author John Blum
 */
public class ExampleQueryMapperUnitTests {

	private RedisMappingContext mappingContext = new RedisMappingContext();
	private ExampleQueryMapper mapper = new ExampleQueryMapper(mappingContext, new PathIndexResolver(mappingContext));

	@Test // DATAREDIS-605
	void shouldRejectCaseInsensitiveMatching() {

		assertThatThrownBy(() -> {
			mapper.getMappedExample(Example.of(new Person(), ExampleMatcher.matching().withIgnoreCase()));
		}).isInstanceOf(InvalidDataAccessApiUsageException.class);
	}

	@Test // DATAREDIS-605
	void shouldRejectUnsupportedStringMatchers() {

		List<StringMatcher> unsupported = Arrays.asList(StringMatcher.STARTING, StringMatcher.REGEX,
				StringMatcher.CONTAINING, StringMatcher.ENDING);

		for (StringMatcher stringMatcher : unsupported) {

			assertThatThrownBy(() -> {
				mapper.getMappedExample(
						Example.of(new Person(), ExampleMatcher.matching().withStringMatcher(StringMatcher.STARTING)));
			}) //
					.hasMessageContaining("does not support") //
					.describedAs("Unsupported matcher " + stringMatcher) //
					.isInstanceOf(InvalidDataAccessApiUsageException.class);
		}
	}

	@Test // DATAREDIS-605
	void shouldMapSimpleExample() {

		Person person = new Person();
		person.setFirstname("Walter");
		person.setGender(Gender.MALE);
		person.setAge(50);

		RedisOperationChain operationChain = mapper.getMappedExample(Example.of(person));

		assertThat(operationChain.getOrSismember()).isEmpty();
		assertThat(operationChain.getSismember()).contains(new PathAndValue("firstname", "Walter"),
				new PathAndValue("gender", Gender.MALE), new PathAndValue("age", 50));
	}

	@Test // DATAREDIS-605
	void shouldIgnoreFieldsWithoutIndexWithAllMatch() {

		Person person = new Person();
		person.setLastname("Foo");

		RedisOperationChain operationChain = mapper.getMappedExample(Example.of(person));

		assertThat(operationChain.getOrSismember()).isEmpty();
		assertThat(operationChain.getSismember()).isEmpty();
	}

	@Test // DATAREDIS-605
	void shouldIncludeFieldsWithoutIndexWithAnyMatch() {

		Person person = new Person();
		person.setLastname("Foo");

		RedisOperationChain operationChain = mapper.getMappedExample(Example.of(person, ExampleMatcher.matchingAny()));

		assertThat(operationChain.getOrSismember()).containsOnly(new PathAndValue("lastname", "Foo"));
		assertThat(operationChain.getSismember()).isEmpty();
	}

	@Test // DATAREDIS-605
	void shouldIgnorePaths() {

		Person person = new Person();
		person.setFirstname("Walter");
		person.setGender(Gender.MALE);
		person.setAge(50);

		RedisOperationChain operationChain = mapper
				.getMappedExample(Example.of(person, ExampleMatcher.matching().withIgnorePaths("gender", "age")));

		assertThat(operationChain.getOrSismember()).isEmpty();
		assertThat(operationChain.getSismember()).containsOnly(new PathAndValue("firstname", "Walter"));
	}

	@Test // DATAREDIS-605
	void shouldMapNestedExample() {

		Person person = new Person();

		Species species = new Species();
		species.name = "Homo Coquus Caeruleus Methiticus";

		person.setSpecies(species);

		RedisOperationChain operationChain = mapper.getMappedExample(Example.of(person));

		assertThat(operationChain.getOrSismember()).isEmpty();
		assertThat(operationChain.getSismember())
				.containsOnly(new PathAndValue("species.name", "Homo Coquus Caeruleus Methiticus"));
	}

	@Test // DATAREDIS-605
	void shouldIgnoreMapsAndCollections() {

		Person person = new Person();
		person.setNicknames(Arrays.asList("Heisenberg"));
		person.setPhysicalAttributes(Collections.singletonMap("healthy", "no"));

		RedisOperationChain operationChain = mapper.getMappedExample(Example.of(person));

		assertThat(operationChain.getOrSismember()).isEmpty();
		assertThat(operationChain.getSismember()).isEmpty();
	}

	@Test // DATAREDIS-605
	void shouldMapMatchingAny() {

		Person person = new Person();
		person.setFirstname("Walter");
		person.setGender(Gender.MALE);
		person.setAge(50);

		RedisOperationChain operationChain = mapper.getMappedExample(Example.of(person, ExampleMatcher.matchingAny()));

		assertThat(operationChain.getSismember()).isEmpty();
		assertThat(operationChain.getOrSismember()).contains(new PathAndValue("firstname", "Walter"),
				new PathAndValue("gender", Gender.MALE), new PathAndValue("age", 50));
	}

	@Test // DATAREDIS-605
	void shouldApplyPropertyTransformation() {

		Person person = new Person();
		person.setFirstname("Walter");

		Example<Person> example = Example.of(person,
				ExampleMatcher.matching().withTransformer("firstname", v -> v.map(s -> s.toString().toUpperCase())));

		RedisOperationChain operationChain = mapper.getMappedExample(example);

		assertThat(operationChain.getSismember()).contains(new PathAndValue("firstname", "WALTER"));
	}

	static class Person {

		@Id String id;

		@Indexed String firstname;
		String lastname;
		@Indexed Gender gender;

		List<String> nicknames;
		@Indexed Integer age;

		Map<String, String> physicalAttributes;

		@Reference Person relative;

		Species species;

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

		public List<String> getNicknames() {
			return this.nicknames;
		}

		public void setNicknames(List<String> nicknames) {
			this.nicknames = nicknames;
		}

		public Integer getAge() {
			return this.age;
		}

		public void setAge(Integer age) {
			this.age = age;
		}

		public Gender getGender() {
			return this.gender;
		}

		public void setGender(Gender gender) {
			this.gender = gender;
		}

		public Map<String, String> getPhysicalAttributes() {
			return this.physicalAttributes;
		}

		public void setPhysicalAttributes(Map<String, String> physicalAttributes) {
			this.physicalAttributes = physicalAttributes;
		}

		public Person getRelative() {
			return this.relative;
		}

		public void setRelative(Person relative) {
			this.relative = relative;
		}

		public Species getSpecies() {
			return this.species;
		}

		public void setSpecies(Species species) {
			this.species = species;
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
					&& Objects.equals(this.getLastname(), that.getLastname()) && Objects.equals(this.getAge(), that.getAge())
					&& Objects.equals(this.getGender(), that.getGender()) && Objects.equals(this.getSpecies(), that.getSpecies());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getId(), getFirstname(), getLastname(), getAge(), getGender(), getSpecies());
		}
	}

	enum Gender {

		MALE, FEMALE {

			@Override
			public String toString() {
				return "Superwoman";
			}
		}
	}

	static class Species {
		@Indexed String name;
	}
}
