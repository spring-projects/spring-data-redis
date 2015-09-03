/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.redis.repository;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.hamcrest.core.IsNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.keyvalue.core.KeyValueTemplate;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.index.RedisIndexDefinition;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Christoph Strobl
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class RedisRepositoryIntegrationTests {

	@Configuration
	@EnableRedisRepositories(considerNestedRepositories = true, indexConfiguration = MyIndexConfiguration.class)
	static class Config {

		@Bean
		RedisTemplate<?, ?> redisTemplate() {

			JedisConnectionFactory connectionFactory = new JedisConnectionFactory();
			connectionFactory.afterPropertiesSet();

			RedisTemplate<byte[], byte[]> template = new RedisTemplate<byte[], byte[]>();
			template.setConnectionFactory(connectionFactory);

			return template;
		}

	}

	@Autowired PersonRepository repo;
	@Autowired KeyValueTemplate kvTemplate;

	@Before
	public void setUp() {

		// flush keyspaces
		kvTemplate.delete(Person.class);
		kvTemplate.delete(City.class);
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void simpleFindSouldReturnEntitiesCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";
		rand.lastname = "al'thor";

		Person egwene = new Person();
		egwene.firstname = "egwene";

		repo.save(Arrays.asList(rand, egwene));

		assertThat(repo.count(), is(2L));

		assertThat(repo.findOne(rand.id), is(rand));
		assertThat(repo.findOne(egwene.id), is(egwene));

		assertThat(repo.findByFirstname("rand").size(), is(1));
		assertThat(repo.findByFirstname("rand"), hasItem(rand));

		assertThat(repo.findByLastname("al'thor"), hasItem(rand));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void simpleFindByMultipleProperties() {

		Person egwene = new Person();
		egwene.firstname = "egwene";
		egwene.lastname = "al'vere";

		Person marin = new Person();
		marin.firstname = "marin";
		marin.lastname = "al'vere";

		repo.save(Arrays.asList(egwene, marin));

		assertThat(repo.findByLastname("al'vere").size(), is(2));

		assertThat(repo.findByFirstnameAndLastname("egwene", "al'vere").size(), is(1));
		assertThat(repo.findByFirstnameAndLastname("egwene", "al'vere").get(0), is(egwene));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void findReturnsReferenceDataCorrectly() {

		// Prepare referenced data entry
		City tarValon = new City();
		tarValon.id = "1";
		tarValon.name = "tar valon";

		kvTemplate.insert(tarValon);

		// Prepare domain entity
		Person moiraine = new Person();
		moiraine.firstname = "moiraine";
		moiraine.city = tarValon; // reference data

		// save domain entity
		repo.save(moiraine);

		// find and assert current location set correctly
		Person loaded = repo.findOne(moiraine.getId());
		assertThat(loaded.city, is(tarValon));

		// remove reference location data
		kvTemplate.delete("1", City.class);

		// find and assert the location is gone
		Person reLoaded = repo.findOne(moiraine.getId());
		assertThat(reLoaded.city, IsNull.nullValue());
	}

	public static interface PersonRepository extends CrudRepository<Person, String> {

		List<Person> findByFirstname(String firstname);

		List<Person> findByLastname(String lastname);

		List<Person> findByFirstnameAndLastname(String firstname, String lastname);
	}

	/**
	 * Custom Redis {@link IndexConfiguration} forcing index of {@link Person#lastname}.
	 * 
	 * @author Christoph Strobl
	 */
	static class MyIndexConfiguration extends IndexConfiguration {

		@Override
		protected Iterable<RedisIndexDefinition> initialConfiguration() {
			return Collections.singleton(new RedisIndexDefinition("persons", "lastname"));
		}
	}

	@KeySpace("persons")
	@SuppressWarnings("serial")
	public static class Person implements Serializable {

		@Id String id;
		@Indexed String firstname;
		String lastname;
		@Reference City city;

		public City getCity() {
			return city;
		}

		public void setCity(City city) {
			this.city = city;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getFirstname() {
			return firstname;
		}

		public void setFirstname(String firstname) {
			this.firstname = firstname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		public String getLastname() {
			return lastname;
		}

		@Override
		public String toString() {
			return "Person [id=" + id + ", firstname=" + firstname + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((firstname == null) ? 0 : firstname.hashCode());
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof Person)) {
				return false;
			}
			Person other = (Person) obj;
			if (firstname == null) {
				if (other.firstname != null) {
					return false;
				}
			} else if (!firstname.equals(other.firstname)) {
				return false;
			}
			if (id == null) {
				if (other.id != null) {
					return false;
				}
			} else if (!id.equals(other.id)) {
				return false;
			}
			return true;
		}

	}

	@KeySpace("cities")
	public static class City {
		@Id String id;
		String name;

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof City)) {
				return false;
			}
			City other = (City) obj;
			if (id == null) {
				if (other.id != null) {
					return false;
				}
			} else if (!id.equals(other.id)) {
				return false;
			}
			if (name == null) {
				if (other.name != null) {
					return false;
				}
			} else if (!name.equals(other.name)) {
				return false;
			}
			return true;
		}
	}

}
