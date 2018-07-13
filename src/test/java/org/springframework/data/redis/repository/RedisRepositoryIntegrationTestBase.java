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
package org.springframework.data.redis.repository;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import lombok.Data;
import lombok.Value;
import lombok.experimental.Wither;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.hamcrest.core.IsNull;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.keyvalue.core.KeyValueTemplate;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.index.GeoIndexed;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.IndexDefinition;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.index.SimpleIndexDefinition;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.QueryByExampleExecutor;

/**
 * Base for testing Redis repository support in different configurations.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public abstract class RedisRepositoryIntegrationTestBase {

	@Autowired PersonRepository repo;
	@Autowired CityRepository cityRepo;
	@Autowired ImmutableObjectRepository immutableObjectRepo;
	@Autowired KeyValueTemplate kvTemplate;

	@Before
	public void setUp() {

		// flush keyspaces
		kvTemplate.delete(Person.class);
		kvTemplate.delete(City.class);
	}

	@Test // DATAREDIS-425
	public void simpleFindShouldReturnEntitiesCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";
		rand.lastname = "al'thor";

		Person egwene = new Person();
		egwene.firstname = "egwene";

		repo.saveAll(Arrays.asList(rand, egwene));

		assertThat(repo.count(), is(2L));

		assertThat(repo.findById(rand.id), is(Optional.of(rand)));
		assertThat(repo.findById(egwene.id), is(Optional.of(egwene)));

		assertThat(repo.findByFirstname("rand").size(), is(1));
		assertThat(repo.findByFirstname("rand"), hasItem(rand));

		assertThat(repo.findByFirstname("egwene").size(), is(1));
		assertThat(repo.findByFirstname("egwene"), hasItem(egwene));

		assertThat(repo.findByLastname("al'thor"), hasItem(rand));
	}

	@Test // DATAREDIS-425
	public void simpleFindByMultipleProperties() {

		Person egwene = new Person();
		egwene.firstname = "egwene";
		egwene.lastname = "al'vere";

		Person marin = new Person();
		marin.firstname = "marin";
		marin.lastname = "al'vere";

		repo.saveAll(Arrays.asList(egwene, marin));

		assertThat(repo.findByLastname("al'vere").size(), is(2));

		assertThat(repo.findByFirstnameAndLastname("egwene", "al'vere").size(), is(1));
		assertThat(repo.findByFirstnameAndLastname("egwene", "al'vere").get(0), is(egwene));
	}

	@Test // DATAREDIS-425
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
		Optional<Person> loaded = repo.findById(moiraine.getId());
		assertThat(loaded.get().city, is(tarValon));

		// remove reference location data
		kvTemplate.delete("1", City.class);

		// find and assert the location is gone
		Optional<Person> reLoaded = repo.findById(moiraine.getId());
		assertThat(reLoaded.get().city, IsNull.nullValue());
	}

	@Test // DATAREDIS-425
	public void findReturnsPageCorrectly() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person sansa = new Person("sansa", "stark");
		Person arya = new Person("arya", "stark");
		Person bran = new Person("bran", "stark");
		Person rickon = new Person("rickon", "stark");

		repo.saveAll(Arrays.asList(eddard, robb, sansa, arya, bran, rickon));

		Page<Person> page1 = repo.findPersonByLastname("stark", PageRequest.of(0, 5));

		assertThat(page1.getNumberOfElements(), is(5));
		assertThat(page1.getTotalElements(), is(6L));

		Page<Person> page2 = repo.findPersonByLastname("stark", page1.nextPageable());

		assertThat(page2.getNumberOfElements(), is(1));
		assertThat(page2.getTotalElements(), is(6L));
	}

	@Test // DATAREDIS-425
	public void findUsingOrReturnsResultCorrectly() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		List<Person> eddardAndJon = repo.findByFirstnameOrLastname("eddard", "snow");

		assertThat(eddardAndJon, hasSize(2));
		assertThat(eddardAndJon, containsInAnyOrder(eddard, jon));
	}

	@Test // DATAREDIS-547
	public void shouldApplyFirstKeywordCorrectly() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		assertThat(repo.findFirstBy(), hasSize(1));
	}

	@Test // DATAREDIS-547
	public void shouldApplyPageableCorrectlyWhenUsingFindAll() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		Page<Person> firstPage = repo.findAll(PageRequest.of(0, 2));
		assertThat(firstPage.getContent(), hasSize(2));
		assertThat(repo.findAll(firstPage.nextPageable()).getContent(), hasSize(1));
	}

	@Test // DATAREDIS-551
	public void shouldApplyPageableCorrectlyWhenUsingFindByWithoutCriteria() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		Page<Person> firstPage = repo.findBy(PageRequest.of(0, 2));
		assertThat(firstPage.getContent(), hasSize(2));
		assertThat(firstPage.getTotalElements(), is(equalTo(3L)));
		assertThat(repo.findBy(firstPage.nextPageable()).getContent(), hasSize(1));
	}

	@Test // DATAREDIS-771
	public void shouldFindByBooleanIsTrue() {

		Person eddard = new Person("eddard", "stark");
		eddard.setAlive(true);

		Person robb = new Person("robb", "stark");
		robb.setAlive(false);

		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		List<Person> result = repo.findPersonByAliveIsTrue();

		assertThat(result, hasSize(1));
		assertThat(result, contains(eddard));
	}

	@Test // DATAREDIS-771
	public void shouldFindByBooleanIsFalse() {

		Person eddard = new Person("eddard", "stark");
		eddard.setAlive(true);

		Person robb = new Person("robb", "stark");
		robb.setAlive(false);

		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		List<Person> result = repo.findPersonByAliveIsFalse();

		assertThat(result, hasSize(1));
		assertThat(result, contains(robb));
	}

	@Test // DATAREDIS-547
	public void shouldReturnEmptyListWhenPageableOutOfBoundsUsingFindAll() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		Page<Person> firstPage = repo.findAll(PageRequest.of(100, 2));
		assertThat(firstPage.getContent(), hasSize(0));
	}

	@Test // DATAREDIS-547
	public void shouldReturnEmptyListWhenPageableOutOfBoundsUsingQueryMethod() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person sansa = new Person("sansa", "stark");

		repo.saveAll(Arrays.asList(eddard, robb, sansa));

		Page<Person> page1 = repo.findPersonByLastname("stark", PageRequest.of(1, 3));

		assertThat(page1.getNumberOfElements(), is(0));
		assertThat(page1.getContent(), hasSize(0));
		assertThat(page1.getTotalElements(), is(3L));

		Page<Person> page2 = repo.findPersonByLastname("stark", PageRequest.of(2, 3));

		assertThat(page2.getNumberOfElements(), is(0));
		assertThat(page2.getContent(), hasSize(0));
		assertThat(page2.getTotalElements(), is(3L));
	}

	@Test // DATAREDIS-547
	public void shouldApplyTopKeywordCorrectly() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		assertThat(repo.findTop2By(), hasSize(2));
	}

	@Test // DATAREDIS-547
	public void shouldApplyTopKeywordCorrectlyWhenCriteriaPresent() {

		Person eddard = new Person("eddard", "stark");
		Person tyrion = new Person("tyrion", "lannister");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");
		Person arya = new Person("arya", "stark");

		repo.saveAll(Arrays.asList(eddard, tyrion, robb, jon, arya));

		List<Person> result = repo.findTop2ByLastname("stark");

		assertThat(result, hasSize(2));
		for (Person p : result) {
			assertThat(p.getLastname(), is("stark"));
		}
	}

	@Test // DATAREDIS-605
	public void shouldFindByExample() {

		Person eddard = new Person("eddard", "stark");
		Person tyrion = new Person("tyrion", "lannister");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");
		Person arya = new Person("arya", "stark");

		repo.saveAll(Arrays.asList(eddard, tyrion, robb, jon, arya));

		List<Person> result = repo.findAll(Example.of(new Person(null, "stark")));

		assertThat(result, hasSize(3));
	}

	@Test // DATAREDIS-533
	public void nearQueryShouldReturnResultsCorrectly() {

		City palermo = new City();
		palermo.location = new Point(13.361389D, 38.115556D);

		City catania = new City();
		catania.location = new Point(15.087269D, 37.502669D);

		cityRepo.saveAll(Arrays.asList(palermo, catania));

		List<City> result = cityRepo.findByLocationNear(new Point(15D, 37D), new Distance(200, Metrics.KILOMETERS));
		assertThat(result, hasItems(palermo, catania));

		result = cityRepo.findByLocationNear(new Point(15D, 37D), new Distance(100, Metrics.KILOMETERS));
		assertThat(result, hasItems(catania));
		assertThat(result, not(hasItems(palermo)));
	}

	@Test // DATAREDIS-533
	public void nearQueryShouldFindNothingIfOutOfRange() {

		City palermo = new City();
		palermo.location = new Point(13.361389D, 38.115556D);

		City catania = new City();
		catania.location = new Point(15.087269D, 37.502669D);

		cityRepo.saveAll(Arrays.asList(palermo, catania));

		List<City> result = cityRepo.findByLocationNear(new Point(15D, 37D), new Distance(10, Metrics.KILOMETERS));
		assertThat(result, is(empty()));
	}

	@Test // DATAREDIS-533
	public void nearQueryShouldReturnResultsCorrectlyOnNestedProperty() {

		City palermo = new City();
		palermo.location = new Point(13.361389D, 38.115556D);

		City catania = new City();
		catania.location = new Point(15.087269D, 37.502669D);

		Person p1 = new Person("foo", "bar");
		p1.hometown = palermo;

		Person p2 = new Person("two", "two");
		p2.hometown = catania;

		repo.saveAll(Arrays.asList(p1, p2));

		List<Person> result = repo.findByHometownLocationNear(new Point(15D, 37D), new Distance(200, Metrics.KILOMETERS));
		assertThat(result, hasItems(p1, p2));

		result = repo.findByHometownLocationNear(new Point(15D, 37D), new Distance(100, Metrics.KILOMETERS));
		assertThat(result, hasItems(p2));
		assertThat(result, not(hasItems(p1)));
	}

	@Test // DATAREDIS-849
	public void shouldReturnNewObjectInstanceOnImmutableSave() {

		Immutable object = new Immutable(null, "Walter", new Immutable("heisenberg", "White", null));
		Immutable saved = immutableObjectRepo.save(object);

		assertThat(object.id, is(nullValue()));
		assertThat(saved.id, is(notNullValue()));
	}

	@Test // DATAREDIS-849
	public void shouldReturnNewObjectInstanceOnImmutableSaveAll() {

		Immutable object = new Immutable(null, "Walter", new Immutable("heisenberg", "White", null));
		List<Immutable> saved = (List) immutableObjectRepo.saveAll(Collections.singleton(object));

		assertThat(object.id, is(nullValue()));
		assertThat(saved.get(0).id, is(notNullValue()));
	}

	@Test // DATAREDIS-849
	public void shouldProperlyReadNestedImmutableObject() {

		Immutable nested = new Immutable("heisenberg", "White", null);
		Immutable object = new Immutable(null, "Walter", nested);
		Immutable saved = immutableObjectRepo.save(object);

		Immutable loaded = immutableObjectRepo.findById(saved.id).get();
		assertThat(loaded.nested, is(nested));
	}

	public static interface PersonRepository
			extends PagingAndSortingRepository<Person, String>, QueryByExampleExecutor<Person> {

		List<Person> findByFirstname(String firstname);

		List<Person> findByLastname(String lastname);

		Page<Person> findPersonByLastname(String lastname, Pageable page);

		List<Person> findPersonByAliveIsTrue();

		List<Person> findPersonByAliveIsFalse();

		List<Person> findByFirstnameAndLastname(String firstname, String lastname);

		List<Person> findByFirstnameOrLastname(String firstname, String lastname);

		List<Person> findFirstBy();

		List<Person> findTop2By();

		List<Person> findTop2ByLastname(String lastname);

		Page<Person> findBy(Pageable page);

		List<Person> findByHometownLocationNear(Point point, Distance distance);

		@Override
		<S extends Person> List<S> findAll(Example<S> example);
	}

	public interface CityRepository extends CrudRepository<City, String> {

		List<City> findByLocationNear(Point point, Distance distance);
	}

	public interface ImmutableObjectRepository extends CrudRepository<Immutable, String> {}

	/**
	 * Custom Redis {@link IndexConfiguration} forcing index of {@link Person#lastname}.
	 *
	 * @author Christoph Strobl
	 */
	static class MyIndexConfiguration extends IndexConfiguration {

		@Override
		protected Iterable<IndexDefinition> initialConfiguration() {
			return Collections.<IndexDefinition> singleton(new SimpleIndexDefinition("persons", "lastname"));
		}
	}

	/**
	 * Custom Redis {@link IndexConfiguration} forcing index of {@link Person#lastname}.
	 *
	 * @author Christoph Strobl
	 */
	static class MyKeyspaceConfiguration extends KeyspaceConfiguration {

		@Override
		protected Iterable<KeyspaceSettings> initialConfiguration() {
			return Collections.singleton(new KeyspaceSettings(City.class, "cities"));
		}
	}

	@RedisHash("persons")
	@Data
	public static class Person {

		@Id String id;
		@Indexed String firstname;
		@Indexed Boolean alive;
		String lastname;
		@Reference City city;
		City hometown;

		public Person() {}

		public Person(String firstname, String lastname) {

			this.firstname = firstname;
			this.lastname = lastname;
		}
	}

	@Data
	public static class City {

		@Id String id;
		String name;

		@GeoIndexed Point location;
	}

	@Value
	@Wither
	public static class Immutable {

		@Id String id;
		String name;

		Immutable nested;
	}
}
