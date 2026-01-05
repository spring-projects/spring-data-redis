/*
 * Copyright 2016-present the original author or authors.
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

import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
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
import org.springframework.data.util.Streamable;
import org.springframework.lang.Nullable;

/**
 * Base for testing Redis repository support in different configurations.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 */
public abstract class RedisRepositoryIntegrationTestBase {

	@Autowired PersonRepository repo;
	@Autowired CityRepository cityRepo;
	@Autowired ImmutableObjectRepository immutableObjectRepo;
	@Autowired UserRepository userRepository;
	@Autowired KeyValueTemplate kvTemplate;

	@BeforeEach
	void setUp() {

		// flush keyspaces
		kvTemplate.delete(Person.class);
		kvTemplate.delete(City.class);
	}

	@Test // DATAREDIS-425
	void simpleFindShouldReturnEntitiesCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";
		rand.lastname = "al'thor";

		Person egwene = new Person();
		egwene.firstname = "egwene";

		repo.saveAll(Arrays.asList(rand, egwene));

		assertThat(repo.count()).isEqualTo(2L);

		assertThat(repo.findById(rand.id)).isEqualTo(Optional.of(rand));
		assertThat(repo.findById(egwene.id)).isEqualTo(Optional.of(egwene));

		assertThat(repo.findByFirstname("rand").size()).isEqualTo(1);
		assertThat(repo.findByFirstname("rand")).contains(rand);

		assertThat(repo.findByFirstname("egwene").size()).isEqualTo(1);
		assertThat(repo.findByFirstname("egwene")).contains(egwene);

		assertThat(repo.findByLastname("al'thor")).contains(rand);
	}

	@Test // GH-2851
	void shouldReturnSingleEntityByIdViaQueryMethod() {

		Person rand = new Person();
		rand.firstname = "rand";
		rand.lastname = "al'thor";

		Person egwene = new Person();
		egwene.firstname = "egwene";

		repo.saveAll(Arrays.asList(rand, egwene));

		assertThat(repo.findEntityById(rand.getId())).isEqualTo(rand);
		assertThat(repo.findEntityById(egwene.getId())).isEqualTo(egwene);
	}

	@Test // GH-2851
	void shouldProjectSingleResult() {

		Person rand = new Person();
		rand.firstname = "rand";
		rand.lastname = "al'thor";

		Person egwene = new Person();
		egwene.firstname = "egwene";

		repo.saveAll(Arrays.asList(rand, egwene));

		PersonProjection projection = repo.findProjectionById(rand.getId(), PersonProjection.class);
		assertThat(projection).isNotNull();
		assertThat(projection.getFirstname()).isEqualTo(rand.firstname);

		PersonDto dto = repo.findProjectionById(rand.getId(), PersonDto.class);
		assertThat(dto).isNotNull();
		assertThat(dto.firstname()).isEqualTo(rand.firstname);
	}

	@Test // GH-2851
	void shouldProjectCollection() {

		Person rand = new Person();
		rand.firstname = "rand";
		rand.lastname = "al'thor";

		Person egwene = new Person();
		egwene.firstname = "egwene";

		repo.saveAll(Arrays.asList(rand, egwene));

		List<PersonProjection> projection = repo.findProjectionBy();
		assertThat(projection).hasSize(2) //
				.extracting(PersonProjection::getFirstname) //
				.contains(rand.getFirstname(), egwene.getFirstname());

		projection = repo.findProjectionStreamBy().toList();
		assertThat(projection).hasSize(2) //
				.extracting(PersonProjection::getFirstname) //
				.contains(rand.getFirstname(), egwene.getFirstname());

		List<PersonDto> dtos = repo.findProjectionDtoBy();
		assertThat(dtos).hasSize(2) //
				.extracting(PersonDto::firstname) //
				.contains(rand.getFirstname(), egwene.getFirstname());
	}

	@Test // DATAREDIS-425
	void simpleFindByMultipleProperties() {

		Person egwene = new Person();
		egwene.firstname = "egwene";
		egwene.lastname = "al'vere";

		Person marin = new Person();
		marin.firstname = "marin";
		marin.lastname = "al'vere";

		repo.saveAll(Arrays.asList(egwene, marin));

		assertThat(repo.findByLastname("al'vere").size()).isEqualTo(2);

		assertThat(repo.findByFirstnameAndLastname("egwene", "al'vere").size()).isEqualTo(1);
		assertThat(repo.findByFirstnameAndLastname("egwene", "al'vere").get(0)).isEqualTo(egwene);
	}

	@Test // GH-2080
	void simpleFindAndSort() {

		Person egwene = new Person();
		egwene.firstname = "egwene";
		egwene.lastname = "al'vere";

		Person marin = new Person();
		marin.firstname = "marin";
		marin.lastname = "al'vere";

		repo.saveAll(Arrays.asList(egwene, marin));

		assertThat(repo.findByLastname("al'vere", Sort.by(Sort.Direction.ASC, "firstname"))).containsSequence(egwene,
				marin);
		assertThat(repo.findByLastname("al'vere", Sort.by(Sort.Direction.DESC, "firstname"))).containsSequence(marin,
				egwene);

		assertThat(repo.findByLastnameOrderByFirstnameAsc("al'vere")).containsSequence(egwene, marin);
		assertThat(repo.findByLastnameOrderByFirstnameDesc("al'vere")).containsSequence(marin, egwene);
	}

	@Test // GH-2080
	void simpleFindAllWithSort() {

		Person egwene = new Person();
		egwene.firstname = "egwene";
		egwene.lastname = "al'vere";

		Person marin = new Person();
		marin.firstname = "marin";
		marin.lastname = "al'vere";

		repo.saveAll(Arrays.asList(egwene, marin));

		assertThat(repo.findAll(Sort.by(Sort.Direction.ASC, "firstname"))).containsSequence(egwene, marin);
		assertThat(repo.findAll(Sort.by(Sort.Direction.DESC, "firstname"))).containsSequence(marin, egwene);
	}

	@Test // DATAREDIS-425
	void findReturnsReferenceDataCorrectly() {

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
		assertThat(loaded.get().city).isEqualTo(tarValon);

		// remove reference location data
		kvTemplate.delete("1", City.class);

		// find and assert the location is gone
		Optional<Person> reLoaded = repo.findById(moiraine.getId());
		assertThat(reLoaded.get().city).isNull();
	}

	@Test // DATAREDIS-425
	void findReturnsPageCorrectly() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person sansa = new Person("sansa", "stark");
		Person arya = new Person("arya", "stark");
		Person bran = new Person("bran", "stark");
		Person rickon = new Person("rickon", "stark");

		repo.saveAll(Arrays.asList(eddard, robb, sansa, arya, bran, rickon));

		Page<Person> page1 = repo.findPersonByLastname("stark", PageRequest.of(0, 5));

		assertThat(page1.getNumberOfElements()).isEqualTo(5);
		assertThat(page1.getTotalElements()).isEqualTo(6L);

		Page<Person> page2 = repo.findPersonByLastname("stark", page1.nextPageable());

		assertThat(page2.getNumberOfElements()).isEqualTo(1);
		assertThat(page2.getTotalElements()).isEqualTo(6L);
	}

	@Test // DATAREDIS-425
	void findUsingOrReturnsResultCorrectly() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		List<Person> eddardAndJon = repo.findByFirstnameOrLastname("eddard", "snow");

		assertThat(eddardAndJon).hasSize(2);
		assertThat(eddardAndJon).contains(eddard, jon);
	}

	@Test // DATAREDIS-547
	void shouldApplyFirstKeywordCorrectly() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		assertThat(repo.findFirstBy()).hasSize(1);
	}

	@Test // DATAREDIS-547
	void shouldApplyPageableCorrectlyWhenUsingFindAll() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		Page<Person> firstPage = repo.findAll(PageRequest.of(0, 2));
		assertThat(firstPage.getContent()).hasSize(2);
		assertThat(repo.findAll(firstPage.nextPageable()).getContent()).hasSize(1);
	}

	@Test // DATAREDIS-551
	void shouldApplyPageableCorrectlyWhenUsingFindByWithoutCriteria() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		Page<Person> firstPage = repo.findBy(PageRequest.of(0, 2));
		assertThat(firstPage.getContent()).hasSize(2);
		assertThat(firstPage.getTotalElements()).isEqualTo(3L);
		assertThat(repo.findBy(firstPage.nextPageable()).getContent()).hasSize(1);
	}

	@Test // GH-2799
	void shouldReturnEntitiesWithoutDuplicates() {

		Person eddard = new Person("eddard", "stark");
		eddard.setAlive(true);

		Person robb = new Person("robb", "stark");
		robb.setAlive(false);

		Person jon = new Person("jon", "snow");
		jon.setAlive(true);

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		List<Person> result = repo.findByFirstnameAndLastnameOrAliveIsTrue("eddard", "stark");
		assertThat(result).hasSize(2);
		assertThat(result).contains(eddard, jon);
	}

	@Test // DATAREDIS-771
	void shouldFindByBooleanIsTrue() {

		Person eddard = new Person("eddard", "stark");
		eddard.setAlive(true);

		Person robb = new Person("robb", "stark");
		robb.setAlive(false);

		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		List<Person> result = repo.findPersonByAliveIsTrue();

		assertThat(result).hasSize(1);
		assertThat(result).containsExactly(eddard);
	}

	@Test // DATAREDIS-771
	void shouldFindByBooleanIsFalse() {

		Person eddard = new Person("eddard", "stark");
		eddard.setAlive(true);

		Person robb = new Person("robb", "stark");
		robb.setAlive(false);

		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		List<Person> result = repo.findPersonByAliveIsFalse();

		assertThat(result).hasSize(1);
		assertThat(result).containsExactly(robb);
	}

	@Test // DATAREDIS-547
	void shouldReturnEmptyListWhenPageableOutOfBoundsUsingFindAll() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		Page<Person> firstPage = repo.findAll(PageRequest.of(100, 2));
		assertThat(firstPage.getContent()).hasSize(0);
	}

	@Test // DATAREDIS-547
	void shouldReturnEmptyListWhenPageableOutOfBoundsUsingQueryMethod() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person sansa = new Person("sansa", "stark");

		repo.saveAll(Arrays.asList(eddard, robb, sansa));

		Page<Person> page1 = repo.findPersonByLastname("stark", PageRequest.of(1, 3));

		assertThat(page1.getNumberOfElements()).isEqualTo(0);
		assertThat(page1.getContent()).hasSize(0);
		assertThat(page1.getTotalElements()).isEqualTo(3L);

		Page<Person> page2 = repo.findPersonByLastname("stark", PageRequest.of(2, 3));

		assertThat(page2.getNumberOfElements()).isEqualTo(0);
		assertThat(page2.getContent()).hasSize(0);
		assertThat(page2.getTotalElements()).isEqualTo(3L);
	}

	@Test // DATAREDIS-547
	void shouldApplyTopKeywordCorrectly() {

		Person eddard = new Person("eddard", "stark");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");

		repo.saveAll(Arrays.asList(eddard, robb, jon));

		assertThat(repo.findTop2By()).hasSize(2);
	}

	@Test // DATAREDIS-547
	void shouldApplyTopKeywordCorrectlyWhenCriteriaPresent() {

		Person eddard = new Person("eddard", "stark");
		Person tyrion = new Person("tyrion", "lannister");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");
		Person arya = new Person("arya", "stark");

		repo.saveAll(Arrays.asList(eddard, tyrion, robb, jon, arya));

		List<Person> result = repo.findTop2ByLastname("stark");

		assertThat(result).hasSize(2);
		for (Person p : result) {
			assertThat(p.getLastname()).isEqualTo("stark");
		}
	}

	@Test // DATAREDIS-605
	void shouldFindByExample() {

		Person eddard = new Person("eddard", "stark");
		Person tyrion = new Person("tyrion", "lannister");
		Person robb = new Person("robb", "stark");
		Person jon = new Person("jon", "snow");
		Person arya = new Person("arya", "stark");

		repo.saveAll(Arrays.asList(eddard, tyrion, robb, jon, arya));

		List<Person> result = repo.findAll(Example.of(new Person(null, "stark")));

		assertThat(result).hasSize(3);
	}

	@Test // DATAREDIS-533
	void nearQueryShouldReturnResultsCorrectly() {

		City palermo = new City();
		palermo.location = new Point(13.361389D, 38.115556D);

		City catania = new City();
		catania.location = new Point(15.087269D, 37.502669D);

		cityRepo.saveAll(Arrays.asList(palermo, catania));

		List<City> result = cityRepo.findByLocationNear(new Point(15D, 37D), new Distance(200, Metrics.KILOMETERS));
		assertThat(result).contains(palermo, catania);

		result = cityRepo.findByLocationNear(new Point(15D, 37D), new Distance(100, Metrics.KILOMETERS));
		assertThat(result).contains(catania).doesNotContain(palermo);
	}

	@Test // DATAREDIS-533
	void nearQueryShouldFindNothingIfOutOfRange() {

		City palermo = new City();
		palermo.location = new Point(13.361389D, 38.115556D);

		City catania = new City();
		catania.location = new Point(15.087269D, 37.502669D);

		cityRepo.saveAll(Arrays.asList(palermo, catania));

		List<City> result = cityRepo.findByLocationNear(new Point(15D, 37D), new Distance(10, Metrics.KILOMETERS));
		assertThat(result).isEmpty();
	}

	@Test // DATAREDIS-533
	void nearQueryShouldReturnResultsCorrectlyOnNestedProperty() {

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
		assertThat(result).contains(p1, p2);

		result = repo.findByHometownLocationNear(new Point(15D, 37D), new Distance(100, Metrics.KILOMETERS));
		assertThat(result).contains(p2).doesNotContain(p1);
	}

	@Test // GH-1242
	void nearQueryShouldConsiderLimit() {

		City palermo = new City();
		palermo.location = new Point(13.361389D, 38.115556D);

		City catania = new City();
		catania.location = new Point(15.087269D, 37.502669D);

		Person p1 = new Person("foo", "bar");
		p1.hometown = palermo;

		Person p2 = new Person("two", "two");
		p2.hometown = catania;

		repo.saveAll(Arrays.asList(p1, p2));

		Slice<Person> result = repo.findByHometownLocationNear(new Point(15D, 37D), new Distance(200, Metrics.KILOMETERS),
				Pageable.ofSize(1));
		assertThat(result).containsOnly(p2);
	}

	@Test // DATAREDIS-849
	void shouldReturnNewObjectInstanceOnImmutableSave() {

		Immutable object = new Immutable(null, "Walter", new Immutable("heisenberg", "White", null));
		Immutable saved = immutableObjectRepo.save(object);

		assertThat(object.id).isNull();
		assertThat(saved.id).isNotNull();
	}

	@Test // DATAREDIS-849
	void shouldReturnNewObjectInstanceOnImmutableSaveAll() {

		Immutable object = new Immutable(null, "Walter", new Immutable("heisenberg", "White", null));
		List<Immutable> saved = (List) immutableObjectRepo.saveAll(Collections.singleton(object));

		assertThat(object.id).isNull();
		assertThat(saved.get(0).id).isNotNull();
	}

	@Test // DATAREDIS-849
	void shouldProperlyReadNestedImmutableObject() {

		Immutable nested = new Immutable("heisenberg", "White", null);
		Immutable object = new Immutable(null, "Walter", nested);
		Immutable saved = immutableObjectRepo.save(object);
		Immutable loaded = immutableObjectRepo.findById(saved.id).get();

		assertThat(loaded.nested).isEqualTo(nested);
	}

	@Test // GH-2677
	void shouldProperlyHandleEntityWithOffsetJavaTimeTypes() {

		User jonDoe = User.of("Jon Doe").expires(OffsetTime.now().plusMinutes(5)).lastAccess(OffsetDateTime.now());

		this.userRepository.save(jonDoe);

		User loadedJonDoe = this.userRepository.findById(jonDoe.getName()).orElse(null);

		assertThat(loadedJonDoe).isNotNull();
		assertThat(loadedJonDoe).isNotSameAs(jonDoe);
		assertThat(loadedJonDoe.getName()).isEqualTo(jonDoe.getName());
		assertThat(loadedJonDoe.getLastAccessed()).isEqualTo(jonDoe.getLastAccessed());
		assertThat(loadedJonDoe.getExpiration()).isEqualTo(jonDoe.getExpiration());
	}

	public interface PersonRepository extends PagingAndSortingRepository<Person, String>, CrudRepository<Person, String>,
			QueryByExampleExecutor<Person> {

		List<Person> findByFirstname(String firstname);

		List<Person> findByLastname(String lastname);

		List<Person> findByLastname(String lastname, Sort sort);

		List<Person> findByLastnameOrderByFirstnameAsc(String lastname);

		List<Person> findByLastnameOrderByFirstnameDesc(String lastname);

		Page<Person> findPersonByLastname(String lastname, Pageable page);

		List<Person> findPersonByAliveIsTrue();

		List<Person> findPersonByAliveIsFalse();

		List<Person> findByFirstnameAndLastname(String firstname, String lastname);

		List<Person> findByFirstnameOrLastname(String firstname, String lastname);

		List<Person> findByFirstnameAndLastnameOrAliveIsTrue(String firstname, String lastname);

		List<Person> findFirstBy();

		List<Person> findTop2By();

		List<Person> findTop2ByLastname(String lastname);

		Page<Person> findBy(Pageable page);

		List<Person> findByHometownLocationNear(Point point, Distance distance);

		Slice<Person> findByHometownLocationNear(Point point, Distance distance, Pageable pageable);

		Person findEntityById(String id);

		<T> T findProjectionById(String id, Class<T> projection);

		Streamable<PersonProjection> findProjectionStreamBy();

		List<PersonProjection> findProjectionBy();

		List<PersonDto> findProjectionDtoBy();

		@Override
		<S extends Person> List<S> findAll(Example<S> example);
	}

	public interface PersonProjection {
		String getFirstname();
	}

	record PersonDto(String firstname) {
	}

	public interface CityRepository extends CrudRepository<City, String> {

		List<City> findByLocationNear(Point point, Distance distance);
	}

	public interface ImmutableObjectRepository extends CrudRepository<Immutable, String> {}

	public interface UserRepository extends CrudRepository<User, String> {}

	/**
	 * Custom Redis {@link IndexConfiguration} forcing index of {@link Person#lastname}.
	 *
	 * @author Christoph Strobl
	 */
	static class MyIndexConfiguration extends IndexConfiguration {

		@Override
		protected Iterable<IndexDefinition> initialConfiguration() {
			return Collections.singleton(new SimpleIndexDefinition("persons", "lastname"));
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

		public Boolean getAlive() {
			return this.alive;
		}

		public void setAlive(Boolean alive) {
			this.alive = alive;
		}

		public String getLastname() {
			return this.lastname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		public City getCity() {
			return this.city;
		}

		public void setCity(City city) {
			this.city = city;
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
					&& Objects.equals(this.getLastname(), that.getLastname()) && Objects.equals(this.getAlive(), that.getAlive())
					&& Objects.equals(this.getCity(), that.getCity()) && Objects.equals(this.getHometown(), that.getHometown());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getId(), getFirstname(), getLastname(), getAlive(), getCity(), getHometown());
		}
	}

	static class City {

		@Id String id;
		String name;
		@GeoIndexed Point location;

		public String getId() {
			return this.id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Point getLocation() {
			return this.location;
		}

		public void setLocation(Point location) {
			this.location = location;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof City that)) {
				return false;
			}

			return Objects.equals(this.getId(), that.getId()) && Objects.equals(this.getName(), that.getName())
					&& Objects.equals(this.getLocation(), that.getLocation());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getId(), getName(), getLocation());
		}

		@Override
		public String toString() {

			return "City{" + "id='" + id + '\'' + ", name='" + name + '\'' + ", location=" + location + '}';
		}
	}

	static final class Immutable {

		private final @Id String id;
		private final String name;
		private final Immutable nested;

		public Immutable(String id, String name, Immutable nested) {
			this.id = id;
			this.name = name;
			this.nested = nested;
		}

		public String getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public Immutable getNested() {
			return this.nested;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof Immutable that)) {
				return false;
			}

			return Objects.equals(this.getId(), that.getId()) && Objects.equals(this.getName(), that.getName())
					&& Objects.equals(this.getNested(), that.getNested());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getId(), getName(), getNested());
		}

		public String toString() {

			return "RedisRepositoryIntegrationTestBase.Immutable(id=" + this.getId() + ", name=" + this.getName()
					+ ", nested=" + this.getNested() + ")";
		}

		public Immutable withId(String id) {
			return Objects.equals(getId(), id) ? this : new Immutable(id, this.name, this.nested);
		}

		public Immutable withName(String name) {
			return Objects.equals(getName(), name) ? this : new Immutable(this.id, name, this.nested);
		}

		public Immutable withNested(Immutable nested) {
			return Objects.equals(getNested(), nested) ? this : new Immutable(this.id, this.name, nested);
		}
	}

	@RedisHash("Users")
	static class User {

		@Id private final String name;

		private OffsetDateTime lastAccessed;

		private OffsetTime expiration;

		private User(String name) {
			this.name = name;
		}

		static User of(String name) {
			return new User(name);
		}

		@Nullable
		public OffsetTime getExpiration() {
			return this.expiration;
		}

		@Nullable
		public OffsetDateTime getLastAccessed() {
			return this.lastAccessed;
		}

		public String getName() {
			return this.name;
		}

		public User lastAccess(OffsetDateTime dateTime) {
			this.lastAccessed = dateTime;
			return this;
		}

		public User expires(OffsetTime time) {
			this.expiration = time;
			return this;
		}

	}
}
