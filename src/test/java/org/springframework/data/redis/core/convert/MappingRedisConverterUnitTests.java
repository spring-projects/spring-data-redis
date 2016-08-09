/*
 * Copyright 2015-2017 the original author or authors.
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
package org.springframework.data.redis.core.convert;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.core.convert.ConversionTestEntities.*;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.redis.core.PartialUpdate;
import org.springframework.data.redis.core.convert.ConversionTestEntities.*;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration.KeyspaceSettings;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.test.util.BucketTester;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Christoph Strobl
 * @author Greg Turnquist
 */
@RunWith(MockitoJUnitRunner.class)
public class MappingRedisConverterUnitTests {

	public @Rule ExpectedException exception = ExpectedException.none();
	@Mock ReferenceResolver resolverMock;
	MappingRedisConverter converter;
	Person rand;

	@Before
	public void setUp() {

		converter = new MappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		converter.afterPropertiesSet();

		rand = new Person();
	}

	@Test // DATAREDIS-425
	public void writeAppendsTypeHintForRootCorrectly() {
		assertThat(BucketTester.from(write(rand).getBucket())).containingTypeHint("_class", Person.class);
	}

	@Test // DATAREDIS-425
	public void writeAppendsKeyCorrectly() {

		rand.id = "1";

		assertThat(write(rand).getId()).isEqualTo("1");
	}

	@Test // DATAREDIS-425
	public void writeAppendsKeyCorrectlyWhenThereIsAnAdditionalIdFieldInNestedElement() {

		AddressWithId address = new AddressWithId();
		address.id = "tear";
		address.city = "Tear";

		rand.id = "1";
		rand.address = address;

		RedisData data = write(rand);

		assertThat(data.getId()).isEqualTo((Serializable) "1");
		assertThat(BucketTester.from(data.getBucket())).containingUtf8String("address.id", "tear");
	}

	@Test // DATAREDIS-425
	public void writeDoesNotAppendPropertiesWithNullValues() {

		rand.firstname = "rand";

		assertThat(BucketTester.from(write(rand).getBucket())).without("lastname");
	}

	@Test // DATAREDIS-425
	public void writeDoesNotAppendPropertiesWithEmptyCollections() {

		rand.firstname = "rand";

		assertThat(BucketTester.from(write(rand).getBucket())).without("nicknames");
	}

	@Test // DATAREDIS-425
	public void writeAppendsSimpleRootPropertyCorrectly() {

		rand.firstname = "nynaeve";

		assertThat(BucketTester.from(write(rand).getBucket())).containingUtf8String("firstname", "nynaeve");
	}

	@Test // DATAREDIS-425
	public void writeAppendsListOfSimplePropertiesCorrectly() {

		rand.nicknames = Arrays.asList("dragon reborn", "lews therin");

		RedisData target = write(rand);

		assertThat(BucketTester.from(target.getBucket())).containingUtf8String("nicknames.[0]", "dragon reborn")
				.containingUtf8String("nicknames.[1]", "lews therin");
	}

	@Test // DATAREDIS-425
	public void writeAppendsComplexObjectCorrectly() {

		Address address = new Address();
		address.city = "two rivers";
		address.country = "andora";
		rand.address = address;

		RedisData target = write(rand);

		assertThat(BucketTester.from(target.getBucket())).containingUtf8String("address.city", "two rivers")
				.containingUtf8String("address.country", "andora");
	}

	@Test // DATAREDIS-425
	public void writeAppendsListOfComplexObjectsCorrectly() {

		Person mat = new Person();
		mat.firstname = "mat";
		mat.nicknames = Arrays.asList("prince of the ravens");

		Person perrin = new Person();
		perrin.firstname = "perrin";
		perrin.address = new Address();
		perrin.address.city = "two rivers";

		rand.coworkers = Arrays.asList(mat, perrin);
		rand.id = UUID.randomUUID().toString();
		rand.firstname = "rand";

		RedisData target = write(rand);

		assertThat(BucketTester.from(target.getBucket())).containingUtf8String("coworkers.[0].firstname", "mat") //
						.containingUtf8String("coworkers.[0].nicknames.[0]", "prince of the ravens") //
						.containingUtf8String("coworkers.[1].firstname", "perrin") //
						.containingUtf8String("coworkers.[1].address.city", "two rivers");
	}

	@Test // DATAREDIS-425
	public void writeDoesNotAddClassTypeInformationCorrectlyForMatchingTypes() {

		Address address = new Address();
		address.city = "two rivers";

		rand.address = address;

		RedisData target = write(rand);

		assertThat(BucketTester.from(target.getBucket())).without("address._class");
	}

	@Test // DATAREDIS-425
	public void writeAddsClassTypeInformationCorrectlyForNonMatchingTypes() {

		AddressWithPostcode address = new AddressWithPostcode();
		address.city = "two rivers";
		address.postcode = "1234";

		rand.address = address;

		RedisData target = write(rand);

		assertThat(BucketTester.from(target.getBucket())).containingTypeHint("address._class", AddressWithPostcode.class);
	}

	@Test // DATAREDIS-425
	public void readConsidersClassTypeInformationCorrectlyForNonMatchingTypes() {

		Map<String, String> map = new HashMap<String, String>();
		map.put("address._class", AddressWithPostcode.class.getName());
		map.put("address.postcode", "1234");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.address).isInstanceOf(AddressWithPostcode.class);
	}

	@Test // DATAREDIS-425
	public void writeAddsClassTypeInformationCorrectlyForNonMatchingTypesInCollections() {

		Person mat = new TaVeren();
		mat.firstname = "mat";

		rand.coworkers = Arrays.asList(mat);

		RedisData target = write(rand);

		assertThat(BucketTester.from(target.getBucket())).containingTypeHint("coworkers.[0]._class", TaVeren.class);
	}

	@Test // DATAREDIS-425
	public void readConvertsSimplePropertiesCorrectly() {

		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("firstname", "rand")));

		assertThat(converter.read(Person.class, rdo).firstname).isEqualTo("rand");
	}

	@Test // DATAREDIS-425
	public void readConvertsListOfSimplePropertiesCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("nicknames.[0]", "dragon reborn");
		map.put("nicknames.[1]", "lews therin");
		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		assertThat(converter.read(Person.class, rdo).nicknames).contains("dragon reborn", "lews therin");
	}

	@Test // DATAREDIS-425
	public void readConvertsUnorderedListOfSimplePropertiesCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("nicknames.[9]", "car'a'carn");
		map.put("nicknames.[10]", "lews therin");
		map.put("nicknames.[1]", "dragon reborn");
		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		assertThat(converter.read(Person.class, rdo).nicknames).contains("dragon reborn", "car'a'carn", "lews therin");
	}

	@Test // DATAREDIS-425
	public void readComplexPropertyCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("address.city", "two rivers");
		map.put("address.country", "andor");
		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		Person target = converter.read(Person.class, rdo);

		assertThat(target.address).isNotNull();
		assertThat(target.address.city).isEqualTo("two rivers");
		assertThat(target.address.country).isEqualTo("andor");
	}

	@Test // DATAREDIS-425
	public void readListComplexPropertyCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("coworkers.[0].firstname", "mat");
		map.put("coworkers.[0].nicknames.[0]", "prince of the ravens");
		map.put("coworkers.[0].nicknames.[1]", "gambler");
		map.put("coworkers.[1].firstname", "perrin");
		map.put("coworkers.[1].address.city", "two rivers");
		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		Person target = converter.read(Person.class, rdo);

		assertThat(target.coworkers).isNotNull();
		assertThat(target.coworkers.get(0).firstname).isEqualTo("mat");
		assertThat(target.coworkers.get(0).nicknames).isNotNull();
		assertThat(target.coworkers.get(0).nicknames.get(0)).isEqualTo("prince of the ravens");
		assertThat(target.coworkers.get(0).nicknames.get(1)).isEqualTo("gambler");

		assertThat(target.coworkers.get(1).firstname).isEqualTo("perrin");
		assertThat(target.coworkers.get(1).address.city).isEqualTo("two rivers");
	}

	@Test // DATAREDIS-425
	public void readUnorderedListOfComplexPropertyCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("coworkers.[10].firstname", "perrin");
		map.put("coworkers.[10].address.city", "two rivers");
		map.put("coworkers.[1].firstname", "mat");
		map.put("coworkers.[1].nicknames.[1]", "gambler");
		map.put("coworkers.[1].nicknames.[0]", "prince of the ravens");

		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		Person target = converter.read(Person.class, rdo);

		assertThat(target.coworkers).isNotNull();
		assertThat(target.coworkers.get(0).firstname).isEqualTo("mat");
		assertThat(target.coworkers.get(0).nicknames).isNotNull();
		assertThat(target.coworkers.get(0).nicknames.get(0)).isEqualTo("prince of the ravens");
		assertThat(target.coworkers.get(0).nicknames.get(1)).isEqualTo("gambler");

		assertThat(target.coworkers.get(1).firstname).isEqualTo("perrin");
		assertThat(target.coworkers.get(1).address.city).isEqualTo("two rivers");
	}

	@Test // DATAREDIS-425
	public void readListComplexPropertyCorrectlyAndConsidersClassTypeInformation() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("coworkers.[0]._class", TaVeren.class.getName());
		map.put("coworkers.[0].firstname", "mat");

		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		Person target = converter.read(Person.class, rdo);

		assertThat(target.coworkers).isNotNull();
		assertThat(target.coworkers.get(0)).isInstanceOf(TaVeren.class);
		assertThat(target.coworkers.get(0).firstname).isEqualTo("mat");
	}

	@Test // DATAREDIS-425
	public void writeAppendsMapWithSimpleKeyCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("hair-color", "red");
		map.put("eye-color", "grey");

		rand.physicalAttributes = map;

		RedisData target = write(rand);

		assertThat(BucketTester.from(target.getBucket())).containingUtf8String("physicalAttributes.[hair-color]", "red") //
				.containingUtf8String("physicalAttributes.[eye-color]", "grey");
	}

	@Test // DATAREDIS-425
	public void writeAppendsMapWithSimpleKeyOnNestedObjectCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("hair-color", "red");
		map.put("eye-color", "grey");

		rand.coworkers = new ArrayList<Person>();
		rand.coworkers.add(new Person());
		rand.coworkers.get(0).physicalAttributes = map;

		RedisData target = write(rand);

		assertThat(BucketTester.from(target.getBucket())).containingUtf8String("coworkers.[0].physicalAttributes.[hair-color]", "red") //
						.containingUtf8String("coworkers.[0].physicalAttributes.[eye-color]", "grey");
	}

	@Test // DATAREDIS-425
	public void readSimpleMapValuesCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("physicalAttributes.[hair-color]", "red");
		map.put("physicalAttributes.[eye-color]", "grey");

		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		Person target = converter.read(Person.class, rdo);

		assertThat(target.physicalAttributes).isNotNull();
		assertThat(target.physicalAttributes.get("hair-color")).isEqualTo("red");
		assertThat(target.physicalAttributes.get("eye-color")).isEqualTo("grey");
	}

	@Test // DATAREDIS-425
	public void writeAppendsMapWithComplexObjectsCorrectly() {

		Map<String, Person> map = new LinkedHashMap<String, Person>();
		Person janduin = new Person();
		janduin.firstname = "janduin";
		map.put("father", janduin);
		Person tam = new Person();
		tam.firstname = "tam";
		map.put("step-father", tam);

		rand.relatives = map;

		RedisData target = write(rand);

		assertThat(BucketTester.from(target.getBucket())).containingUtf8String("relatives.[father].firstname", "janduin") //
				.containingUtf8String("relatives.[step-father].firstname", "tam");
	}

	@Test // DATAREDIS-425
	public void readMapWithComplexObjectsCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("relatives.[father].firstname", "janduin");
		map.put("relatives.[step-father].firstname", "tam");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.relatives).isNotNull();
		assertThat(target.relatives.get("father")).isNotNull();
		assertThat(target.relatives.get("father").firstname).isEqualTo("janduin");
		assertThat(target.relatives.get("step-father")).isNotNull();
		assertThat(target.relatives.get("step-father").firstname).isEqualTo("tam");
	}

	@Test // DATAREDIS-425
	public void writeAppendsClassTypeInformationCorrectlyForMapWithComplexObjects() {

		Map<String, Person> map = new LinkedHashMap<String, Person>();
		Person lews = new TaVeren();
		lews.firstname = "lews";
		map.put("previous-incarnation", lews);

		rand.relatives = map;

		RedisData target = write(rand);

		assertThat(BucketTester.from(target.getBucket())).containingTypeHint("relatives.[previous-incarnation]._class", TaVeren.class);
	}

	@Test // DATAREDIS-425
	public void readConsidersClassTypeInformationCorrectlyForMapWithComplexObjects() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("relatives.[previous-incarnation]._class", TaVeren.class.getName());
		map.put("relatives.[previous-incarnation].firstname", "lews");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.relatives.get("previous-incarnation")).isNotNull();
		assertThat(target.relatives.get("previous-incarnation")).isInstanceOf(TaVeren.class);
		assertThat(target.relatives.get("previous-incarnation").firstname).isEqualTo("lews");
	}

	@Test // DATAREDIS-425
	public void writesIntegerValuesCorrectly() {

		rand.age = 20;

		assertThat(BucketTester.from(write(rand).getBucket())).containingUtf8String("age", "20");
	}

	@Test // DATAREDIS-425
	public void writesLocalDateTimeValuesCorrectly() {

		rand.localDateTime = LocalDateTime.parse("2016-02-19T10:18:01");

		assertThat(BucketTester.from(write(rand).getBucket())).containingUtf8String("localDateTime", "2016-02-19T10:18:01");
	}

	@Test // DATAREDIS-425
	public void readsLocalDateTimeValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("localDateTime", "2016-02-19T10:18:01"))));

		assertThat(target.localDateTime).isEqualTo(LocalDateTime.parse("2016-02-19T10:18:01"));
	}

	@Test // DATAREDIS-425
	public void writesLocalDateValuesCorrectly() {

		rand.localDate = LocalDate.parse("2016-02-19");

		assertThat(BucketTester.from(write(rand).getBucket())).containingUtf8String("localDate", "2016-02-19");
	}

	@Test // DATAREDIS-425
	public void readsLocalDateValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("localDate", "2016-02-19"))));

		assertThat(target.localDate).isEqualTo(LocalDate.parse("2016-02-19"));
	}

	@Test // DATAREDIS-425
	public void writesLocalTimeValuesCorrectly() {

		rand.localTime = LocalTime.parse("11:12:13");

		assertThat(BucketTester.from(write(rand).getBucket())).containingUtf8String("localTime", "11:12:13");
	}

	@Test // DATAREDIS-425
	public void readsLocalTimeValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("localTime", "11:12"))));

		assertThat(target.localTime).isEqualTo(LocalTime.parse("11:12:00"));
	}

	@Test // DATAREDIS-425
	public void writesZonedDateTimeValuesCorrectly() {

		rand.zonedDateTime = ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]");

		assertThat(BucketTester.from(write(rand).getBucket())).containingUtf8String("zonedDateTime", "2007-12-03T10:15:30+01:00[Europe/Paris]");
	}

	@Test // DATAREDIS-425
	public void readsZonedDateTimeValuesCorrectly() {

		Person target = converter.read(Person.class, new RedisData(Bucket
				.newBucketFromStringMap(Collections.singletonMap("zonedDateTime", "2007-12-03T10:15:30+01:00[Europe/Paris]"))));

		assertThat(target.zonedDateTime).isEqualTo(ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]"));
	}

	@Test // DATAREDIS-425
	public void writesInstantValuesCorrectly() {

		rand.instant = Instant.parse("2007-12-03T10:15:30.01Z");

		assertThat(BucketTester.from(write(rand).getBucket())).containingUtf8String("instant", "2007-12-03T10:15:30.010Z");
	}

	@Test // DATAREDIS-425
	public void readsInstantValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("instant", "2007-12-03T10:15:30.01Z"))));

		assertThat(target.instant).isEqualTo(Instant.parse("2007-12-03T10:15:30.01Z"));
	}

	@Test // DATAREDIS-425
	public void writesZoneIdValuesCorrectly() {

		rand.zoneId = ZoneId.of("Europe/Paris");

		assertThat(BucketTester.from(write(rand).getBucket())).containingUtf8String("zoneId", "Europe/Paris");
	}

	@Test // DATAREDIS-425
	public void readsZoneIdValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("zoneId", "Europe/Paris"))));

		assertThat(target.zoneId).isEqualTo(ZoneId.of("Europe/Paris"));
	}

	@Test // DATAREDIS-425
	public void writesDurationValuesCorrectly() {

		rand.duration = Duration.parse("P2DT3H4M");

		assertThat(BucketTester.from(write(rand).getBucket())).containingUtf8String("duration", "PT51H4M");
	}

	@Test // DATAREDIS-425
	public void readsDurationValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("duration", "PT51H4M"))));

		assertThat(target.duration).isEqualTo(Duration.parse("P2DT3H4M"));
	}

	@Test // DATAREDIS-425
	public void writesPeriodValuesCorrectly() {

		rand.period = Period.parse("P1Y2M25D");

		assertThat(BucketTester.from(write(rand).getBucket())).containingUtf8String("period", "P1Y2M25D");
	}

	@Test // DATAREDIS-425
	public void readsPeriodValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("period", "P1Y2M25D"))));

		assertThat(target.period).isEqualTo(Period.parse("P1Y2M25D"));
	}

	@Test // DATAREDIS-425
	public void writesEnumValuesCorrectly() {

		rand.gender = Gender.MALE;

		assertThat(BucketTester.from(write(rand).getBucket())).containingUtf8String("gender", "MALE");
	}

	@Test // DATAREDIS-425
	public void readsEnumValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("gender", "MALE"))));

		assertThat(target.gender).isEqualTo(Gender.MALE);
	}

	@Test // DATAREDIS-425
	public void writesBooleanValuesCorrectly() {

		rand.alive = Boolean.TRUE;

		assertThat(BucketTester.from(write(rand).getBucket())).containingUtf8String("alive", "1");
	}

	@Test // DATAREDIS-425
	public void readsBooleanValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("alive", "1"))));

		assertThat(target.alive).isEqualTo(Boolean.TRUE);
	}

	@Test // DATAREDIS-425
	public void readsStringBooleanValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("alive", "true"))));

		assertThat(target.alive).isEqualTo(Boolean.TRUE);
	}

	@Test // DATAREDIS-425
	public void writesDateValuesCorrectly() {

		Calendar cal = Calendar.getInstance();
		cal.set(1978, 10, 25);

		rand.birthdate = cal.getTime();

		assertThat(BucketTester.from(write(rand).getBucket())).containingDateAsMsec("birthdate", rand.birthdate);
	}

	@Test // DATAREDIS-425
	public void readsDateValuesCorrectly() {

		Calendar cal = Calendar.getInstance();
		cal.set(1978, 10, 25);

		Date date = cal.getTime();

		Person target = converter.read(Person.class, new RedisData(
				Bucket.newBucketFromStringMap(Collections.singletonMap("birthdate", Long.valueOf(date.getTime()).toString()))));

		assertThat(target.birthdate).isEqualTo(date);
	}

	@Test // DATAREDIS-425
	public void writeSingleReferenceOnRootCorrectly() {

		Location location = new Location();
		location.id = "1";
		location.name = "tar valon";

		rand.location = location;

		RedisData target = write(rand);

		assertThat(BucketTester.from(target.getBucket())).containingUtf8String("location", "locations:1") //
						.without("location.id") //
						.without("location.name");
	}

	@Test // DATAREDIS-425
	public void readLoadsReferenceDataOnRootCorrectly() {

		Location location = new Location();
		location.id = "1";
		location.name = "tar valon";

		Map<String, String> locationMap = new LinkedHashMap<String, String>();
		locationMap.put("id", location.id);
		locationMap.put("name", location.name);

		when(resolverMock.resolveReference(eq("1"), eq("locations")))
				.thenReturn(Bucket.newBucketFromStringMap(locationMap).rawMap());

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("location", "locations:1");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.location).isEqualTo(location);
	}

	@Test // DATAREDIS-425
	public void writeSingleReferenceOnNestedElementCorrectly() {

		Location location = new Location();
		location.id = "1";
		location.name = "tar valon";

		Person egwene = new Person();
		egwene.location = location;

		rand.coworkers = Collections.singletonList(egwene);

		RedisData target = write(rand);

		assertThat(BucketTester.from(target.getBucket())).containingUtf8String("coworkers.[0].location", "locations:1") //
						.without("coworkers.[0].location.id") //
						.without("coworkers.[0].location.name");
	}

	@Test // DATAREDIS-425
	public void readLoadsReferenceDataOnNestedElementCorrectly() {

		Location location = new Location();
		location.id = "1";
		location.name = "tar valon";

		Map<String, String> locationMap = new LinkedHashMap<String, String>();
		locationMap.put("id", location.id);
		locationMap.put("name", location.name);

		when(resolverMock.resolveReference(eq("1"), eq("locations")))
				.thenReturn(Bucket.newBucketFromStringMap(locationMap).rawMap());

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("coworkers.[0].location", "locations:1");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.coworkers.get(0).location).isEqualTo(location);
	}

	@Test // DATAREDIS-425
	public void writeListOfReferencesOnRootCorrectly() {

		Location tarValon = new Location();
		tarValon.id = "1";
		tarValon.name = "tar valon";

		Location falme = new Location();
		falme.id = "2";
		falme.name = "falme";

		Location tear = new Location();
		tear.id = "3";
		tear.name = "city of tear";

		rand.visited = Arrays.asList(tarValon, falme, tear);

		RedisData target = write(rand);

		assertThat(BucketTester.from(target.getBucket())).containingUtf8String("visited.[0]", "locations:1") //
						.containingUtf8String("visited.[1]", "locations:2") //
						.containingUtf8String("visited.[2]", "locations:3");
	}

	@Test // DATAREDIS-425
	public void readLoadsListOfReferencesOnRootCorrectly() {

		Location tarValon = new Location();
		tarValon.id = "1";
		tarValon.name = "tar valon";

		Location falme = new Location();
		falme.id = "2";
		falme.name = "falme";

		Location tear = new Location();
		tear.id = "3";
		tear.name = "city of tear";

		Map<String, String> tarValonMap = new LinkedHashMap<String, String>();
		tarValonMap.put("id", tarValon.id);
		tarValonMap.put("name", tarValon.name);

		Map<String, String> falmeMap = new LinkedHashMap<String, String>();
		falmeMap.put("id", falme.id);
		falmeMap.put("name", falme.name);

		Map<String, String> tearMap = new LinkedHashMap<String, String>();
		tearMap.put("id", tear.id);
		tearMap.put("name", tear.name);

		Bucket.newBucketFromStringMap(tearMap).rawMap();

		when(resolverMock.resolveReference(eq("1"), eq("locations")))
				.thenReturn(Bucket.newBucketFromStringMap(tarValonMap).rawMap());
		when(resolverMock.resolveReference(eq("2"), eq("locations")))
				.thenReturn(Bucket.newBucketFromStringMap(falmeMap).rawMap());
		when(resolverMock.resolveReference(eq("3"), eq("locations")))
				.thenReturn(Bucket.newBucketFromStringMap(tearMap).rawMap());

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("visited.[0]", "locations:1");
		map.put("visited.[1]", "locations:2");
		map.put("visited.[2]", "locations:3");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.visited.get(0)).isEqualTo(tarValon);
		assertThat(target.visited.get(1)).isEqualTo(falme);
		assertThat(target.visited.get(2)).isEqualTo(tear);
	}

	@Test // DATAREDIS-425
	public void writeSetsAnnotatedTimeToLiveCorrectly() {

		ExpiringPerson birgitte = new ExpiringPerson();
		birgitte.id = "birgitte";
		birgitte.name = "Birgitte Silverbow";

		assertThat(write(birgitte).getTimeToLive()).isEqualTo(5L);
	}

	@Test // DATAREDIS-425
	public void writeDoesNotTTLWhenNotPresent() {

		Location tear = new Location();
		tear.id = "tear";
		tear.name = "Tear";

		assertThat(write(tear).getTimeToLive()).isNull();
	}

	@Test // DATAREDIS-425
	public void writeShouldConsiderKeyspaceConfiguration() {

		this.converter.getMappingContext().getMappingConfiguration().getKeyspaceConfiguration()
				.addKeyspaceSettings(new KeyspaceSettings(Address.class, "o_O"));

		Address address = new Address();
		address.city = "Tear";

		assertThat(write(address).getKeyspace()).isEqualTo("o_O");
	}

	@Test // DATAREDIS-425
	public void writeShouldConsiderTimeToLiveConfiguration() {

		KeyspaceSettings assignment = new KeyspaceSettings(Address.class, "o_O");
		assignment.setTimeToLive(5L);

		this.converter.getMappingContext().getMappingConfiguration().getKeyspaceConfiguration()
				.addKeyspaceSettings(assignment);

		Address address = new Address();
		address.city = "Tear";

		assertThat(write(address).getTimeToLive()).isEqualTo(5L);
	}

	@Test // DATAREDIS-425
	public void writeShouldHonorCustomConversionOnRootType() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new CustomConversions(Collections.singletonList(new AddressToBytesConverter())));
		this.converter.afterPropertiesSet();

		Address address = new Address();
		address.country = "Tel'aran'rhiod";
		address.city = "unknown";

		assertThat(BucketTester.from(write(address).getBucket())).containingUtf8String("_raw", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}");
	}

	@Test // DATAREDIS-425
	public void writeShouldHonorCustomConversionOnNestedType() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new CustomConversions(Collections.singletonList(new AddressToBytesConverter())));
		this.converter.afterPropertiesSet();

		Address address = new Address();
		address.country = "Tel'aran'rhiod";
		address.city = "unknown";
		rand.address = address;

		assertThat(BucketTester.from(write(rand).getBucket())).containingUtf8String("address", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}");
	}

	@Test // DATAREDIS-425
	public void writeShouldHonorIndexOnCustomConversionForNestedType() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new CustomConversions(Collections.singletonList(new AddressToBytesConverter())));
		this.converter.afterPropertiesSet();

		Address address = new Address();
		address.country = "andor";
		rand.address = address;

		assertThat(write(rand).getIndexedData()).contains(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "address.country", "andor"));
	}

	@Test // DATAREDIS-425
	public void writeShouldHonorIndexAnnotationsOnWhenCustomConversionOnNestedype() {

		this.converter = new MappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter
				.setCustomConversions(new CustomConversions(Collections.singletonList(new AddressToBytesConverter())));
		this.converter.afterPropertiesSet();

		Address address = new Address();
		address.country = "Tel'aran'rhiod";
		address.city = "unknown";
		rand.address = address;

		assertThat(write(rand).getIndexedData().isEmpty()).isFalse();
	}

	@Test // DATAREDIS-425
	public void readShouldHonorCustomConversionOnRootType() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new CustomConversions(Collections.singletonList(new BytesToAddressConverter())));
		this.converter.afterPropertiesSet();

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("_raw", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}");

		Address target = converter.read(Address.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.city).isEqualTo("unknown");
		assertThat(target.country).isEqualTo("Tel'aran'rhiod");
	}

	@Test // DATAREDIS-425
	public void readShouldHonorCustomConversionOnNestedType() {

		this.converter = new MappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter
				.setCustomConversions(new CustomConversions(Collections.singletonList(new BytesToAddressConverter())));
		this.converter.afterPropertiesSet();

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("address", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.address).isNotNull();
		assertThat(target.address.city).isEqualTo("unknown");
		assertThat(target.address.country).isEqualTo("Tel'aran'rhiod");
	}

	@Test // DATAREDIS-425
	public void writeShouldPickUpTimeToLiveFromPropertyIfPresent() {

		ExipringPersonWithExplicitProperty aviendha = new ExipringPersonWithExplicitProperty();
		aviendha.id = "aviendha";
		aviendha.ttl = 2L;

		assertThat(write(aviendha).getTimeToLive()).isEqualTo(120L);
	}

	@Test // DATAREDIS-425
	public void writeShouldUseDefaultTimeToLiveIfPropertyIsPresentButNull() {

		ExipringPersonWithExplicitProperty aviendha = new ExipringPersonWithExplicitProperty();
		aviendha.id = "aviendha";

		assertThat(write(aviendha).getTimeToLive()).isEqualTo(5L);
	}

	@Test // DATAREDIS-425
	public void writeShouldConsiderMapConvertersForRootType() {

		this.converter = new MappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter.setCustomConversions(new CustomConversions(Collections.singletonList(new SpeciesToMapConverter())));
		this.converter.afterPropertiesSet();

		Species myrddraal = new Species();
		myrddraal.name = "myrddraal";
		myrddraal.alsoKnownAs = Arrays.asList("halfmen", "fades", "neverborn");

		assertThat(BucketTester.from(write(myrddraal).getBucket())).containingUtf8String("species-name", "myrddraal")
				.containingUtf8String("species-nicknames", "halfmen,fades,neverborn");
	}

	@Test // DATAREDIS-425
	public void writeShouldConsiderMapConvertersForNestedType() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter.setCustomConversions(new CustomConversions(Collections.singletonList(new SpeciesToMapConverter())));
		this.converter.afterPropertiesSet();

		rand.species = new Species();
		rand.species.name = "human";

		assertThat(BucketTester.from(write(rand).getBucket())).containingUtf8String("species.species-name", "human");
	}

	@Test // DATAREDIS-425
	public void readShouldConsiderMapConvertersForRootType() {

		this.converter = new MappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter.setCustomConversions(new CustomConversions(Collections.singletonList(new MapToSpeciesConverter())));
		this.converter.afterPropertiesSet();
		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("species-name", "trolloc");

		Species target = converter.read(Species.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target).isNotNull();
		assertThat(target.name).isEqualTo("trolloc");
	}

	@Test // DATAREDIS-425
	public void readShouldConsiderMapConvertersForNestedType() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter.setCustomConversions(new CustomConversions(Collections.singletonList(new MapToSpeciesConverter())));
		this.converter.afterPropertiesSet();

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("species.species-name", "trolloc");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target).isNotNull();
		assertThat(target.species.name).isEqualTo("trolloc");
	}

	@Test // DATAREDIS-425
	public void writeShouldConsiderMapConvertersInsideLists() {

		this.converter = new MappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter.setCustomConversions(new CustomConversions(Collections.singletonList(new SpeciesToMapConverter())));
		this.converter.afterPropertiesSet();

		TheWheelOfTime twot = new TheWheelOfTime();
		twot.species = new ArrayList<Species>();

		Species myrddraal = new Species();
		myrddraal.name = "myrddraal";
		myrddraal.alsoKnownAs = Arrays.asList("halfmen", "fades", "neverborn");
		twot.species.add(myrddraal);

		assertThat(BucketTester.from(write(twot).getBucket())).containingUtf8String("species.[0].species-name", "myrddraal")
				.containingUtf8String("species.[0].species-nicknames", "halfmen,fades,neverborn");
	}

	@Test // DATAREDIS-425
	public void readShouldConsiderMapConvertersForValuesInList() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter.setCustomConversions(new CustomConversions(Collections.singletonList(new MapToSpeciesConverter())));
		this.converter.afterPropertiesSet();

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("species.[0].species-name", "trolloc");

		TheWheelOfTime target = converter.read(TheWheelOfTime.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target).isNotNull();
		assertThat(target.species).isNotNull();
		assertThat(target.species.get(0)).isNotNull();
		assertThat(target.species.get(0).name).isEqualTo("trolloc");
	}

	@Test // DATAREDIS-492
	public void writeHandlesArraysProperly() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter.setCustomConversions(new CustomConversions(Collections.singletonList(new ListToByteConverter())));
		this.converter.afterPropertiesSet();

		Map<String, Object> innerMap = new LinkedHashMap<String, Object>();
		innerMap.put("address", "tyrionl@netflix.com");
		innerMap.put("when", new String[] { "pipeline.failed" });

		Map<String, Object> map = new LinkedHashMap<String, Object>();
		map.put("email", Collections.singletonList(innerMap));

		RedisData target = write(map);
	}

	@Test // DATAREDIS-492
	public void writeHandlesArraysOfSimpleTypeProperly() {

		WithArrays source = new WithArrays();
		source.arrayOfSimpleTypes = new String[] { "rand", "mat", "perrin" };

		assertThat(BucketTester.from(write(source).getBucket())).containingUtf8String("arrayOfSimpleTypes.[0]", "rand")
						.containingUtf8String("arrayOfSimpleTypes.[1]", "mat")
						.containingUtf8String("arrayOfSimpleTypes.[2]", "perrin");
	}

	@Test // DATAREDIS-492
	public void readHandlesArraysOfSimpleTypeProperly() {

		Map<String, String> source = new LinkedHashMap<String, String>();
		source.put("arrayOfSimpleTypes.[0]", "rand");
		source.put("arrayOfSimpleTypes.[1]", "mat");
		source.put("arrayOfSimpleTypes.[2]", "perrin");

		WithArrays target = read(WithArrays.class, source);

		assertThat(target.arrayOfSimpleTypes).isEqualTo(new String[] { "rand", "mat", "perrin" });
	}

	@Test // DATAREDIS-492
	public void writeHandlesArraysOfComplexTypeProperly() {

		WithArrays source = new WithArrays();

		Species trolloc = new Species();
		trolloc.name = "trolloc";

		Species myrddraal = new Species();
		myrddraal.name = "myrddraal";
		myrddraal.alsoKnownAs = Arrays.asList("halfmen", "fades", "neverborn");

		source.arrayOfCompexTypes = new Species[] { trolloc, myrddraal };

		assertThat(BucketTester.from(write(source).getBucket())).containingUtf8String("arrayOfCompexTypes.[0].name", "trolloc") //
						.containingUtf8String("arrayOfCompexTypes.[1].name", "myrddraal") //
						.containingUtf8String("arrayOfCompexTypes.[1].alsoKnownAs.[0]", "halfmen") //
						.containingUtf8String("arrayOfCompexTypes.[1].alsoKnownAs.[1]", "fades") //
						.containingUtf8String("arrayOfCompexTypes.[1].alsoKnownAs.[2]", "neverborn");
	}

	@Test // DATAREDIS-492
	public void readHandlesArraysOfComplexTypeProperly() {

		Map<String, String> source = new LinkedHashMap<String, String>();
		source.put("arrayOfCompexTypes.[0].name", "trolloc");
		source.put("arrayOfCompexTypes.[1].name", "myrddraal");
		source.put("arrayOfCompexTypes.[1].alsoKnownAs.[0]", "halfmen");
		source.put("arrayOfCompexTypes.[1].alsoKnownAs.[1]", "fades");
		source.put("arrayOfCompexTypes.[1].alsoKnownAs.[2]", "neverborn");

		WithArrays target = read(WithArrays.class, source);

		assertThat(target.arrayOfCompexTypes[0]).isNotNull();
		assertThat(target.arrayOfCompexTypes[0].name).isEqualTo("trolloc");
		assertThat(target.arrayOfCompexTypes[1]).isNotNull();
		assertThat(target.arrayOfCompexTypes[1].name).isEqualTo("myrddraal");
		assertThat(target.arrayOfCompexTypes[1].alsoKnownAs).contains("halfmen", "fades", "neverborn");
	}

	@Test // DATAREDIS-489
	public void writeHandlesArraysOfObjectTypeProperly() {

		Species trolloc = new Species();
		trolloc.name = "trolloc";

		WithArrays source = new WithArrays();
		source.arrayOfObject = new Object[] { "rand", trolloc, 100L };

		assertThat(BucketTester.from(write(source).getBucket())).containingUtf8String("arrayOfObject.[0]", "rand") //
						.containingUtf8String("arrayOfObject.[0]._class", "java.lang.String")
						.containingUtf8String("arrayOfObject.[1]._class", Species.class.getName()) //
						.containingUtf8String("arrayOfObject.[1].name", "trolloc") //
						.containingUtf8String("arrayOfObject.[2]._class", "java.lang.Long") //
						.containingUtf8String("arrayOfObject.[2]", "100");
	}

	@Test // DATAREDIS-489
	public void readHandlesArraysOfObjectTypeProperly() {

		Map<String, String> source = new LinkedHashMap<String, String>();
		source.put("arrayOfObject.[0]", "rand");
		source.put("arrayOfObject.[0]._class", "java.lang.String");
		source.put("arrayOfObject.[1]._class", Species.class.getName());
		source.put("arrayOfObject.[1].name", "trolloc");
		source.put("arrayOfObject.[2]._class", "java.lang.Long");
		source.put("arrayOfObject.[2]", "100");

		WithArrays target = read(WithArrays.class, source);

		assertThat(target.arrayOfObject[0]).isInstanceOf(String.class);
		assertThat(target.arrayOfObject[1]).isInstanceOf(Species.class);
		assertThat(target.arrayOfObject[2]).isInstanceOf(Long.class);
	}

	@Test // DATAREDIS-489
	public void writeShouldAppendTyeHintToObjectPropertyValueTypesCorrectly() {

		TypeWithObjectValueTypes sample = new TypeWithObjectValueTypes();
		sample.object = "bar";

		Bucket bucket = write(sample).getBucket();

		assertThat(BucketTester.from(bucket)).containingUtf8String("object", "bar").containingUtf8String("object._class", "java.lang.String");
	}

	@Test // DATAREDIS-489
	public void shouldWriteReadObjectPropertyValueTypeCorrectly() {

		TypeWithObjectValueTypes di = new TypeWithObjectValueTypes();
		di.object = "foo";

		RedisData rd = write(di);

		TypeWithObjectValueTypes result = converter.read(TypeWithObjectValueTypes.class, rd);
		assertThat(result.object).isInstanceOf(String.class);
	}

	@Test // DATAREDIS-489
	public void writeShouldAppendTyeHintToObjectMapValueTypesCorrectly() {

		TypeWithObjectValueTypes sample = new TypeWithObjectValueTypes();
		sample.map.put("string", "bar");
		sample.map.put("long", new Long(1L));
		sample.map.put("date", new Date());

		Bucket bucket = write(sample).getBucket();

		assertThat(BucketTester.from(bucket)).containingUtf8String("map.[string]", "bar")
				.containingUtf8String("map.[string]._class", "java.lang.String");
		assertThat(BucketTester.from(bucket)).containingUtf8String("map.[long]", "1").containingUtf8String("map.[long]._class", "java.lang.Long");
		assertThat(BucketTester.from(bucket)).containingUtf8String("map.[date]._class", "java.util.Date");
	}

	@Test // DATAREDIS-489
	public void shouldWriteReadObjectMapValueTypeCorrectly() {

		TypeWithObjectValueTypes sample = new TypeWithObjectValueTypes();
		sample.map.put("string", "bar");
		sample.map.put("long", new Long(1L));
		sample.map.put("date", new Date());

		RedisData rd = write(sample);

		TypeWithObjectValueTypes result = converter.read(TypeWithObjectValueTypes.class, rd);
		assertThat(result.map.get("string")).isInstanceOf(String.class);
		assertThat(result.map.get("long")).isInstanceOf(Long.class);
		assertThat(result.map.get("date")).isInstanceOf(Date.class);
	}

	@Test // DATAREDIS-489
	public void writeShouldAppendTyeHintToObjectListValueTypesCorrectly() {

		TypeWithObjectValueTypes sample = new TypeWithObjectValueTypes();
		sample.list.add("string");
		sample.list.add(new Long(1L));
		sample.list.add(new Date());

		Bucket bucket = write(sample).getBucket();

		assertThat(BucketTester.from(bucket)).containingUtf8String("list.[0]", "string").containingUtf8String("list.[0]._class",
				"java.lang.String");
		assertThat(BucketTester.from(bucket)).containingUtf8String("list.[1]", "1").containingUtf8String("list.[1]._class", "java.lang.Long");
		assertThat(BucketTester.from(bucket)).containingUtf8String("list.[2]._class", "java.util.Date");
	}

	@Test // DATAREDIS-489
	public void shouldWriteReadObjectListValueTypeCorrectly() {

		TypeWithObjectValueTypes sample = new TypeWithObjectValueTypes();
		sample.list.add("string");
		sample.list.add(new Long(1L));
		sample.list.add(new Date());

		RedisData rd = write(sample);

		TypeWithObjectValueTypes result = converter.read(TypeWithObjectValueTypes.class, rd);
		assertThat(result.list.get(0)).isInstanceOf(String.class);
		assertThat(result.list.get(1)).isInstanceOf(Long.class);
		assertThat(result.list.get(2)).isInstanceOf(Date.class);
	}

	@Test // DATAREDIS-509
	public void writeHandlesArraysOfPrimitivesProperly() {

		Map<String, String> source = new LinkedHashMap<String, String>();
		source.put("arrayOfPrimitives.[0]", "1");
		source.put("arrayOfPrimitives.[1]", "2");
		source.put("arrayOfPrimitives.[2]", "3");

		WithArrays target = read(WithArrays.class, source);

		assertThat(target.arrayOfPrimitives[0]).isEqualTo(1);
		assertThat(target.arrayOfPrimitives[1]).isEqualTo(2);
		assertThat(target.arrayOfPrimitives[2]).isEqualTo(3);
	}

	@Test // DATAREDIS-509
	public void readHandlesArraysOfPrimitivesProperly() {

		WithArrays source = new WithArrays();
		source.arrayOfPrimitives = new int[] { 1, 2, 3 };
		assertThat(BucketTester.from(write(source).getBucket())).containingUtf8String("arrayOfPrimitives.[0]", "1")
				.containingUtf8String("arrayOfPrimitives.[1]", "2").containingUtf8String("arrayOfPrimitives.[2]", "3");
	}

	@Test // DATAREDIS-471
	public void writeShouldNotAppendClassTypeHint() {

		Person value = new Person();
		value.firstname = "rand";
		value.age = 24;

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", value);

		assertThat(BucketTester.from(write(update).getBucket())).without("_class");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdateSimpleValueCorrectly() {

		Person value = new Person();
		value.firstname = "rand";
		value.age = 24;

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", value);

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("firstname", "rand").containingUtf8String("age", "24");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithSimpleValueCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("firstname", "rand").set("age",
				24);

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("firstname", "rand").containingUtf8String("age", "24");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdateNestedPathWithSimpleValueCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("address.city", "two rivers");

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("address.city", "two rivers");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithComplexValueCorrectly() {

		Address address = new Address();
		address.city = "two rivers";
		address.country = "andor";

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("address", address);

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("address.city", "two rivers").containingUtf8String("address.country", "andor");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithSimpleListValueCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("nicknames",
				Arrays.asList("dragon", "lews"));

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("nicknames.[0]", "dragon").containingUtf8String("nicknames.[1]", "lews");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithComplexListValueCorrectly() {

		Person mat = new Person();
		mat.firstname = "mat";
		mat.age = 24;

		Person perrin = new Person();
		perrin.firstname = "perrin";

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("coworkers",
				Arrays.asList(mat, perrin));

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("coworkers.[0].firstname", "mat")
				.containingUtf8String("coworkers.[0].age", "24").containingUtf8String("coworkers.[1].firstname", "perrin");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithSimpleListValueWhenNotPassedInAsCollectionCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("nicknames", "dragon");

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("nicknames.[0]", "dragon");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithComplexListValueWhenNotPassedInAsCollectionCorrectly() {

		Person mat = new Person();
		mat.firstname = "mat";
		mat.age = 24;

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("coworkers", mat);

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("coworkers.[0].firstname", "mat")
				.containingUtf8String("coworkers.[0].age", "24");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithSimpleListValueWhenNotPassedInAsCollectionWithPositionalParameterCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("nicknames.[5]", "dragon");

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("nicknames.[5]", "dragon");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithComplexListValueWhenNotPassedInAsCollectionWithPositionalParameterCorrectly() {

		Person mat = new Person();
		mat.firstname = "mat";
		mat.age = 24;

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("coworkers.[5]", mat);

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("coworkers.[5].firstname", "mat")
				.containingUtf8String("coworkers.[5].age", "24");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithSimpleMapValueCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("physicalAttributes",
				Collections.singletonMap("eye-color", "grey"));

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("physicalAttributes.[eye-color]", "grey");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithComplexMapValueCorrectly() {

		Person tam = new Person();
		tam.firstname = "tam";
		tam.alive = false;

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("relatives",
				Collections.singletonMap("father", tam));

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("relatives.[father].firstname", "tam")
				.containingUtf8String("relatives.[father].alive", "0");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithSimpleMapValueWhenNotPassedInAsCollectionCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("physicalAttributes",
				Collections.singletonMap("eye-color", "grey").entrySet().iterator().next());

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("physicalAttributes.[eye-color]", "grey");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithComplexMapValueWhenNotPassedInAsCollectionCorrectly() {

		Person tam = new Person();
		tam.firstname = "tam";
		tam.alive = false;

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("relatives",
				Collections.singletonMap("father", tam).entrySet().iterator().next());

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("relatives.[father].firstname", "tam")
				.containingUtf8String("relatives.[father].alive", "0");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithSimpleMapValueWhenNotPassedInAsCollectionWithPositionalParameterCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("physicalAttributes.[eye-color]",
				"grey");

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("physicalAttributes.[eye-color]", "grey");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithSimpleMapValueOnNestedElementCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("relatives.[father].firstname",
				"tam");

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("relatives.[father].firstname", "tam");
	}

	@Test(expected = MappingException.class) // DATAREDIS-471
	public void writeShouldThrowExceptionOnPartialUpdatePathWithSimpleMapValueWhenItsASingleValueWithoutPath() {

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("physicalAttributes", "grey");

		write(update);
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithRegisteredCustomConversionCorrectly() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new CustomConversions(Collections.singletonList(new AddressToBytesConverter())));
		this.converter.afterPropertiesSet();

		Address address = new Address();
		address.country = "Tel'aran'rhiod";
		address.city = "unknown";

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("address", address);

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("address", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithReferenceCorrectly() {

		Location tar = new Location();
		tar.id = "1";
		tar.name = "tar valon";

		Location tear = new Location();
		tear.id = "2";
		tear.name = "city of tear";

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class).set("visited",
				Arrays.asList(tar, tear));

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("visited.[0]", "locations:1").containingUtf8String("visited.[1]", "locations:2") //
						.without("visited.id") //
						.without("visited.name");
	}

	@Test // DATAREDIS-471
	public void writeShouldWritePartialUpdatePathWithListOfReferencesCorrectly() {

		Location location = new Location();
		location.id = "1";
		location.name = "tar valon";

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class) //
				.set("location", location);

		assertThat(BucketTester.from(write(update).getBucket())).containingUtf8String("location", "locations:1") //
						.without("location.id") //
						.without("location.name");
	}

	@Test // DATAREDIS-471
	public void writeShouldThrowExceptionForUpdateValueNotAssignableToDomainTypeProperty() {

		exception.expect(MappingException.class);
		exception.expectMessage("java.lang.String cannot be assigned");
		exception.expectMessage("java.lang.Integer");
		exception.expectMessage("age");

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class) //
				.set("age", "twenty-four");

		assertThat(BucketTester.from(write(update).getBucket())).without("_class");
	}

	@Test // DATAREDIS-471
	public void writeShouldThrowExceptionForUpdateCollectionValueNotAssignableToDomainTypeProperty() {

		exception.expect(MappingException.class);
		exception.expectMessage("java.lang.String cannot be assigned");
		exception.expectMessage(Person.class.getName());
		exception.expectMessage("coworkers.[0]");

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class) //
				.set("coworkers.[0]", "buh buh the bear");

assertThat(BucketTester.from(write(update).getBucket())).without("_class");	}

	@Test // DATAREDIS-471
	public void writeShouldThrowExceptionForUpdateValueInCollectionNotAssignableToDomainTypeProperty() {

		exception.expect(MappingException.class);
		exception.expectMessage("java.lang.String cannot be assigned");
		exception.expectMessage(Person.class.getName());
		exception.expectMessage("coworkers");

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class) //
				.set("coworkers", Collections.singletonList("foo"));

assertThat(BucketTester.from(write(update).getBucket())).without("_class");	}

	@Test // DATAREDIS-471
	public void writeShouldThrowExceptionForUpdateMapValueNotAssignableToDomainTypeProperty() {

		exception.expect(MappingException.class);
		exception.expectMessage("java.lang.String cannot be assigned");
		exception.expectMessage(Person.class.getName());
		exception.expectMessage("relatives.[father]");

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class) //
				.set("relatives.[father]", "buh buh the bear");

		assertThat(BucketTester.from(write(update).getBucket())).without("_class");
	}

	@Test // DATAREDIS-471
	public void writeShouldThrowExceptionForUpdateValueInMapNotAssignableToDomainTypeProperty() {

		exception.expect(MappingException.class);
		exception.expectMessage("java.lang.String cannot be assigned");
		exception.expectMessage(Person.class.getName());
		exception.expectMessage("relatives.[father]");

		PartialUpdate<Person> update = new PartialUpdate<Person>("123", Person.class) //
				.set("relatives", Collections.singletonMap("father", "buh buh the bear"));

		assertThat(BucketTester.from(write(update).getBucket())).without("_class");
	}

	private RedisData write(Object source) {

		RedisData rdo = new RedisData();
		converter.write(source, rdo);
		return rdo;
	}

	private <T> T read(Class<T> type, Map<String, String> source) {
		return converter.read(type, new RedisData(Bucket.newBucketFromStringMap(source)));
	}

	@WritingConverter
	static class AddressToBytesConverter implements Converter<Address, byte[]> {

		private final ObjectMapper mapper;
		private final Jackson2JsonRedisSerializer<Address> serializer;

		AddressToBytesConverter() {

			mapper = new ObjectMapper();
			mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
					.withFieldVisibility(Visibility.ANY).withGetterVisibility(Visibility.NONE)
					.withSetterVisibility(Visibility.NONE).withCreatorVisibility(Visibility.NONE));

			serializer = new Jackson2JsonRedisSerializer<Address>(Address.class);
			serializer.setObjectMapper(mapper);
		}

		@Override
		public byte[] convert(Address value) {
			return serializer.serialize(value);
		}
	}

	@WritingConverter
	static class SpeciesToMapConverter implements Converter<Species, Map<String, byte[]>> {

		@Override
		public Map<String, byte[]> convert(Species source) {

			if (source == null) {
				return null;
			}

			Map<String, byte[]> map = new LinkedHashMap<String, byte[]>();
			if (source.name != null) {
				map.put("species-name", source.name.getBytes(Charset.forName("UTF-8")));
			}
			map.put("species-nicknames",
					StringUtils.collectionToCommaDelimitedString(source.alsoKnownAs).getBytes(Charset.forName("UTF-8")));
			return map;
		}
	}

	@WritingConverter
	static class ListToByteConverter implements Converter<List, byte[]> {

		private final ObjectMapper mapper;
		private final Jackson2JsonRedisSerializer<List> serializer;

		ListToByteConverter() {

			mapper = new ObjectMapper();
			mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
					.withFieldVisibility(Visibility.ANY).withGetterVisibility(Visibility.NONE)
					.withSetterVisibility(Visibility.NONE).withCreatorVisibility(Visibility.NONE));

			serializer = new Jackson2JsonRedisSerializer<List>(List.class);
			serializer.setObjectMapper(mapper);
		}

		@Override
		public byte[] convert(List source) {

			if (source == null || source.isEmpty()) {
				return null;
			}

			return serializer.serialize(source);
		}
	}

	@ReadingConverter
	static class MapToSpeciesConverter implements Converter<Map<String, byte[]>, Species> {

		@Override
		public Species convert(Map<String, byte[]> source) {

			if (source == null || source.isEmpty()) {
				return null;
			}

			Species species = new Species();

			if (source.containsKey("species-name")) {
				species.name = new String(source.get("species-name"), Charset.forName("UTF-8"));
			}
			if (source.containsKey("species-nicknames")) {
				species.alsoKnownAs = Arrays.asList(StringUtils
						.commaDelimitedListToStringArray(new String(source.get("species-nicknames"), Charset.forName("UTF-8"))));
			}
			return species;
		}
	}

	@ReadingConverter
	static class BytesToAddressConverter implements Converter<byte[], Address> {

		private final ObjectMapper mapper;
		private final Jackson2JsonRedisSerializer<Address> serializer;

		BytesToAddressConverter() {

			mapper = new ObjectMapper();
			mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
					.withFieldVisibility(Visibility.ANY).withGetterVisibility(Visibility.NONE)
					.withSetterVisibility(Visibility.NONE).withCreatorVisibility(Visibility.NONE));

			serializer = new Jackson2JsonRedisSerializer<Address>(Address.class);
			serializer.setObjectMapper(mapper);
		}

		@Override
		public Address convert(byte[] value) {
			return serializer.deserialize(value);
		}
	}

}
