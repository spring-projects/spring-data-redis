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
package org.springframework.data.redis.core.convert;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsInstanceOf.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.core.convert.ConversionTestEntities.*;
import static org.springframework.data.redis.test.util.IsBucketMatcher.*;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.redis.core.convert.ConversionTestEntities.Address;
import org.springframework.data.redis.core.convert.ConversionTestEntities.AddressWithId;
import org.springframework.data.redis.core.convert.ConversionTestEntities.AddressWithPostcode;
import org.springframework.data.redis.core.convert.ConversionTestEntities.ExipringPersonWithExplicitProperty;
import org.springframework.data.redis.core.convert.ConversionTestEntities.ExpiringPerson;
import org.springframework.data.redis.core.convert.ConversionTestEntities.Gender;
import org.springframework.data.redis.core.convert.ConversionTestEntities.Location;
import org.springframework.data.redis.core.convert.ConversionTestEntities.Person;
import org.springframework.data.redis.core.convert.ConversionTestEntities.Species;
import org.springframework.data.redis.core.convert.ConversionTestEntities.TaVeren;
import org.springframework.data.redis.core.convert.ConversionTestEntities.TheWheelOfTime;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration.KeyspaceSettings;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Christoph Strobl
 */
@RunWith(MockitoJUnitRunner.class)
public class MappingRedisConverterUnitTests {

	@Mock ReferenceResolver resolverMock;
	MappingRedisConverter converter;
	Person rand;

	@Before
	public void setUp() {

		converter = new MappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		converter.afterPropertiesSet();

		rand = new Person();

	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeAppendsTypeHintForRootCorrectly() {
		assertThat(write(rand).getBucket(), isBucket().containingTypeHint("_class", Person.class));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeAppendsKeyCorrectly() {

		rand.id = "1";

		assertThat(write(rand).getId(), is((Serializable) "1"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeAppendsKeyCorrectlyWhenThereIsAnAdditionalIdFieldInNestedElement() {

		AddressWithId address = new AddressWithId();
		address.id = "tear";
		address.city = "Tear";

		rand.id = "1";
		rand.address = address;

		RedisData data = write(rand);

		assertThat(data.getId(), is((Serializable) "1"));
		assertThat(data.getBucket(), isBucket().containingUtf8String("address.id", "tear"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeDoesNotAppendPropertiesWithNullValues() {

		rand.firstname = "rand";

		assertThat(write(rand).getBucket(), isBucket().without("lastname"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeDoesNotAppendPropertiesWithEmtpyCollections() {

		rand.firstname = "rand";

		assertThat(write(rand).getBucket(), isBucket().without("nicknames"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeAppendsSimpleRootPropertyCorrectly() {

		rand.firstname = "nynaeve";

		assertThat(write(rand).getBucket(), isBucket().containingUtf8String("firstname", "nynaeve"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeAppendsListOfSimplePropertiesCorrectly() {

		rand.nicknames = Arrays.asList("dragon reborn", "lews therin");

		RedisData target = write(rand);

		assertThat(target.getBucket(), isBucket().containingUtf8String("nicknames.[0]", "dragon reborn")
				.containingUtf8String("nicknames.[1]", "lews therin"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeAppendsComplexObjectCorrectly() {

		Address address = new Address();
		address.city = "two rivers";
		address.country = "andora";
		rand.address = address;

		RedisData target = write(rand);

		assertThat(target.getBucket(), isBucket().containingUtf8String("address.city", "two rivers")
				.containingUtf8String("address.country", "andora"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
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

		assertThat(target.getBucket(),
				isBucket().containingUtf8String("coworkers.[0].firstname", "mat") //
						.containingUtf8String("coworkers.[0].nicknames.[0]", "prince of the ravens") //
						.containingUtf8String("coworkers.[1].firstname", "perrin") //
						.containingUtf8String("coworkers.[1].address.city", "two rivers"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeDoesNotAddClassTypeInformationCorrectlyForMatchingTypes() {

		Address address = new Address();
		address.city = "two rivers";

		rand.address = address;

		RedisData target = write(rand);

		assertThat(target.getBucket(), isBucket().without("address._class"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeAddsClassTypeInformationCorrectlyForNonMatchingTypes() {

		AddressWithPostcode address = new AddressWithPostcode();
		address.city = "two rivers";
		address.postcode = "1234";

		rand.address = address;

		RedisData target = write(rand);

		assertThat(target.getBucket(), isBucket().containingTypeHint("address._class", AddressWithPostcode.class));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readConsidersClassTypeInformationCorrectlyForNonMatchingTypes() {

		Map<String, String> map = new HashMap<String, String>();
		map.put("address._class", AddressWithPostcode.class.getName());
		map.put("address.postcode", "1234");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.address, instanceOf(AddressWithPostcode.class));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeAddsClassTypeInformationCorrectlyForNonMatchingTypesInCollections() {

		Person mat = new TaVeren();
		mat.firstname = "mat";

		rand.coworkers = Arrays.asList(mat);

		RedisData target = write(rand);

		assertThat(target.getBucket(), isBucket().containingTypeHint("coworkers.[0]._class", TaVeren.class));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readConvertsSimplePropertiesCorrectly() {

		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("firstname", "rand")));

		assertThat(converter.read(Person.class, rdo).firstname, is("rand"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readConvertsListOfSimplePropertiesCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("nicknames.[0]", "dragon reborn");
		map.put("nicknames.[1]", "lews therin");
		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		assertThat(converter.read(Person.class, rdo).nicknames, hasItems("dragon reborn", "lews therin"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readComplexPropertyCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("address.city", "two rivers");
		map.put("address.country", "andor");
		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		Person target = converter.read(Person.class, rdo);

		assertThat(target.address, notNullValue());
		assertThat(target.address.city, is("two rivers"));
		assertThat(target.address.country, is("andor"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readListComplexPropertyCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("coworkers.[0].firstname", "mat");
		map.put("coworkers.[0].nicknames.[0]", "prince of the ravens");
		map.put("coworkers.[1].firstname", "perrin");
		map.put("coworkers.[1].address.city", "two rivers");
		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		Person target = converter.read(Person.class, rdo);

		assertThat(target.coworkers, notNullValue());
		assertThat(target.coworkers.get(0).firstname, is("mat"));
		assertThat(target.coworkers.get(0).nicknames, notNullValue());
		assertThat(target.coworkers.get(0).nicknames.get(0), is("prince of the ravens"));

		assertThat(target.coworkers.get(1).firstname, is("perrin"));
		assertThat(target.coworkers.get(1).address.city, is("two rivers"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readListComplexPropertyCorrectlyAndConsidersClassTypeInformation() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("coworkers.[0]._class", TaVeren.class.getName());
		map.put("coworkers.[0].firstname", "mat");

		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		Person target = converter.read(Person.class, rdo);

		assertThat(target.coworkers, notNullValue());
		assertThat(target.coworkers.get(0), instanceOf(TaVeren.class));
		assertThat(target.coworkers.get(0).firstname, is("mat"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeAppendsMapWithSimpleKeyCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("hair-color", "red");
		map.put("eye-color", "grey");

		rand.physicalAttributes = map;

		RedisData target = write(rand);

		assertThat(target.getBucket(), isBucket().containingUtf8String("physicalAttributes.[hair-color]", "red") //
				.containingUtf8String("physicalAttributes.[eye-color]", "grey"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeAppendsMapWithSimpleKeyOnNestedObjectCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("hair-color", "red");
		map.put("eye-color", "grey");

		rand.coworkers = new ArrayList<Person>();
		rand.coworkers.add(new Person());
		rand.coworkers.get(0).physicalAttributes = map;

		RedisData target = write(rand);

		assertThat(target.getBucket(),
				isBucket().containingUtf8String("coworkers.[0].physicalAttributes.[hair-color]", "red") //
						.containingUtf8String("coworkers.[0].physicalAttributes.[eye-color]", "grey"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readSimpleMapValuesCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("physicalAttributes.[hair-color]", "red");
		map.put("physicalAttributes.[eye-color]", "grey");

		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		Person target = converter.read(Person.class, rdo);

		assertThat(target.physicalAttributes, notNullValue());
		assertThat(target.physicalAttributes.get("hair-color"), is("red"));
		assertThat(target.physicalAttributes.get("eye-color"), is("grey"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
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

		assertThat(target.getBucket(), isBucket().containingUtf8String("relatives.[father].firstname", "janduin") //
				.containingUtf8String("relatives.[step-father].firstname", "tam"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readMapWithComplexObjectsCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("relatives.[father].firstname", "janduin");
		map.put("relatives.[step-father].firstname", "tam");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.relatives, notNullValue());
		assertThat(target.relatives.get("father"), notNullValue());
		assertThat(target.relatives.get("father").firstname, is("janduin"));
		assertThat(target.relatives.get("step-father"), notNullValue());
		assertThat(target.relatives.get("step-father").firstname, is("tam"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeAppendsClassTypeInformationCorrectlyForMapWithComplexObjects() {

		Map<String, Person> map = new LinkedHashMap<String, Person>();
		Person lews = new TaVeren();
		lews.firstname = "lews";
		map.put("previous-incarnation", lews);

		rand.relatives = map;

		RedisData target = write(rand);

		assertThat(target.getBucket(),
				isBucket().containingTypeHint("relatives.[previous-incarnation]._class", TaVeren.class));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readConsidersClassTypeInformationCorrectlyForMapWithComplexObjects() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("relatives.[previous-incarnation]._class", TaVeren.class.getName());
		map.put("relatives.[previous-incarnation].firstname", "lews");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.relatives.get("previous-incarnation"), notNullValue());
		assertThat(target.relatives.get("previous-incarnation"), instanceOf(TaVeren.class));
		assertThat(target.relatives.get("previous-incarnation").firstname, is("lews"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writesIntegerValuesCorrectly() {

		rand.age = 20;

		assertThat(write(rand).getBucket(), isBucket().containingUtf8String("age", "20"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writesLocalDateTimeValuesCorrectly() {

		rand.localDateTime = LocalDateTime.parse("2016-02-19T10:18:01");

		assertThat(write(rand).getBucket(), isBucket().containingUtf8String("localDateTime", "2016-02-19T10:18:01"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readsLocalDateTimeValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("localDateTime", "2016-02-19T10:18:01"))));

		assertThat(target.localDateTime, is(LocalDateTime.parse("2016-02-19T10:18:01")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writesLocalDateValuesCorrectly() {

		rand.localDate = LocalDate.parse("2016-02-19");

		assertThat(write(rand).getBucket(), isBucket().containingUtf8String("localDate", "2016-02-19"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readsLocalDateValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("localDate", "2016-02-19"))));

		assertThat(target.localDate, is(LocalDate.parse("2016-02-19")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writesLocalTimeValuesCorrectly() {

		rand.localTime = LocalTime.parse("11:12:13");

		assertThat(write(rand).getBucket(), isBucket().containingUtf8String("localTime", "11:12:13"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readsLocalTimeValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("localTime", "11:12"))));

		assertThat(target.localTime, is(LocalTime.parse("11:12:00")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writesZonedDateTimeValuesCorrectly() {

		rand.zonedDateTime = ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]");

		assertThat(write(rand).getBucket(),
				isBucket().containingUtf8String("zonedDateTime", "2007-12-03T10:15:30+01:00[Europe/Paris]"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readsZonedDateTimeValuesCorrectly() {

		Person target = converter.read(Person.class, new RedisData(Bucket
				.newBucketFromStringMap(Collections.singletonMap("zonedDateTime", "2007-12-03T10:15:30+01:00[Europe/Paris]"))));

		assertThat(target.zonedDateTime, is(ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writesInstantValuesCorrectly() {

		rand.instant = Instant.parse("2007-12-03T10:15:30.01Z");

		assertThat(write(rand).getBucket(), isBucket().containingUtf8String("instant", "2007-12-03T10:15:30.010Z"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readsInstantValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("instant", "2007-12-03T10:15:30.01Z"))));

		assertThat(target.instant, is(Instant.parse("2007-12-03T10:15:30.01Z")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writesZoneIdValuesCorrectly() {

		rand.zoneId = ZoneId.of("Europe/Paris");

		assertThat(write(rand).getBucket(), isBucket().containingUtf8String("zoneId", "Europe/Paris"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readsZoneIdValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("zoneId", "Europe/Paris"))));

		assertThat(target.zoneId, is(ZoneId.of("Europe/Paris")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writesDurationValuesCorrectly() {

		rand.duration = Duration.parse("P2DT3H4M");

		assertThat(write(rand).getBucket(), isBucket().containingUtf8String("duration", "PT51H4M"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readsDurationValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("duration", "PT51H4M"))));

		assertThat(target.duration, is(Duration.parse("P2DT3H4M")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writesPeriodValuesCorrectly() {

		rand.period = Period.parse("P1Y2M25D");

		assertThat(write(rand).getBucket(), isBucket().containingUtf8String("period", "P1Y2M25D"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readsPeriodValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("period", "P1Y2M25D"))));

		assertThat(target.period, is(Period.parse("P1Y2M25D")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writesEnumValuesCorrectly() {

		rand.gender = Gender.MALE;

		assertThat(write(rand).getBucket(), isBucket().containingUtf8String("gender", "MALE"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readsEnumValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("gender", "MALE"))));

		assertThat(target.gender, is(Gender.MALE));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writesBooleanValuesCorrectly() {

		rand.alive = Boolean.TRUE;

		assertThat(write(rand).getBucket(), isBucket().containingUtf8String("alive", "1"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readsBooleanValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("alive", "1"))));

		assertThat(target.alive, is(Boolean.TRUE));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readsStringBooleanValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("alive", "true"))));

		assertThat(target.alive, is(Boolean.TRUE));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writesDateValuesCorrectly() {

		Calendar cal = Calendar.getInstance();
		cal.set(1978, 10, 25);

		rand.birthdate = cal.getTime();

		assertThat(write(rand).getBucket(), isBucket().containingDateAsMsec("birthdate", rand.birthdate));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readsDateValuesCorrectly() {

		Calendar cal = Calendar.getInstance();
		cal.set(1978, 10, 25);

		Date date = cal.getTime();

		Person target = converter.read(Person.class, new RedisData(
				Bucket.newBucketFromStringMap(Collections.singletonMap("birthdate", Long.valueOf(date.getTime()).toString()))));

		assertThat(target.birthdate, is(date));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeSingleReferenceOnRootCorrectly() {

		Location location = new Location();
		location.id = "1";
		location.name = "tar valon";

		rand.location = location;

		RedisData target = write(rand);

		assertThat(target.getBucket(),
				isBucket().containingUtf8String("location", "locations:1") //
						.without("location.id") //
						.without("location.name"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
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

		assertThat(target.location, is(location));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeSingleReferenceOnNestedElementCorrectly() {

		Location location = new Location();
		location.id = "1";
		location.name = "tar valon";

		Person egwene = new Person();
		egwene.location = location;

		rand.coworkers = Collections.singletonList(egwene);

		RedisData target = write(rand);

		assertThat(target.getBucket(),
				isBucket().containingUtf8String("coworkers.[0].location", "locations:1") //
						.without("coworkers.[0].location.id") //
						.without("coworkers.[0].location.name"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
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

		assertThat(target.coworkers.get(0).location, is(location));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
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

		assertThat(target.getBucket(),
				isBucket().containingUtf8String("visited.[0]", "locations:1") //
						.containingUtf8String("visited.[1]", "locations:2") //
						.containingUtf8String("visited.[2]", "locations:3"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
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

		assertThat(target.visited.get(0), is(tarValon));
		assertThat(target.visited.get(1), is(falme));
		assertThat(target.visited.get(2), is(tear));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeSetsAnnotatedTimeToLiveCorrectly() {

		ExpiringPerson birgitte = new ExpiringPerson();
		birgitte.id = "birgitte";
		birgitte.name = "Birgitte Silverbow";

		assertThat(write(birgitte).getTimeToLive(), is(5L));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeDoesNotTTLWhenNotPresent() {

		Location tear = new Location();
		tear.id = "tear";
		tear.name = "Tear";

		assertThat(write(tear).getTimeToLive(), nullValue());
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeShouldConsiderKeyspaceConfiguration() {

		this.converter.getMappingContext().getMappingConfiguration().getKeyspaceConfiguration()
				.addKeyspaceSettings(new KeyspaceSettings(Address.class, "o_O"));

		Address address = new Address();
		address.city = "Tear";

		assertThat(write(address).getKeyspace(), is("o_O"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeShouldConsiderTimeToLiveConfiguration() {

		KeyspaceSettings assignment = new KeyspaceSettings(Address.class, "o_O");
		assignment.setTimeToLive(5L);

		this.converter.getMappingContext().getMappingConfiguration().getKeyspaceConfiguration()
				.addKeyspaceSettings(assignment);

		Address address = new Address();
		address.city = "Tear";

		assertThat(write(address).getTimeToLive(), is(5L));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeShouldHonorCustomConversionOnRootType() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new CustomConversions(Collections.singletonList(new AddressToBytesConverter())));
		this.converter.afterPropertiesSet();

		Address address = new Address();
		address.country = "Tel'aran'rhiod";
		address.city = "unknown";

		assertThat(write(address).getBucket(),
				isBucket().containingUtf8String("_raw", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeShouldHonorCustomConversionOnNestedType() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new CustomConversions(Collections.singletonList(new AddressToBytesConverter())));
		this.converter.afterPropertiesSet();

		Address address = new Address();
		address.country = "Tel'aran'rhiod";
		address.city = "unknown";
		rand.address = address;

		assertThat(write(rand).getBucket(),
				isBucket().containingUtf8String("address", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeShouldHonorIndexOnCustomConversionForNestedType() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new CustomConversions(Collections.singletonList(new AddressToBytesConverter())));
		this.converter.afterPropertiesSet();

		Address address = new Address();
		address.country = "andor";
		rand.address = address;

		assertThat(write(rand).getIndexedData(),
				hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "address.country", "andor")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeShouldHonorIndexAnnotationsOnWhenCustomConversionOnNestedype() {

		this.converter = new MappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter
				.setCustomConversions(new CustomConversions(Collections.singletonList(new AddressToBytesConverter())));
		this.converter.afterPropertiesSet();

		Address address = new Address();
		address.country = "Tel'aran'rhiod";
		address.city = "unknown";
		rand.address = address;

		assertThat(write(rand).getIndexedData().isEmpty(), is(false));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readShouldHonorCustomConversionOnRootType() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new CustomConversions(Collections.singletonList(new BytesToAddressConverter())));
		this.converter.afterPropertiesSet();

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("_raw", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}");

		Address target = converter.read(Address.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.city, is("unknown"));
		assertThat(target.country, is("Tel'aran'rhiod"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readShouldHonorCustomConversionOnNestedType() {

		this.converter = new MappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter
				.setCustomConversions(new CustomConversions(Collections.singletonList(new BytesToAddressConverter())));
		this.converter.afterPropertiesSet();

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("address", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.address, notNullValue());
		assertThat(target.address.city, is("unknown"));
		assertThat(target.address.country, is("Tel'aran'rhiod"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeShouldPickUpTimeToLiveFromPropertyIfPresent() {

		ExipringPersonWithExplicitProperty aviendha = new ExipringPersonWithExplicitProperty();
		aviendha.id = "aviendha";
		aviendha.ttl = 2L;

		assertThat(write(aviendha).getTimeToLive(), is(120L));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeShouldUseDefaultTimeToLiveIfPropertyIsPresentButNull() {

		ExipringPersonWithExplicitProperty aviendha = new ExipringPersonWithExplicitProperty();
		aviendha.id = "aviendha";

		assertThat(write(aviendha).getTimeToLive(), is(5L));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeShouldConsiderMapConvertersForRootType() {

		this.converter = new MappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter.setCustomConversions(new CustomConversions(Collections.singletonList(new SpeciesToMapConverter())));
		this.converter.afterPropertiesSet();

		Species myrddraal = new Species();
		myrddraal.name = "myrddraal";
		myrddraal.alsoKnownAs = Arrays.asList("halfmen", "fades", "neverborn");

		assertThat(write(myrddraal).getBucket(), isBucket().containingUtf8String("species-name", "myrddraal")
				.containingUtf8String("species-nicknames", "halfmen,fades,neverborn"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void writeShouldConsiderMapConvertersForNestedType() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter.setCustomConversions(new CustomConversions(Collections.singletonList(new SpeciesToMapConverter())));
		this.converter.afterPropertiesSet();

		rand.species = new Species();
		rand.species.name = "human";

		assertThat(write(rand).getBucket(), isBucket().containingUtf8String("species.species-name", "human"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readShouldConsiderMapConvertersForRootType() {

		this.converter = new MappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter.setCustomConversions(new CustomConversions(Collections.singletonList(new MapToSpeciesConverter())));
		this.converter.afterPropertiesSet();
		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("species-name", "trolloc");

		Species target = converter.read(Species.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target, notNullValue());
		assertThat(target.name, is("trolloc"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readShouldConsiderMapConvertersForNestedType() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter.setCustomConversions(new CustomConversions(Collections.singletonList(new MapToSpeciesConverter())));
		this.converter.afterPropertiesSet();

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("species.species-name", "trolloc");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target, notNullValue());
		assertThat(target.species.name, is("trolloc"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
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

		assertThat(write(twot).getBucket(), isBucket().containingUtf8String("species.[0].species-name", "myrddraal")
				.containingUtf8String("species.[0].species-nicknames", "halfmen,fades,neverborn"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void readShouldConsiderMapConvertersForValuesInList() {

		this.converter = new MappingRedisConverter(null, null, resolverMock);
		this.converter.setCustomConversions(new CustomConversions(Collections.singletonList(new MapToSpeciesConverter())));
		this.converter.afterPropertiesSet();

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("species.[0].species-name", "trolloc");

		TheWheelOfTime target = converter.read(TheWheelOfTime.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target, notNullValue());
		assertThat(target.species, notNullValue());
		assertThat(target.species.get(0), notNullValue());
		assertThat(target.species.get(0).name, is("trolloc"));
	}

	private RedisData write(Object source) {

		RedisData rdo = new RedisData();
		converter.write(source, rdo);
		return rdo;
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
