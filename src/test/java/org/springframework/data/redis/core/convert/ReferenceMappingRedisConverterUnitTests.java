/*
 * Copyright 2015-2022 the original author or authors.
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
package org.springframework.data.redis.core.convert;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.redis.core.PartialUpdate;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration.KeyspaceSettings;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.test.util.RedisTestData;
import org.springframework.util.StringUtils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.when;
import static org.springframework.data.redis.core.convert.ConversionTestEntities.*;

/**
 * Unit tests for {@link MappingRedisConverter}.
 *
 * @author Christoph Strobl
 * @author Greg Turnquist
 * @author Mark Paluch
 * @author Golam Mazid Sajib
 * @author Ilya Viaznin
 */
@ExtendWith(MockitoExtension.class)
class ReferenceMappingRedisConverterUnitTests {

	@Mock ReferenceResolver resolverMock;

	private ReferenceMappingRedisConverter converter;

	private Person rand;

	@BeforeEach
	void setUp() {

		converter = new ReferenceMappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		converter.afterPropertiesSet();

		rand = new Person();
	}

	@Test // DATAREDIS-425
	void writeAppendsTypeHintForRootCorrectly() {
		assertThat(write(rand)).containingTypeHint("_class", Person.class);
	}

	@Test // DATAREDIS-543
	void writeSkipsTypeHintIfConfigured() {

		converter = new ReferenceMappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		converter.afterPropertiesSet();

		assertThat(write(rand)).containingTypeHint("_class", Person.class);
	}

	@Test // DATAREDIS-425
	void writeAppendsKeyCorrectly() {

		rand.id = "1";

		assertThat(write(rand).getId()).isEqualTo("1");
	}

	@Test // DATAREDIS-425
	void writeAppendsKeyCorrectlyWhenThereIsAnAdditionalIdFieldInNestedElement() {

		AddressWithId address = new AddressWithId();
		address.id = "tear";
		address.city = "Tear";

		rand.id = "1";
		rand.address = address;

		RedisTestData data = write(rand);

		assertThat(data.getId()).isEqualTo("1");
		assertThat(data).containsEntry("address.id", "tear");
	}

	@Test // DATAREDIS-425
	void writeDoesNotAppendPropertiesWithNullValues() {

		rand.firstname = "rand";

		assertThat(write(rand)).without("lastname");
	}

	@Test // DATAREDIS-425
	void writeDoesNotAppendPropertiesWithEmptyCollections() {

		rand.firstname = "rand";

		assertThat(write(rand)).without("nicknames");
	}

	@Test // DATAREDIS-425
	void writeAppendsSimpleRootPropertyCorrectly() {

		rand.firstname = "nynaeve";

		assertThat(write(rand)).containsEntry("firstname", "nynaeve");
	}

	@Test // DATAREDIS-425
	void writeAppendsListOfSimplePropertiesCorrectly() {

		rand.nicknames = Arrays.asList("dragon reborn", "lews therin");

		RedisTestData target = write(rand);

		assertThat(target).containsEntry("nicknames.[0]", "dragon reborn").containsEntry("nicknames.[1]", "lews therin");
	}

	@Test // DATAREDIS-425
	void writeAppendsComplexObjectCorrectly() {

		Address address = new Address();
		address.city = "two rivers";
		address.country = "andora";
		rand.address = address;

		RedisTestData target = write(rand);

		assertThat(target).containsEntry("address.city", "two rivers").containsEntry("address.country", "andora");
	}

	@Test // DATAREDIS-425
	void writeAppendsListOfComplexObjectsCorrectly() {

		Person mat = new Person();
		mat.firstname = "mat";
		mat.nicknames = Collections.singletonList("prince of the ravens");

		Person perrin = new Person();
		perrin.firstname = "perrin";
		perrin.address = new Address();
		perrin.address.city = "two rivers";

		rand.coworkers = Arrays.asList(mat, perrin);
		rand.id = UUID.randomUUID().toString();
		rand.firstname = "rand";

		RedisTestData target = write(rand);

		assertThat(target).containsEntry("coworkers.[0].firstname", "mat") //
				.containsEntry("coworkers.[0].nicknames.[0]", "prince of the ravens") //
				.containsEntry("coworkers.[1].firstname", "perrin") //
				.containsEntry("coworkers.[1].address.city", "two rivers");
	}

	@Test // DATAREDIS-425
	void writeDoesNotAddTypeInformationCorrectlyForMatchingTypes() {

		Address address = new Address();
		address.city = "two rivers";

		rand.address = address;

		RedisTestData target = write(rand);

		assertThat(target).without("address._class");
	}

	@Test // DATAREDIS-425, DATAREDIS-543
	void writeAddsTypeInformationCorrectlyForNonMatchingTypes() {

		AddressWithPostcode address = new AddressWithPostcode();
		address.city = "two rivers";
		address.postcode = "1234";

		rand.address = address;

		RedisTestData target = write(rand);

		assertThat(target).containsEntry("address._class", "with-post-code");
	}

	@Test // DATAREDIS-425
	void readConsidersTypeInformationCorrectlyForNonMatchingTypes() {

		Map<String, String> map = new HashMap<>();
		map.put("address._class", AddressWithPostcode.class.getName());
		map.put("address.postcode", "1234");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.address).isInstanceOf(AddressWithPostcode.class);
	}

	@Test // DATAREDIS-544
	void readEntityViaConstructor() {

		Map<String, String> map = new HashMap<>();
		map.put("id", "bart");
		map.put("firstname", "Bart");
		map.put("lastname", "Simpson");

		map.put("father.id", "homer");
		map.put("father.firstname", "Homer");
		map.put("father.lastname", "Simpson");

		RecursiveConstructorPerson target = converter.read(RecursiveConstructorPerson.class,
				new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.id).isEqualTo("bart");
		assertThat(target.firstname).isEqualTo("Bart");
		assertThat(target.lastname).isEqualTo("Simpson");
		assertThat(target.father).isNotNull();
		assertThat(target.father.id).isEqualTo("homer");
		assertThat(target.father.firstname).isEqualTo("Homer");
		assertThat(target.father.lastname).isEqualTo("Simpson");
		assertThat(target.father.father).isNull();
	}

	@Test // DATAREDIS-425
	void writeAddsTypeInformationCorrectlyForNonMatchingTypesInCollections() {

		Person mat = new TaVeren();
		mat.firstname = "mat";

		rand.coworkers = Collections.singletonList(mat);

		RedisTestData target = write(rand);

		assertThat(target).containingTypeHint("coworkers.[0]._class", TaVeren.class);
	}

	@Test // DATAREDIS-425
	void readConvertsSimplePropertiesCorrectly() {

		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("firstname", "rand")));

		assertThat(converter.read(Person.class, rdo).firstname).isEqualTo("rand");
	}

	@Test // DATAREDIS-425
	void readConvertsListOfSimplePropertiesCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("nicknames.[0]", "dragon reborn");
		map.put("nicknames.[1]", "lews therin");
		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		assertThat(converter.read(Person.class, rdo).nicknames).containsExactly("dragon reborn", "lews therin");
	}

	@Test // DATAREDIS-425
	void readConvertsUnorderedListOfSimplePropertiesCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("nicknames.[9]", "car'a'carn");
		map.put("nicknames.[10]", "lews therin");
		map.put("nicknames.[1]", "dragon reborn");
		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		assertThat(converter.read(Person.class, rdo).nicknames).containsExactly("dragon reborn", "car'a'carn",
				"lews therin");
	}

	@Test // DATAREDIS-768
	void readConvertsUnorderedListOfSimpleIntegerPropertiesCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("positions.[9]", "0");
		map.put("positions.[10]", "1");
		map.put("positions.[1]", "2");
		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		assertThat(converter.read(Person.class, rdo).positions).containsExactly(2, 0, 1);
	}

	@Test // DATAREDIS-425
	void readComplexPropertyCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("address.city", "two rivers");
		map.put("address.country", "andor");
		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		Person target = converter.read(Person.class, rdo);

		assertThat(target.address).isNotNull();
		assertThat(target.address.city).isEqualTo("two rivers");
		assertThat(target.address.country).isEqualTo("andor");
	}

	@Test // DATAREDIS-425
	void readListComplexPropertyCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
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
	void readUnorderedListOfComplexPropertyCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
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
	void readListComplexPropertyCorrectlyAndConsidersTypeInformation() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("coworkers.[0]._class", TaVeren.class.getName());
		map.put("coworkers.[0].firstname", "mat");

		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		Person target = converter.read(Person.class, rdo);

		assertThat(target.coworkers).isNotNull();
		assertThat(target.coworkers.get(0)).isInstanceOf(TaVeren.class);
		assertThat(target.coworkers.get(0).firstname).isEqualTo("mat");
	}

	@Test // DATAREDIS-425
	void writeAppendsMapWithSimpleKeyCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("hair-color", "red");
		map.put("eye-color", "grey");

		rand.physicalAttributes = map;

		RedisTestData target = write(rand);

		assertThat(target).containsEntry("physicalAttributes.[hair-color]", "red") //
				.containsEntry("physicalAttributes.[eye-color]", "grey");
	}

	@Test // DATAREDIS-425
	void writeAppendsMapWithSimpleKeyOnNestedObjectCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("hair-color", "red");
		map.put("eye-color", "grey");

		rand.coworkers = new ArrayList<>();
		rand.coworkers.add(new Person());
		rand.coworkers.get(0).physicalAttributes = map;

		RedisTestData target = write(rand);

		assertThat(target).containsEntry("coworkers.[0].physicalAttributes.[hair-color]", "red") //
				.containsEntry("coworkers.[0].physicalAttributes.[eye-color]", "grey");
	}

	@Test // DATAREDIS-425
	void readSimpleMapValuesCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("physicalAttributes.[hair-color]", "red");
		map.put("physicalAttributes.[eye-color]", "grey");

		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		Person target = converter.read(Person.class, rdo);

		assertThat(target.physicalAttributes).isNotNull();
		assertThat(target.physicalAttributes.get("hair-color")).isEqualTo("red");
		assertThat(target.physicalAttributes.get("eye-color")).isEqualTo("grey");
	}

	@Test // DATAREDIS-768
	void readSimpleIntegerMapValuesCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("integerMapKeyMapping.[1]", "2");
		map.put("integerMapKeyMapping.[3]", "4");

		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		TypeWithMaps target = converter.read(TypeWithMaps.class, rdo);

		assertThat(target.integerMapKeyMapping).isNotNull();
		assertThat(target.integerMapKeyMapping.get(1)).isEqualTo(2);
		assertThat(target.integerMapKeyMapping.get(3)).isEqualTo(4);
	}

	@Test // DATAREDIS-768
	void readMapWithDecimalMapKeyCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("decimalMapKeyMapping.[1.7]", "2");
		map.put("decimalMapKeyMapping.[3.1]", "4");

		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		TypeWithMaps target = converter.read(TypeWithMaps.class, rdo);

		assertThat(target.decimalMapKeyMapping).isNotNull();
		assertThat(target.decimalMapKeyMapping.get(1.7D)).isEqualTo("2");
		assertThat(target.decimalMapKeyMapping.get(3.1D)).isEqualTo("4");
	}

	@Test // DATAREDIS-768
	void writeMapWithDecimalMapKeyCorrectly() {

		TypeWithMaps source = new TypeWithMaps();
		source.decimalMapKeyMapping = new LinkedHashMap<>();
		source.decimalMapKeyMapping.put(1.7D, "2");
		source.decimalMapKeyMapping.put(3.1D, "4");

		RedisTestData target = write(source);

		assertThat(target).containsEntry("decimalMapKeyMapping.[1.7]", "2") //
				.containsEntry("decimalMapKeyMapping.[3.1]", "4");
	}

	@Test // DATAREDIS-768
	void readMapWithDateMapKeyCorrectly() {

		Date judgmentDay = Date.from(Instant.parse("1979-08-29T12:00:00Z"));

		Map<String, String> map = new LinkedHashMap<>();
		map.put("dateMapKeyMapping.[" + judgmentDay.getTime() + "]", "skynet");

		RedisData rdo = new RedisData(Bucket.newBucketFromStringMap(map));

		TypeWithMaps target = converter.read(TypeWithMaps.class, rdo);

		assertThat(target.dateMapKeyMapping).isNotNull();
		assertThat(target.dateMapKeyMapping.get(judgmentDay)).isEqualTo("skynet");
	}

	@Test // DATAREDIS-768
	void writeMapWithDateMapKeyCorrectly() {

		Date judgmentDay = Date.from(Instant.parse("1979-08-29T12:00:00Z"));

		TypeWithMaps source = new TypeWithMaps();
		source.dateMapKeyMapping = Collections.singletonMap(judgmentDay, "skynet");

		assertThat(write(source)).containsEntry("dateMapKeyMapping.[" + judgmentDay.getTime() + "]", "skynet");
	}

	@Test // DATAREDIS-425
	void writeAppendsMapWithComplexObjectsCorrectly() {

		Map<String, Person> map = new LinkedHashMap<>();
		Person janduin = new Person();
		janduin.firstname = "janduin";
		map.put("father", janduin);
		Person tam = new Person();
		tam.firstname = "tam";
		map.put("step-father", tam);

		rand.relatives = map;

		RedisTestData target = write(rand);

		assertThat(target).containsEntry("relatives.[father].firstname", "janduin") //
				.containsEntry("relatives.[step-father].firstname", "tam");
	}

	@Test // DATAREDIS-425
	void readMapWithComplexObjectsCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("relatives.[father].firstname", "janduin");
		map.put("relatives.[step-father].firstname", "tam");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.relatives).isNotNull();
		assertThat(target.relatives.get("father")).isNotNull();
		assertThat(target.relatives.get("father").firstname).isEqualTo("janduin");
		assertThat(target.relatives.get("step-father")).isNotNull();
		assertThat(target.relatives.get("step-father").firstname).isEqualTo("tam");
	}

	@Test // DATAREDIS-768
	void readMapWithIntegerKeysAndComplexObjectsCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("favoredRelatives.[1].firstname", "janduin");
		map.put("favoredRelatives.[2].firstname", "tam");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.favoredRelatives).isNotNull();
		assertThat(target.favoredRelatives.get(1)).isNotNull();
		assertThat(target.favoredRelatives.get(1).firstname).isEqualTo("janduin");
		assertThat(target.favoredRelatives.get(2)).isNotNull();
		assertThat(target.favoredRelatives.get(2).firstname).isEqualTo("tam");
	}

	@Test // DATAREDIS-425
	void writeAppendsTypeInformationCorrectlyForMapWithComplexObjects() {

		Map<String, Person> map = new LinkedHashMap<>();
		Person lews = new TaVeren();
		lews.firstname = "lews";
		map.put("previous-incarnation", lews);

		rand.relatives = map;

		RedisTestData target = write(rand);

		assertThat(target).containingTypeHint("relatives.[previous-incarnation]._class", TaVeren.class);
	}

	@Test // DATAREDIS-425
	void readConsidersTypeInformationCorrectlyForMapWithComplexObjects() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("relatives.[previous-incarnation]._class", TaVeren.class.getName());
		map.put("relatives.[previous-incarnation].firstname", "lews");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.relatives.get("previous-incarnation")).isNotNull();
		assertThat(target.relatives.get("previous-incarnation")).isInstanceOf(TaVeren.class);
		assertThat(target.relatives.get("previous-incarnation").firstname).isEqualTo("lews");
	}

	@Test // DATAREDIS-425
	void writesIntegerValuesCorrectly() {

		rand.age = 20;

		assertThat(write(rand)).containsEntry("age", "20");
	}

	@Test // DATAREDIS-425
	void writesLocalDateTimeValuesCorrectly() {

		rand.localDateTime = LocalDateTime.parse("2016-02-19T10:18:01");

		assertThat(write(rand)).containsEntry("localDateTime", "2016-02-19T10:18:01");
	}

	@Test // DATAREDIS-425
	void readsLocalDateTimeValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("localDateTime", "2016-02-19T10:18:01"))));

		assertThat(target.localDateTime).isEqualTo(LocalDateTime.parse("2016-02-19T10:18:01"));
	}

	@Test // DATAREDIS-425
	void writesLocalDateValuesCorrectly() {

		rand.localDate = LocalDate.parse("2016-02-19");

		assertThat(write(rand)).containsEntry("localDate", "2016-02-19");
	}

	@Test // DATAREDIS-425
	void readsLocalDateValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("localDate", "2016-02-19"))));

		assertThat(target.localDate).isEqualTo(LocalDate.parse("2016-02-19"));
	}

	@Test // DATAREDIS-425
	void writesLocalTimeValuesCorrectly() {

		rand.localTime = LocalTime.parse("11:12:13");

		assertThat(write(rand)).containsEntry("localTime", "11:12:13");
	}

	@Test // DATAREDIS-425
	void readsLocalTimeValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("localTime", "11:12"))));

		assertThat(target.localTime).isEqualTo(LocalTime.parse("11:12:00"));
	}

	@Test // DATAREDIS-425
	void writesZonedDateTimeValuesCorrectly() {

		rand.zonedDateTime = ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]");

		assertThat(write(rand)).containsEntry("zonedDateTime", "2007-12-03T10:15:30+01:00[Europe/Paris]");
	}

	@Test // DATAREDIS-425
	void readsZonedDateTimeValuesCorrectly() {

		Person target = converter.read(Person.class, new RedisData(Bucket
				.newBucketFromStringMap(Collections.singletonMap("zonedDateTime", "2007-12-03T10:15:30+01:00[Europe/Paris]"))));

		assertThat(target.zonedDateTime).isEqualTo(ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]"));
	}

	@Test // DATAREDIS-425
	void writesInstantValuesCorrectly() {

		rand.instant = Instant.parse("2007-12-03T10:15:30.01Z");

		assertThat(write(rand)).containsEntry("instant", "2007-12-03T10:15:30.010Z");
	}

	@Test // DATAREDIS-425
	void readsInstantValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("instant", "2007-12-03T10:15:30.01Z"))));

		assertThat(target.instant).isEqualTo(Instant.parse("2007-12-03T10:15:30.01Z"));
	}

	@Test // DATAREDIS-425
	void writesZoneIdValuesCorrectly() {

		rand.zoneId = ZoneId.of("Europe/Paris");

		assertThat(write(rand)).containsEntry("zoneId", "Europe/Paris");
	}

	@Test // DATAREDIS-425, GH-2307
	void readsZoneIdValuesCorrectly() {

		Map<String, String> map = new HashMap<>();
		map.put("zoneId", "Europe/Paris");
		map.put("zoneId._class", "java.time.ZoneRegion");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.zoneId).isEqualTo(ZoneId.of("Europe/Paris"));
	}

	@Test // DATAREDIS-425
	void writesDurationValuesCorrectly() {

		rand.duration = Duration.parse("P2DT3H4M");

		assertThat(write(rand)).containsEntry("duration", "PT51H4M");
	}

	@Test // DATAREDIS-425
	void readsDurationValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("duration", "PT51H4M"))));

		assertThat(target.duration).isEqualTo(Duration.parse("P2DT3H4M"));
	}

	@Test // DATAREDIS-425
	void writesPeriodValuesCorrectly() {

		rand.period = Period.parse("P1Y2M25D");

		assertThat(write(rand)).containsEntry("period", "P1Y2M25D");
	}

	@Test // DATAREDIS-425
	void readsPeriodValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("period", "P1Y2M25D"))));

		assertThat(target.period).isEqualTo(Period.parse("P1Y2M25D"));
	}

	@Test // DATAREDIS-425, DATAREDIS-593
	void writesEnumValuesCorrectly() {

		rand.gender = Gender.FEMALE;

		assertThat(write(rand)).containsEntry("gender", "FEMALE");
	}

	@Test // DATAREDIS-425, DATAREDIS-593
	void readsEnumValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("gender", "FEMALE"))));

		assertThat(target.gender).isEqualTo(Gender.FEMALE);
	}

	@Test // DATAREDIS-425
	void writesBooleanValuesCorrectly() {

		rand.alive = Boolean.TRUE;

		assertThat(write(rand)).containsEntry("alive", "1");
	}

	@Test // DATAREDIS-425
	void readsBooleanValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("alive", "1"))));

		assertThat(target.alive).isEqualTo(Boolean.TRUE);
	}

	@Test // DATAREDIS-425
	void readsStringBooleanValuesCorrectly() {

		Person target = converter.read(Person.class,
				new RedisData(Bucket.newBucketFromStringMap(Collections.singletonMap("alive", "true"))));

		assertThat(target.alive).isEqualTo(Boolean.TRUE);
	}

	@Test // DATAREDIS-425
	void writesDateValuesCorrectly() {

		Calendar cal = Calendar.getInstance();
		cal.set(1978, Calendar.NOVEMBER, 25);

		rand.birthdate = cal.getTime();

		assertThat(write(rand)).containsEntry("birthdate", rand.birthdate);
	}

	@Test // DATAREDIS-425
	void readsDateValuesCorrectly() {

		Calendar cal = Calendar.getInstance();
		cal.set(1978, Calendar.NOVEMBER, 25);

		Date date = cal.getTime();

		Person target = converter.read(Person.class, new RedisData(
				Bucket.newBucketFromStringMap(Collections.singletonMap("birthdate", Long.valueOf(date.getTime()).toString()))));

		assertThat(target.birthdate).isEqualTo(date);
	}

	@Test // DATAREDIS-425
	void writeSingleReferenceOnRootCorrectly() {

		Location location = new Location();
		location.id = "1";
		location.name = "tar valon";

		rand.location = location;

		RedisTestData target = write(rand);

		assertThat(target).containsEntry("location", "locations:1") //
				.without("location.id") //
				.without("location.name");
	}

	@Test // DATAREDIS-425
	void readLoadsReferenceDataOnRootCorrectly() {

		Location location = new Location();
		location.id = "1";
		location.name = "tar valon";

		Map<String, String> locationMap = new LinkedHashMap<>();
		locationMap.put("id", location.id);
		locationMap.put("name", location.name);

		when(resolverMock.resolveReference(eq("1"), eq("locations")))
				.thenReturn(Bucket.newBucketFromStringMap(locationMap).rawMap());

		Map<String, String> map = new LinkedHashMap<>();
		map.put("location", "locations:1");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.location).isEqualTo(location);
	}

	@Test // DATAREDIS-425
	void writeSingleReferenceOnNestedElementCorrectly() {

		Location location = new Location();
		location.id = "1";
		location.name = "tar valon";

		Person egwene = new Person();
		egwene.location = location;

		rand.coworkers = Collections.singletonList(egwene);

		assertThat(write(rand)).containsEntry("coworkers.[0].location", "locations:1") //
				.without("coworkers.[0].location.id") //
				.without("coworkers.[0].location.name");
	}

	@Test // DATAREDIS-425
	void readLoadsReferenceDataOnNestedElementCorrectly() {

		Location location = new Location();
		location.id = "1";
		location.name = "tar valon";

		Map<String, String> locationMap = new LinkedHashMap<>();
		locationMap.put("id", location.id);
		locationMap.put("name", location.name);

		when(resolverMock.resolveReference(eq("1"), eq("locations")))
				.thenReturn(Bucket.newBucketFromStringMap(locationMap).rawMap());

		Map<String, String> map = new LinkedHashMap<>();
		map.put("coworkers.[0].location", "locations:1");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.coworkers.get(0).location).isEqualTo(location);
	}

	@Test // DATAREDIS-425
	void writeListOfReferencesOnRootCorrectly() {

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

		RedisTestData target = write(rand);

		assertThat(target).containsEntry("visited.[0]", "locations:1") //
				.containsEntry("visited.[1]", "locations:2") //
				.containsEntry("visited.[2]", "locations:3");
	}

	@Test // DATAREDIS-425
	void readLoadsListOfReferencesOnRootCorrectly() {

		Location tarValon = new Location();
		tarValon.id = "1";
		tarValon.name = "tar valon";

		Location falme = new Location();
		falme.id = "2";
		falme.name = "falme";

		Location tear = new Location();
		tear.id = "3";
		tear.name = "city of tear";

		Map<String, String> tarValonMap = new LinkedHashMap<>();
		tarValonMap.put("id", tarValon.id);
		tarValonMap.put("name", tarValon.name);

		Map<String, String> falmeMap = new LinkedHashMap<>();
		falmeMap.put("id", falme.id);
		falmeMap.put("name", falme.name);

		Map<String, String> tearMap = new LinkedHashMap<>();
		tearMap.put("id", tear.id);
		tearMap.put("name", tear.name);

		Bucket.newBucketFromStringMap(tearMap).rawMap();

		when(resolverMock.resolveReference(eq("1"), eq("locations")))
				.thenReturn(Bucket.newBucketFromStringMap(tarValonMap).rawMap());
		when(resolverMock.resolveReference(eq("2"), eq("locations")))
				.thenReturn(Bucket.newBucketFromStringMap(falmeMap).rawMap());
		when(resolverMock.resolveReference(eq("3"), eq("locations")))
				.thenReturn(Bucket.newBucketFromStringMap(tearMap).rawMap());

		Map<String, String> map = new LinkedHashMap<>();
		map.put("visited.[0]", "locations:1");
		map.put("visited.[1]", "locations:2");
		map.put("visited.[2]", "locations:3");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.visited.get(0)).isEqualTo(tarValon);
		assertThat(target.visited.get(1)).isEqualTo(falme);
		assertThat(target.visited.get(2)).isEqualTo(tear);
	}

	@Test // DATAREDIS-425
	void writeSetsAnnotatedTimeToLiveCorrectly() {

		ExpiringPerson birgitte = new ExpiringPerson();
		birgitte.id = "birgitte";
		birgitte.name = "Birgitte Silverbow";

		assertThat(write(birgitte).getRedisData().getTimeToLive()).isEqualTo(5L);
	}

	@Test // DATAREDIS-425
	void writeDoesNotTTLWhenNotPresent() {

		Location tear = new Location();
		tear.id = "tear";
		tear.name = "Tear";

		assertThat(write(tear).getRedisData().getTimeToLive()).isNull();
	}

	@Test // DATAREDIS-425
	void writeShouldConsiderKeyspaceConfiguration() {

		this.converter.getMappingContext().getMappingConfiguration().getKeyspaceConfiguration()
				.addKeyspaceSettings(new KeyspaceSettings(Address.class, "o_O"));

		Address address = new Address();
		address.city = "Tear";

		assertThat(write(address).getRedisData().getKeyspace()).isEqualTo("o_O");
	}

	@Test // DATAREDIS-425
	void writeShouldConsiderTimeToLiveConfiguration() {

		KeyspaceSettings assignment = new KeyspaceSettings(Address.class, "o_O");
		assignment.setTimeToLive(5L);

		this.converter.getMappingContext().getMappingConfiguration().getKeyspaceConfiguration()
				.addKeyspaceSettings(assignment);

		Address address = new Address();
		address.city = "Tear";

		assertThat(write(address).getRedisData().getTimeToLive()).isEqualTo(5L);
	}

	@Test // DATAREDIS-425, DATAREDIS-634
	void writeShouldHonorCustomConversionOnRootType() {

		RedisCustomConversions customConversions = new RedisCustomConversions(
				Collections.singletonList(new AddressToBytesConverter()));

		RedisMappingContext mappingContext = new RedisMappingContext();
		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

		this.converter = new ReferenceMappingRedisConverter(mappingContext, null, resolverMock);
		this.converter.setCustomConversions(customConversions);
		this.converter.afterPropertiesSet();

		Address address = new Address();
		address.country = "Tel'aran'rhiod";
		address.city = "unknown";

		assertThat(write(address)).containsEntry("_raw", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}");
	}

	@Test // DATAREDIS-425, DATAREDIS-634
	void writeShouldHonorCustomConversionOnNestedType() {

		RedisCustomConversions customConversions = new RedisCustomConversions(
				Collections.singletonList(new AddressToBytesConverter()));

		RedisMappingContext mappingContext = new RedisMappingContext();
		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

		this.converter = new ReferenceMappingRedisConverter(mappingContext, null, resolverMock);
		this.converter.setCustomConversions(customConversions);
		this.converter.afterPropertiesSet();

		Address address = new Address();
		address.country = "Tel'aran'rhiod";
		address.city = "unknown";
		rand.address = address;

		assertThat(write(rand)).containsEntry("address", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}");
	}

	@Test // DATAREDIS-425
	void writeShouldHonorIndexOnCustomConversionForNestedType() {

		this.converter = new ReferenceMappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new RedisCustomConversions(Collections.singletonList(new AddressToBytesConverter())));
		this.converter.afterPropertiesSet();

		Address address = new Address();
		address.country = "andor";
		rand.address = address;

		assertThat(write(rand).getRedisData().getIndexedData())
				.contains(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "address.country", "andor"));
	}

	@Test // DATAREDIS-425
	void writeShouldHonorIndexAnnotationsOnWhenCustomConversionOnNestedype() {

		this.converter = new ReferenceMappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter
				.setCustomConversions(new RedisCustomConversions(Collections.singletonList(new AddressToBytesConverter())));
		this.converter.afterPropertiesSet();

		Address address = new Address();
		address.country = "Tel'aran'rhiod";
		address.city = "unknown";
		rand.address = address;

		assertThat(write(rand).getRedisData().getIndexedData().isEmpty()).isFalse();
	}

	@Test // DATAREDIS-425
	void readShouldHonorCustomConversionOnRootType() {

		this.converter = new ReferenceMappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new RedisCustomConversions(Collections.singletonList(new BytesToAddressConverter())));
		this.converter.afterPropertiesSet();

		Map<String, String> map = new LinkedHashMap<>();
		map.put("_raw", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}");

		Address target = converter.read(Address.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.city).isEqualTo("unknown");
		assertThat(target.country).isEqualTo("Tel'aran'rhiod");
	}

	@Test // DATAREDIS-425
	void readShouldHonorCustomConversionOnNestedType() {

		this.converter = new ReferenceMappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter
				.setCustomConversions(new RedisCustomConversions(Collections.singletonList(new BytesToAddressConverter())));
		this.converter.afterPropertiesSet();

		Map<String, String> map = new LinkedHashMap<>();
		map.put("address", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.address).isNotNull();
		assertThat(target.address.city).isEqualTo("unknown");
		assertThat(target.address.country).isEqualTo("Tel'aran'rhiod");
	}

	@Test // DATAREDIS-544
	void readShouldHonorCustomConversionOnNestedTypeViaConstructorCreation() {

		this.converter = new ReferenceMappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter
				.setCustomConversions(new RedisCustomConversions(Collections.singletonList(new BytesToAddressConverter())));
		this.converter.afterPropertiesSet();

		Map<String, String> map = new LinkedHashMap<>();
		map.put("address", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}");

		PersonWithConstructorAndAddress target = converter.read(PersonWithConstructorAndAddress.class,
				new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target.address).isNotNull();
		assertThat(target.address.city).isEqualTo("unknown");
		assertThat(target.address.country).isEqualTo("Tel'aran'rhiod");
	}

	@Test // DATAREDIS-425
	void writeShouldPickUpTimeToLiveFromPropertyIfPresent() {

		ExipringPersonWithExplicitProperty aviendha = new ExipringPersonWithExplicitProperty();
		aviendha.id = "aviendha";
		aviendha.ttl = 2L;

		assertThat(write(aviendha).getRedisData().getTimeToLive()).isEqualTo(120L);
	}

	@Test // DATAREDIS-425
	void writeShouldUseDefaultTimeToLiveIfPropertyIsPresentButNull() {

		ExipringPersonWithExplicitProperty aviendha = new ExipringPersonWithExplicitProperty();
		aviendha.id = "aviendha";

		assertThat(write(aviendha).getRedisData().getTimeToLive()).isEqualTo(5L);
	}

	@Test // DATAREDIS-425
	void writeShouldConsiderMapConvertersForRootType() {

		this.converter = new ReferenceMappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter
				.setCustomConversions(new RedisCustomConversions(Collections.singletonList(new SpeciesToMapConverter())));
		this.converter.afterPropertiesSet();

		Species myrddraal = new Species();
		myrddraal.name = "myrddraal";
		myrddraal.alsoKnownAs = Arrays.asList("halfmen", "fades", "neverborn");

		assertThat(write(myrddraal)).containsEntry("species-name", "myrddraal").containsEntry("species-nicknames",
				"halfmen,fades,neverborn");
	}

	@Test // DATAREDIS-425
	void writeShouldConsiderMapConvertersForNestedType() {

		this.converter = new ReferenceMappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new RedisCustomConversions(Collections.singletonList(new SpeciesToMapConverter())));
		this.converter.afterPropertiesSet();

		rand.species = new Species();
		rand.species.name = "human";

		assertThat(write(rand)).containsEntry("species.species-name", "human");
	}

	@Test // DATAREDIS-425
	void readShouldConsiderMapConvertersForRootType() {

		this.converter = new ReferenceMappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter
				.setCustomConversions(new RedisCustomConversions(Collections.singletonList(new MapToSpeciesConverter())));
		this.converter.afterPropertiesSet();
		Map<String, String> map = new LinkedHashMap<>();
		map.put("species-name", "trolloc");

		Species target = converter.read(Species.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target).isNotNull();
		assertThat(target.name).isEqualTo("trolloc");
	}

	@Test // DATAREDIS-425
	void readShouldConsiderMapConvertersForNestedType() {

		this.converter = new ReferenceMappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new RedisCustomConversions(Collections.singletonList(new MapToSpeciesConverter())));
		this.converter.afterPropertiesSet();

		Map<String, String> map = new LinkedHashMap<>();
		map.put("species.species-name", "trolloc");

		Person target = converter.read(Person.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target).isNotNull();
		assertThat(target.species.name).isEqualTo("trolloc");
	}

	@Test // DATAREDIS-425
	void writeShouldConsiderMapConvertersInsideLists() {

		this.converter = new ReferenceMappingRedisConverter(new RedisMappingContext(), null, resolverMock);
		this.converter
				.setCustomConversions(new RedisCustomConversions(Collections.singletonList(new SpeciesToMapConverter())));
		this.converter.afterPropertiesSet();

		TheWheelOfTime twot = new TheWheelOfTime();
		twot.species = new ArrayList<>();

		Species myrddraal = new Species();
		myrddraal.name = "myrddraal";
		myrddraal.alsoKnownAs = Arrays.asList("halfmen", "fades", "neverborn");
		twot.species.add(myrddraal);

		assertThat(write(twot)).containsEntry("species.[0].species-name", "myrddraal")
				.containsEntry("species.[0].species-nicknames", "halfmen,fades,neverborn");
	}

	@Test // DATAREDIS-425
	void readShouldConsiderMapConvertersForValuesInList() {

		this.converter = new ReferenceMappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new RedisCustomConversions(Collections.singletonList(new MapToSpeciesConverter())));
		this.converter.afterPropertiesSet();

		Map<String, String> map = new LinkedHashMap<>();
		map.put("species.[0].species-name", "trolloc");

		TheWheelOfTime target = converter.read(TheWheelOfTime.class, new RedisData(Bucket.newBucketFromStringMap(map)));

		assertThat(target).isNotNull();
		assertThat(target.species).isNotNull();
		assertThat(target.species.get(0)).isNotNull();
		assertThat(target.species.get(0).name).isEqualTo("trolloc");
	}

	@Test // DATAREDIS-492
	void writeHandlesArraysOfSimpleTypeProperly() {

		WithArrays source = new WithArrays();
		source.arrayOfSimpleTypes = new String[] { "rand", "mat", "perrin" };

		assertThat(write(source)).containsEntry("arrayOfSimpleTypes.[0]", "rand")
				.containsEntry("arrayOfSimpleTypes.[1]", "mat").containsEntry("arrayOfSimpleTypes.[2]", "perrin");
	}

	@Test // DATAREDIS-492
	void readHandlesArraysOfSimpleTypeProperly() {

		Map<String, String> source = new LinkedHashMap<>();
		source.put("arrayOfSimpleTypes.[0]", "rand");
		source.put("arrayOfSimpleTypes.[1]", "mat");
		source.put("arrayOfSimpleTypes.[2]", "perrin");

		WithArrays target = read(WithArrays.class, source);

		assertThat(target.arrayOfSimpleTypes).isEqualTo(new String[] { "rand", "mat", "perrin" });
	}

	@Test // GH-1981
	void readHandlesByteArrays() {

		Map<String, String> source = new LinkedHashMap<>();
		source.put("avatar", "foo-bar-baz");
		source.put("otherAvatar", "foo-bar-baz");

		WithArrays target = read(WithArrays.class, source);

		assertThat(target.avatar).isEqualTo("foo-bar-baz".getBytes());
	}

	@Test // GH-1981
	void writeHandlesByteArrays() {

		WithArrays withArrays = new WithArrays();
		withArrays.avatar = "foo-bar-baz".getBytes();

		assertThat(write(withArrays)).containsEntry("avatar", "foo-bar-baz");
	}

	@Test // GH-1981
	void readHandlesByteArraysUsingCollectionRepresentation() {

		Map<String, String> source = new LinkedHashMap<>();
		source.put("avatar.[0]", "102");
		source.put("avatar.[1]", "111");
		source.put("avatar.[2]", "111");

		WithArrays target = read(WithArrays.class, source);

		assertThat(target.avatar).isEqualTo("foo".getBytes());
	}

	@Test // DATAREDIS-492
	void writeHandlesArraysOfComplexTypeProperly() {

		WithArrays source = new WithArrays();

		Species trolloc = new Species();
		trolloc.name = "trolloc";

		Species myrddraal = new Species();
		myrddraal.name = "myrddraal";
		myrddraal.alsoKnownAs = Arrays.asList("halfmen", "fades", "neverborn");

		source.arrayOfCompexTypes = new Species[] { trolloc, myrddraal };

		assertThat(write(source)).containsEntry("arrayOfCompexTypes.[0].name", "trolloc") //
				.containsEntry("arrayOfCompexTypes.[1].name", "myrddraal") //
				.containsEntry("arrayOfCompexTypes.[1].alsoKnownAs.[0]", "halfmen") //
				.containsEntry("arrayOfCompexTypes.[1].alsoKnownAs.[1]", "fades") //
				.containsEntry("arrayOfCompexTypes.[1].alsoKnownAs.[2]", "neverborn");
	}

	@Test // DATAREDIS-492
	void readHandlesArraysOfComplexTypeProperly() {

		Map<String, String> source = new LinkedHashMap<>();
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
		assertThat(target.arrayOfCompexTypes[1].alsoKnownAs).containsExactly("halfmen", "fades", "neverborn");
	}

	@Test // DATAREDIS-489
	void writeHandlesArraysOfObjectTypeProperly() {

		Species trolloc = new Species();
		trolloc.name = "trolloc";

		WithArrays source = new WithArrays();
		source.arrayOfObject = new Object[] { "rand", trolloc, 100L };

		assertThat(write(source)).containsEntry("arrayOfObject.[0]", "rand") //
				.containsEntry("arrayOfObject.[0]._class", "java.lang.String")
				.containsEntry("arrayOfObject.[1]._class", Species.class.getName()) //
				.containsEntry("arrayOfObject.[1].name", "trolloc") //
				.containsEntry("arrayOfObject.[2]._class", "java.lang.Long") //
				.containsEntry("arrayOfObject.[2]", "100");
	}

	@Test // DATAREDIS-489
	void readHandlesArraysOfObjectTypeProperly() {

		Map<String, String> source = new LinkedHashMap<>();
		source.put("arrayOfObject.[0]", "rand");
		source.put("arrayOfObject.[0]._class", "java.lang.String");
		source.put("arrayOfObject.[1]._class", Species.class.getName());
		source.put("arrayOfObject.[1].name", "trolloc");
		source.put("arrayOfObject.[2]._class", "java.lang.Long");
		source.put("arrayOfObject.[2]", "100");

		WithArrays target = read(WithArrays.class, source);

		assertThat(target.arrayOfObject[0]).isNotNull();
		assertThat(target.arrayOfObject[0]).isInstanceOf(String.class);
		assertThat(target.arrayOfObject[1]).isNotNull();
		assertThat(target.arrayOfObject[1]).isInstanceOf(Species.class);
		assertThat(target.arrayOfObject[2]).isNotNull();
		assertThat(target.arrayOfObject[2]).isInstanceOf(Long.class);
	}

	@Test // DATAREDIS-489
	void writeShouldAppendTyeHintToObjectPropertyValueTypesCorrectly() {

		TypeWithObjectValueTypes sample = new TypeWithObjectValueTypes();
		sample.object = "bar";

		RedisTestData bucket = write(sample);

		assertThat(bucket).containsEntry("object", "bar").containsEntry("object._class", "java.lang.String");
	}

	@Test // DATAREDIS-489
	void shouldWriteReadObjectPropertyValueTypeCorrectly() {

		TypeWithObjectValueTypes di = new TypeWithObjectValueTypes();
		di.object = "foo";

		RedisTestData rd = write(di);

		TypeWithObjectValueTypes result = converter.read(TypeWithObjectValueTypes.class, rd.getRedisData());
		assertThat(result.object).isInstanceOf(String.class);
	}

	@Test // DATAREDIS-489
	void writeShouldAppendTyeHintToObjectMapValueTypesCorrectly() {

		TypeWithObjectValueTypes sample = new TypeWithObjectValueTypes();
		sample.map.put("string", "bar");
		sample.map.put("long", 1L);
		sample.map.put("date", new Date());

		RedisTestData bucket = write(sample);

		assertThat(bucket).containsEntry("map.[string]", "bar").containsEntry("map.[string]._class", "java.lang.String");
		assertThat(bucket).containsEntry("map.[long]", "1").containsEntry("map.[long]._class", "java.lang.Long");
		assertThat(bucket).containsEntry("map.[date]._class", "java.util.Date");
	}

	@Test // DATAREDIS-489
	void shouldWriteReadObjectMapValueTypeCorrectly() {

		TypeWithObjectValueTypes sample = new TypeWithObjectValueTypes();
		sample.map.put("string", "bar");
		sample.map.put("long", 1L);
		sample.map.put("date", new Date());

		RedisTestData rd = write(sample);

		TypeWithObjectValueTypes result = converter.read(TypeWithObjectValueTypes.class, rd.getRedisData());
		assertThat(result.map.get("string")).isInstanceOf(String.class);
		assertThat(result.map.get("long")).isInstanceOf(Long.class);
		assertThat(result.map.get("date")).isInstanceOf(Date.class);
	}

	@Test // DATAREDIS-489
	void writeShouldAppendTyeHintToObjectListValueTypesCorrectly() {

		TypeWithObjectValueTypes sample = new TypeWithObjectValueTypes();
		sample.list.add("string");
		sample.list.add(1L);
		sample.list.add(new Date());

		RedisTestData bucket = write(sample);

		assertThat(bucket).containsEntry("list.[0]", "string").containsEntry("list.[0]._class", "java.lang.String");
		assertThat(bucket).containsEntry("list.[1]", "1").containsEntry("list.[1]._class", "java.lang.Long");
		assertThat(bucket).containsEntry("list.[2]._class", "java.util.Date");
	}

	@Test // DATAREDIS-489
	void shouldWriteReadObjectListValueTypeCorrectly() {

		TypeWithObjectValueTypes sample = new TypeWithObjectValueTypes();
		sample.list.add("string");
		sample.list.add(1L);
		sample.list.add(new Date());

		RedisTestData rd = write(sample);

		TypeWithObjectValueTypes result = converter.read(TypeWithObjectValueTypes.class, rd.getRedisData());
		assertThat(result.list.get(0)).isInstanceOf(String.class);
		assertThat(result.list.get(1)).isInstanceOf(Long.class);
		assertThat(result.list.get(2)).isInstanceOf(Date.class);
	}

	@Test // DATAREDIS-909
	void shouldWriteReadObjectWithConstructorConversion() {

		Device sample = new Device(Instant.now(), Collections.singleton("foo"));

		RedisTestData rd = write(sample);

		Device result = converter.read(Device.class, rd.getRedisData());
		assertThat(result.now).isEqualTo(sample.now);
		assertThat(result.profiles).isEqualTo(sample.profiles);
	}

	@Test // DATAREDIS-509
	void writeHandlesArraysOfPrimitivesProperly() {

		Map<String, String> source = new LinkedHashMap<>();
		source.put("arrayOfPrimitives.[0]", "1");
		source.put("arrayOfPrimitives.[1]", "2");
		source.put("arrayOfPrimitives.[2]", "3");

		WithArrays target = read(WithArrays.class, source);

		assertThat(target.arrayOfPrimitives[0]).isEqualTo(1);
		assertThat(target.arrayOfPrimitives[1]).isEqualTo(2);
		assertThat(target.arrayOfPrimitives[2]).isEqualTo(3);
	}

	@Test // DATAREDIS-509
	void readHandlesArraysOfPrimitivesProperly() {

		WithArrays source = new WithArrays();
		source.arrayOfPrimitives = new int[] { 1, 2, 3 };
		assertThat(write(source)).containsEntry("arrayOfPrimitives.[0]", "1").containsEntry("arrayOfPrimitives.[1]", "2")
				.containsEntry("arrayOfPrimitives.[2]", "3");
	}

	@Test // DATAREDIS-471
	void writeShouldNotAppendClassTypeHint() {

		Person value = new Person();
		value.firstname = "rand";
		value.age = 24;

		PartialUpdate<Person> update = new PartialUpdate<>("123", value);

		assertThat(write(update).getBucket().get("_class")).isNull();
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdateSimpleValueCorrectly() {

		Person value = new Person();
		value.firstname = "rand";
		value.age = 24;

		PartialUpdate<Person> update = new PartialUpdate<>("123", value);

		assertThat(write(update)).containsEntry("firstname", "rand").containsEntry("age", "24");
	}

	@Test // GH-1981
	void writeShouldWritePartialUpdateFromEntityByteArrayValueCorrectly() {

		WithArrays value = new WithArrays();
		value.avatar = "foo-bar-baz".getBytes();

		PartialUpdate<WithArrays> update = new PartialUpdate<>("123", value);

		assertThat(write(update)).containsEntry("avatar", "foo-bar-baz");
	}

	@Test // GH-1981
	void writeShouldWritePartialUpdateFromSetByteArrayValueCorrectly() {

		PartialUpdate<WithArrays> update = PartialUpdate.newPartialUpdate(42, WithArrays.class).set("avatar",
				"foo-bar-baz".getBytes());

		assertThat(write(update)).containsEntry("avatar", "foo-bar-baz");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithSimpleValueCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("firstname", "rand").set("age", 24);

		assertThat(write(update)).containsEntry("firstname", "rand").containsEntry("age", "24");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdateNestedPathWithSimpleValueCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("address.city", "two rivers");

		assertThat(write(update)).containsEntry("address.city", "two rivers");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithComplexValueCorrectly() {

		Address address = new Address();
		address.city = "two rivers";
		address.country = "andor";

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("address", address);

		assertThat(write(update)).containsEntry("address.city", "two rivers").containsEntry("address.country", "andor");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithSimpleListValueCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("nicknames",
				Arrays.asList("dragon", "lews"));

		assertThat(write(update)).containsEntry("nicknames.[0]", "dragon").containsEntry("nicknames.[1]", "lews");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithComplexListValueCorrectly() {

		Person mat = new Person();
		mat.firstname = "mat";
		mat.age = 24;

		Person perrin = new Person();
		perrin.firstname = "perrin";

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("coworkers",
				Arrays.asList(mat, perrin));

		assertThat(write(update)).containsEntry("coworkers.[0].firstname", "mat").containsEntry("coworkers.[0].age", "24")
				.containsEntry("coworkers.[1].firstname", "perrin");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithSimpleListValueWhenNotPassedInAsCollectionCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("nicknames", "dragon");

		assertThat(write(update)).containsEntry("nicknames.[0]", "dragon");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithComplexListValueWhenNotPassedInAsCollectionCorrectly() {

		Person mat = new Person();
		mat.firstname = "mat";
		mat.age = 24;

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("coworkers", mat);

		assertThat(write(update)).containsEntry("coworkers.[0].firstname", "mat").containsEntry("coworkers.[0].age", "24");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithSimpleListValueWhenNotPassedInAsCollectionWithPositionalParameterCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("nicknames.[5]", "dragon");

		assertThat(write(update)).containsEntry("nicknames.[5]", "dragon");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithComplexListValueWhenNotPassedInAsCollectionWithPositionalParameterCorrectly() {

		Person mat = new Person();
		mat.firstname = "mat";
		mat.age = 24;

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("coworkers.[5]", mat);

		assertThat(write(update)).containsEntry("coworkers.[5].firstname", "mat").containsEntry("coworkers.[5].age", "24");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithSimpleMapValueCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("physicalAttributes",
				Collections.singletonMap("eye-color", "grey"));

		assertThat(write(update)).containsEntry("physicalAttributes.[eye-color]", "grey");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithComplexMapValueCorrectly() {

		Person tam = new Person();
		tam.firstname = "tam";
		tam.alive = false;

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("relatives",
				Collections.singletonMap("father", tam));

		assertThat(write(update)).containsEntry("relatives.[father].firstname", "tam")
				.containsEntry("relatives.[father].alive", "0");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithSimpleMapValueWhenNotPassedInAsCollectionCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("physicalAttributes",
				Collections.singletonMap("eye-color", "grey").entrySet().iterator().next());

		assertThat(write(update)).containsEntry("physicalAttributes.[eye-color]", "grey");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithComplexMapValueWhenNotPassedInAsCollectionCorrectly() {

		Person tam = new Person();
		tam.firstname = "tam";
		tam.alive = false;

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("relatives",
				Collections.singletonMap("father", tam).entrySet().iterator().next());

		assertThat(write(update)).containsEntry("relatives.[father].firstname", "tam")
				.containsEntry("relatives.[father].alive", "0");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithSimpleMapValueWhenNotPassedInAsCollectionWithPositionalParameterCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("physicalAttributes.[eye-color]",
				"grey");

		assertThat(write(update)).containsEntry("physicalAttributes.[eye-color]", "grey");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithSimpleMapValueOnNestedElementCorrectly() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("relatives.[father].firstname", "tam");

		assertThat(write(update)).containsEntry("relatives.[father].firstname", "tam");
	}

	@Test // DATAREDIS-471
	void writeShouldThrowExceptionOnPartialUpdatePathWithSimpleMapValueWhenItsASingleValueWithoutPath() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("physicalAttributes", "grey");

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> write(update));
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithRegisteredCustomConversionCorrectly() {

		this.converter = new ReferenceMappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new RedisCustomConversions(Collections.singletonList(new AddressToBytesConverter())));
		this.converter.afterPropertiesSet();

		Address address = new Address();
		address.country = "Tel'aran'rhiod";
		address.city = "unknown";

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("address", address);

		assertThat(write(update)).containsEntry("address", "{\"city\":\"unknown\",\"country\":\"Tel'aran'rhiod\"}");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithReferenceCorrectly() {

		Location tar = new Location();
		tar.id = "1";
		tar.name = "tar valon";

		Location tear = new Location();
		tear.id = "2";
		tear.name = "city of tear";

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class).set("visited", Arrays.asList(tar, tear));

		assertThat(write(update)).containsEntry("visited.[0]", "locations:1").containsEntry("visited.[1]", "locations:2") //
				.without("visited.id") //
				.without("visited.name");
	}

	@Test // DATAREDIS-471
	void writeShouldWritePartialUpdatePathWithListOfReferencesCorrectly() {

		Location location = new Location();
		location.id = "1";
		location.name = "tar valon";

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class) //
				.set("location", location);

		assertThat(write(update)).containsEntry("location", "locations:1") //
				.without("location.id") //
				.without("location.name");
	}

	@Test // DATAREDIS-471
	void writeShouldThrowExceptionForUpdateValueNotAssignableToDomainTypeProperty() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class) //
				.set("age", "twenty-four");

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> write(update))
				.withMessageContaining("java.lang.String cannot be assigned");
	}

	@Test // DATAREDIS-471
	void writeShouldThrowExceptionForUpdateCollectionValueNotAssignableToDomainTypeProperty() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class) //
				.set("coworkers.[0]", "buh buh the bear");

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> write(update))
				.withMessageContaining("java.lang.String cannot be assigned").withMessageContaining(Person.class.getName())
				.withMessageContaining("coworkers.[0]");
	}

	@Test // DATAREDIS-471
	void writeShouldThrowExceptionForUpdateValueInCollectionNotAssignableToDomainTypeProperty() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class) //
				.set("coworkers", Collections.singletonList("foo"));

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> write(update))
				.withMessageContaining("java.lang.String cannot be assigned").withMessageContaining(Person.class.getName())
				.withMessageContaining("coworkers");
	}

	@Test // DATAREDIS-471
	void writeShouldThrowExceptionForUpdateMapValueNotAssignableToDomainTypeProperty() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class) //
				.set("relatives.[father]", "buh buh the bear");

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> write(update))
				.withMessageContaining("java.lang.String cannot be assigned").withMessageContaining(Person.class.getName())
				.withMessageContaining("relatives.[father]");
	}

	@Test // DATAREDIS-471
	void writeShouldThrowExceptionForUpdateValueInMapNotAssignableToDomainTypeProperty() {

		PartialUpdate<Person> update = new PartialUpdate<>("123", Person.class) //
				.set("relatives", Collections.singletonMap("father", "buh buh the bear"));

		assertThatExceptionOfType(MappingException.class).isThrownBy(() -> write(update))
				.withMessageContaining("java.lang.String cannot be assigned").withMessageContaining(Person.class.getName())
				.withMessageContaining("relatives.[father]");
	}

	@Test // DATAREDIS-875
	void shouldNotWriteTypeHintForPrimitveTypes() {

		Size source = new Size();
		source.height = 1;

		assertThat(write(source).getBucket().get("height._class")).isNull();
	}

	@Test // DATAREDIS-875
	void shouldReadPrimitveTypes() {

		Map<String, String> source = new LinkedHashMap<>();
		source.put("height", "1000");

		assertThat(read(Size.class, source).height).isEqualTo(1000);
	}

	@Test // DATAREDIS-925
	void readUUID() {

		UUID uuid = UUID.randomUUID();
		Map<String, String> source = new LinkedHashMap<>();
		source.put("uuid", uuid.toString());

		assertThat(read(JustSomeDifferentPropertyTypes.class, source).uuid).isEqualTo(uuid);
	}

	@Test // DATAREDIS-925
	void writeUUID() {

		JustSomeDifferentPropertyTypes source = new JustSomeDifferentPropertyTypes();
		source.uuid = UUID.randomUUID();

		assertThat(write(source)).containsEntry("uuid", source.uuid.toString());
	}

	@Test // DATAREDIS-955
	void readInnerListShouldNotInfluenceOuterWithSameName() {

		Map<String, String> source = new LinkedHashMap<>();
		source.put("inners.[0].values.[0]", "i-1");
		source.put("inners.[0].values.[1]", "i-2");
		source.put("values.[0]", "o-1");
		source.put("values.[1]", "o-2");

		Outer outer = read(Outer.class, source);

		assertThat(outer.values).isEqualTo(Arrays.asList("o-1", "o-2"));
		assertThat(outer.inners.get(0).values).isEqualTo(Arrays.asList("i-1", "i-2"));
	}

	@Test // DATAREDIS-955
	void readInnerListShouldNotInfluenceOuterWithSameNameWhenNull() {

		Map<String, String> source = new LinkedHashMap<>();
		source.put("inners.[0].values.[0]", "i-1");
		source.put("inners.[0].values.[1]", "i-2");

		Outer outer = read(Outer.class, source);

		assertThat(outer.values).isNull();
		assertThat(outer.inners.get(0).values).isEqualTo(Arrays.asList("i-1", "i-2"));
	}

	@Test // DATAREDIS-911
	void writeEntityWithCustomConverter() {

		this.converter = new ReferenceMappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new RedisCustomConversions(Collections.singletonList(new AccountInfoToBytesConverter())));
		this.converter.afterPropertiesSet();

		AccountInfo accountInfo = new AccountInfo();
		accountInfo.setId("ai-id-1");
		accountInfo.setAccount("123456");
		accountInfo.setAccountName("Inamur Rahman Sadid");

		assertThat(write(accountInfo).getRedisData().getId()).isEqualTo(accountInfo.getId());
	}

	@Test // DATAREDIS-911
	void readEntityWithCustomConverter() {

		this.converter = new ReferenceMappingRedisConverter(null, null, resolverMock);
		this.converter
				.setCustomConversions(new RedisCustomConversions(Collections.singletonList(new BytesToAccountInfoConverter())));
		this.converter.afterPropertiesSet();

		Bucket bucket = new Bucket();
		bucket.put("_raw", "ai-id-1|123456|Golam Mazid Sajib".getBytes(StandardCharsets.UTF_8));

		RedisData redisData = new RedisData(bucket);
		redisData.setKeyspace(KEYSPACE_ACCOUNT);
		redisData.setId("ai-id-1");

		AccountInfo target = converter.read(AccountInfo.class, redisData);

		assertThat(target.getAccount()).isEqualTo("123456");
		assertThat(target.getAccountName()).isEqualTo("Golam Mazid Sajib");
	}

	@Test // GH-2349
	void writeGenericEntity() {

		WithGenericEntity<User> generic = new WithGenericEntity<>();
		generic.entity = new User("hello");

		assertThat(write(generic)).hasSize(3) //
				.containsEntry("_class",
						"org.springframework.data.redis.core.convert.ReferenceMappingRedisConverterUnitTests$WithGenericEntity")
				.containsEntry("entity.name", "hello") //
				.containsEntry("entity._class",
						"org.springframework.data.redis.core.convert.ReferenceMappingRedisConverterUnitTests$User");
	}

	@Test // GH-2349
	void readGenericEntity() {

		Bucket bucket = new Bucket();
		bucket.put("entity.name", "hello".getBytes());
		bucket.put("entity._class",
				"org.springframework.data.redis.core.convert.ReferenceMappingRedisConverterUnitTests$User".getBytes());

		RedisData redisData = new RedisData(bucket);
		redisData.setKeyspace(KEYSPACE_ACCOUNT);
		redisData.setId("ai-id-1");

		WithGenericEntity<User> generic = converter.read(WithGenericEntity.class, redisData);

		assertThat(generic.entity).isNotNull();
		assertThat(generic.entity.name).isEqualTo("hello");
	}

	@Test // DATAREDIS-1175
	@EnabledOnJre(JRE.JAVA_8)
	// FIXME: https://github.com/spring-projects/spring-data-redis/issues/2168
	void writePlainList() {

		List<Object> source = Arrays.asList("Hello", "stream", "message", 100L);
		RedisTestData target = write(source);

		assertThat(target).containsEntry("[0]", "Hello") //
				.containsEntry("[1]", "stream") //
				.containsEntry("[2]", "message") //
				.containsEntry("[3]", "100");
	}

	@Test // DATAREDIS-1175
	void readPlainList() {

		Map<String, String> source = new LinkedHashMap<>();
		source.put("[0]._class", "java.lang.String");
		source.put("[0]", "Hello");
		source.put("[1]._class", "java.lang.String");
		source.put("[1]", "stream");
		source.put("[2]._class", "java.lang.String");
		source.put("[2]", "message");
		source.put("[3]._class", "java.lang.Long");
		source.put("[3]", "100");

		List target = read(List.class, source);

		assertThat(target).containsExactly("Hello", "stream", "message", 100L);
	}

	private RedisTestData write(Object source) {

		RedisData rdo = new RedisData();
		converter.write(source, rdo);
		return RedisTestData.from(rdo);
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

			serializer = new Jackson2JsonRedisSerializer<>(Address.class);
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

			Map<String, byte[]> map = new LinkedHashMap<>();
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

			serializer = new Jackson2JsonRedisSerializer<>(Address.class);
			serializer.setObjectMapper(mapper);
		}

		@Override
		public Address convert(byte[] value) {
			return serializer.deserialize(value);
		}
	}

	@WritingConverter
	static class AccountInfoToBytesConverter implements Converter<AccountInfo, byte[]> {

		@Override
		public byte[] convert(AccountInfo accountInfo) {
			StringBuilder resp = new StringBuilder();
			resp.append(accountInfo.getId()).append("|").append(accountInfo.getAccount()).append("|")
					.append(accountInfo.getAccountName());
			return resp.toString().getBytes(StandardCharsets.UTF_8);
		}
	}

	@ReadingConverter
	static class BytesToAccountInfoConverter implements Converter<byte[], AccountInfo> {

		@Override
		public AccountInfo convert(byte[] bytes) {
			String[] values = new String(bytes, StandardCharsets.UTF_8).split("\\|");
			AccountInfo accountInfo = new AccountInfo();
			accountInfo.setId(values[0]);
			accountInfo.setAccount(values[1]);
			accountInfo.setAccountName(values[2]);
			return accountInfo;
		}
	}

	static class WithGenericEntity<T> {
		T entity;
	}

	@AllArgsConstructor
	static class User {
		String name;
	}

}
