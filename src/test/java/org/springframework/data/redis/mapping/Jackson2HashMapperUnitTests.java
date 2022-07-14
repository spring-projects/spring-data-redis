/*
 * Copyright 2016-2022 the original author or authors.
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
package org.springframework.data.redis.mapping;

import static org.assertj.core.api.Assertions.*;

import lombok.Data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.Address;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.hash.Jackson2HashMapper;

/**
 * Unit tests for {@link Jackson2HashMapper}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
abstract class Jackson2HashMapperUnitTests extends AbstractHashMapperTests {

	private final Jackson2HashMapper mapper;

	Jackson2HashMapperUnitTests(Jackson2HashMapper mapper) {
		this.mapper = mapper;
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected <T> HashMapper mapperFor(Class<T> t) {
		return this.mapper;
	}

	@Test // DATAREDIS-423
	void shouldMapTypedListOfSimpleType() {

		WithList source = new WithList();
		source.strings = Arrays.asList("spring", "data", "redis");
		assertBackAndForwardMapping(source);
	}

	@Test // DATAREDIS-423
	void shouldMapTypedListOfComplexType() {

		WithList source = new WithList();

		source.persons = Arrays.asList(new Person("jon", "snow", 19), new Person("tyrion", "lannister", 27));
		assertBackAndForwardMapping(source);
	}

	@Test // DATAREDIS-423
	void shouldMapTypedListOfComplexObjectWihtNestedElements() {

		WithList source = new WithList();

		Person jon = new Person("jon", "snow", 19);
		Address adr = new Address();
		adr.setStreet("the wall");
		adr.setNumber(100);
		jon.setAddress(adr);

		source.persons = Arrays.asList(jon, new Person("tyrion", "lannister", 27));
		assertBackAndForwardMapping(source);
	}

	@Test // DATAREDIS-423
	void shouldMapNestedObject() {

		Person jon = new Person("jon", "snow", 19);
		Address adr = new Address();
		adr.setStreet("the wall");
		adr.setNumber(100);
		jon.setAddress(adr);

		assertBackAndForwardMapping(jon);
	}

	@Test // DATAREDIS-423
	void shouldMapUntypedList() {

		WithList source = new WithList();
		source.objects = Arrays.asList(100, "foo", new Person("jon", "snow", 19));
		assertBackAndForwardMapping(source);
	}

	@Test // DATAREDIS-423
	void shouldMapTypedMapOfSimpleTypes() {

		WithMap source = new WithMap();
		source.strings = new LinkedHashMap<>();
		source.strings.put("1", "spring");
		source.strings.put("2", "data");
		source.strings.put("3", "redis");
		assertBackAndForwardMapping(source);
	}

	@Test // DATAREDIS-423
	void shouldMapTypedMapOfComplexTypes() {

		WithMap source = new WithMap();
		source.persons = new LinkedHashMap<>();
		source.persons.put("1", new Person("jon", "snow", 19));
		source.persons.put("2", new Person("tyrion", "lannister", 19));
		assertBackAndForwardMapping(source);
	}

	@Test // DATAREDIS-423
	void shouldMapUntypedMap() {

		WithMap source = new WithMap();
		source.objects = new LinkedHashMap<>();
		source.objects.put("1", "spring");
		source.objects.put("2", 100);
		source.objects.put("3", "redis");
		assertBackAndForwardMapping(source);
	}

	@Test // DATAREDIS-423
	void nestedStuff() {

		WithList nestedList = new WithList();
		nestedList.objects = new ArrayList<>();

		WithMap deepNestedMap = new WithMap();
		deepNestedMap.persons = new LinkedHashMap<>();
		deepNestedMap.persons.put("jon", new Person("jon", "snow", 24));

		nestedList.objects.add(deepNestedMap);

		WithMap outer = new WithMap();
		outer.objects = new LinkedHashMap<>();
		outer.objects.put("1", nestedList);

		assertBackAndForwardMapping(outer);
	}

	@Test // DATAREDIS-1001
	void dateValueShouldBeTreatedCorrectly() {

		WithDates source = new WithDates();
		source.string = "id-1";
		source.date = new Date(1561543964015L);
		source.calendar = Calendar.getInstance();
		source.localDate = LocalDate.parse("2018-01-02");
		source.localDateTime = LocalDateTime.parse("2018-01-02T12:13:14");

		assertBackAndForwardMapping(source);
	}

	@Test // GH-2198
	void shouldDeserializeObjectWithoutClassHint() {

		WithDates source = new WithDates();
		source.string = "id-1";
		source.date = new Date(1561543964015L);
		source.calendar = Calendar.getInstance();
		source.localDate = LocalDate.parse("2018-01-02");
		source.localDateTime = LocalDateTime.parse("2018-01-02T12:13:14");

		Map<String, Object> map = mapper.toHash(source);

		// ensure that we remove the correct type hint
		assertThat(map.remove("@class")).isNotNull();

		assertThat(mapper.fromHash(WithDates.class, map)).isEqualTo(source);
	}

	@Test // GH-1566
	void mapFinalClass() {

		MeFinal source = new MeFinal();
		source.value = "id-1";

		assertBackAndForwardMapping(source);
	}

	@Test // GH-2365
	void bigIntegerShouldBeTreatedCorrectly() {

		WithBigWhatever source = new WithBigWhatever();
		source.bigI = BigInteger.TEN;

		assertBackAndForwardMapping(source);
	}

	@Test // GH-2365
	void bigDecimalShouldBeTreatedCorrectly() {

		WithBigWhatever source = new WithBigWhatever();
		source.bigD = BigDecimal.ONE;

		assertBackAndForwardMapping(source);
	}

	@Data
	public static class WithList {
		List<String> strings;
		List<Object> objects;
		List<Person> persons;
	}

	@Data
	public static class WithMap {
		Map<String, String> strings;
		Map<String, Object> objects;
		Map<String, Person> persons;
	}

	@Data
	private static class WithDates {

		private String string;
		private Date date;
		private Calendar calendar;
		private LocalDate localDate;
		private LocalDateTime localDateTime;
	}

	@Data
	private static class WithBigWhatever {
		private BigDecimal bigD;
		private BigInteger bigI;
	}

	@Data
	public static final class MeFinal {
		private String value;
	}
}
