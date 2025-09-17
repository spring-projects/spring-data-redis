/*
 * Copyright 2025 the original author or authors.
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

import tools.jackson.core.json.JsonReadFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;

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
import java.util.Objects;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.Address;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.hash.JacksonHashMapper;

/**
 * Support class for {@link JacksonHashMapper} unit tests.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 */
abstract class JacksonHashMapperUnitTests extends AbstractHashMapperTests {

	private final JacksonHashMapper mapper;

	JacksonHashMapperUnitTests(JacksonHashMapper mapper) {

		this.mapper = mapper;
	}

	JacksonHashMapper getMapper() {
		return this.mapper;
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected <T> HashMapper mapperFor(Class<T> t) {
		return getMapper();
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

	@Test // GH-2979
	void enumsShouldBeTreatedCorrectly() {

		WithEnumValue source = new WithEnumValue();
		source.value = SpringDataEnum.REDIS;

		assertBackAndForwardMapping(source);
	}

	@Test // GH-3292
	void configuresObjectMapper() {

		JacksonHashMapper serializer = JacksonHashMapper.builder(() -> new ObjectMapper().rebuild())
				.customize(mb -> mb.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)).build();

		assertThat(serializer).isNotNull();
	}

	@Test // GH-3292
	void configuresJsonMapper() {

		JacksonHashMapper serializer = JacksonHashMapper.create(b -> {
			b.customize(mb -> mb.enable(JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER));
		});

		assertThat(serializer).isNotNull();
	}

	public static class WithList {

		List<String> strings;
		List<Object> objects;
		List<Person> persons;

		public List<String> getStrings() {
			return this.strings;
		}

		public void setStrings(List<String> strings) {
			this.strings = strings;
		}

		public List<Object> getObjects() {
			return this.objects;
		}

		public void setObjects(List<Object> objects) {
			this.objects = objects;
		}

		public List<Person> getPersons() {
			return this.persons;
		}

		public void setPersons(List<Person> persons) {
			this.persons = persons;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof WithList that)) {
				return false;
			}

			return Objects.equals(this.getObjects(), that.getObjects())
					&& Objects.equals(this.getPersons(), that.getPersons())
					&& Objects.equals(this.getStrings(), that.getStrings());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getObjects(), getPersons(), getStrings());
		}
	}

	public static class WithMap {

		Map<String, String> strings;
		Map<String, Object> objects;
		Map<String, Person> persons;

		public Map<String, String> getStrings() {
			return this.strings;
		}

		public void setStrings(Map<String, String> strings) {
			this.strings = strings;
		}

		public Map<String, Object> getObjects() {
			return this.objects;
		}

		public void setObjects(Map<String, Object> objects) {
			this.objects = objects;
		}

		public Map<String, Person> getPersons() {
			return this.persons;
		}

		public void setPersons(Map<String, Person> persons) {
			this.persons = persons;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof WithMap that)) {
				return false;
			}

			return Objects.equals(this.getObjects(), that.getObjects())
					&& Objects.equals(this.getPersons(), that.getPersons())
					&& Objects.equals(this.getStrings(), that.getStrings());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getObjects(), getPersons(), getStrings());
		}
	}

	private static class WithDates {

		private String string;
		private Date date;
		private Calendar calendar;
		private LocalDate localDate;
		private LocalDateTime localDateTime;

		public String getString() {
			return this.string;
		}

		public void setString(String string) {
			this.string = string;
		}

		public Date getDate() {
			return this.date;
		}

		public void setDate(Date date) {
			this.date = date;
		}

		public Calendar getCalendar() {
			return this.calendar;
		}

		public void setCalendar(Calendar calendar) {
			this.calendar = calendar;
		}

		public LocalDate getLocalDate() {
			return this.localDate;
		}

		public void setLocalDate(LocalDate localDate) {
			this.localDate = localDate;
		}

		public LocalDateTime getLocalDateTime() {
			return this.localDateTime;
		}

		public void setLocalDateTime(LocalDateTime localDateTime) {
			this.localDateTime = localDateTime;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof WithDates that)) {
				return false;
			}

			return Objects.equals(this.getString(), that.getString())
					&& Objects.equals(this.getCalendar(), that.getCalendar()) && Objects.equals(this.getDate(), that.getDate())
					&& Objects.equals(this.getLocalDate(), that.getLocalDate())
					&& Objects.equals(this.getLocalDateTime(), that.getLocalDateTime());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getString(), getCalendar(), getDate(), getLocalDate(), getLocalDateTime());
		}

		@Override
		public String toString() {
			return "WithDates{" + "string='" + string + '\'' + ", date=" + date + ", calendar=" + calendar + ", localDate="
					+ localDate + ", localDateTime=" + localDateTime + '}';
		}
	}

	private static class WithBigWhatever {

		private BigDecimal bigD;
		private BigInteger bigI;

		public BigDecimal getBigD() {
			return this.bigD;
		}

		public void setBigD(BigDecimal bigD) {
			this.bigD = bigD;
		}

		public BigInteger getBigI() {
			return this.bigI;
		}

		public void setBigI(BigInteger bigI) {
			this.bigI = bigI;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof WithBigWhatever that)) {
				return false;
			}

			return Objects.equals(this.getBigD(), that.getBigD()) && Objects.equals(this.getBigI(), that.getBigI());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getBigD(), getBigI());
		}
	}

	public static final class MeFinal {

		private String value;

		public String getValue() {
			return this.value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof MeFinal that)) {
				return false;
			}

			return Objects.equals(this.getValue(), that.getValue());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getValue());
		}
	}

	enum SpringDataEnum {
		COMMONS, REDIS
	}

	static class WithEnumValue {

		SpringDataEnum value;

		public SpringDataEnum getValue() {
			return value;
		}

		public void setValue(SpringDataEnum value) {
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (o == this) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			WithEnumValue that = (WithEnumValue) o;
			return value == that.value;
		}

		@Override
		public int hashCode() {
			return Objects.hash(value);
		}
	}
}
