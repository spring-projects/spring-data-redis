/*
 * Copyright 2015-2018 the original author or authors.
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

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.TimeToLive;
import org.springframework.data.redis.core.index.Indexed;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ConversionTestEntities {

	static final String KEYSPACE_PERSON = "persons";
	static final String KEYSPACE_TWOT = "twot";
	static final String KEYSPACE_LOCATION = "locations";

	@RedisHash(KEYSPACE_PERSON)
	public static class Person {

		@Id String id;
		String firstname;
		Gender gender;

		List<String> nicknames;
		List<Person> coworkers;
		List<Integer> positions;
		Integer age;
		Boolean alive;
		Date birthdate;

		LocalDate localDate;
		LocalDateTime localDateTime;
		LocalTime localTime;
		Instant instant;
		ZonedDateTime zonedDateTime;
		ZoneId zoneId;
		Duration duration;
		Period period;

		Address address;

		Map<String, String> physicalAttributes;
		Map<String, Person> relatives;
		Map<Integer, Person> favoredRelatives;

		@Reference Location location;
		@Reference List<Location> visited;

		Species species;
	}

	@RedisHash(KEYSPACE_PERSON)
	@Data
	public static class RecursiveConstructorPerson {

		final @Id String id;
		final String firstname;
		final RecursiveConstructorPerson father;
		String lastname;
	}

	@RedisHash(KEYSPACE_PERSON)
	@Data
	public static class PersonWithConstructorAndAddress {

		final @Id String id;
		final Address address;
	}

	public static class PersonWithAddressReference extends Person {

		@Reference AddressWithId addressRef;
	}

	public static class Address {

		String city;
		@Indexed String country;
	}

	public static class AddressWithId extends Address {

		@Id String id;
	}

	public static enum Gender {
		MALE, FEMALE {

			@Override
			public String toString() {
				return "Superwoman";
			}
		}
	}

	@TypeAlias("with-post-code")
	public static class AddressWithPostcode extends Address {

		String postcode;
	}

	public static class TaVeren extends Person {

		Object feature;
		Map<String, Object> characteristics;
		List<Object> items;
	}

	@EqualsAndHashCode
	@RedisHash(KEYSPACE_LOCATION)
	public static class Location {

		@Id String id;
		String name;
		Address address;

	}

	@RedisHash(timeToLive = 5)
	public static class ExpiringPerson {

		@Id String id;
		String name;
	}

	public static class ExipringPersonWithExplicitProperty extends ExpiringPerson {

		@TimeToLive(unit = TimeUnit.MINUTES) Long ttl;
	}

	public static class Species {

		String name;
		List<String> alsoKnownAs;
	}

	@RedisHash(KEYSPACE_TWOT)
	public static class TheWheelOfTime {

		List<Person> mainCharacters;
		List<Species> species;
		Map<String, Location> places;
	}

	public static class Item {

		@Indexed String type;
		String description;
		Size size;
	}

	public static class Size {

		int width;
		int height;
		int length;
	}

	public static class WithArrays {

		Object[] arrayOfObject;
		String[] arrayOfSimpleTypes;
		Species[] arrayOfCompexTypes;
		int[] arrayOfPrimitives;
	}

	static class TypeWithObjectValueTypes {

		Object object;
		Map<String, Object> map = new HashMap<>();
		List<Object> list = new ArrayList<>();
	}

	static class TypeWithMaps {

		Map<Integer, Integer> integerMapKeyMapping;
		Map<Double, String> decimalMapKeyMapping;
		Map<Date, String> dateMapKeyMapping;
	}
}
