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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.TimeToLive;
import org.springframework.data.redis.core.index.Indexed;

/**
 * @author Christoph Strobl
 */
class ConversionTestEntities {

	static final String KEYSPACE_PERSON = "persons";
	static final String KEYSPACE_TWOT = "twot";
	static final String KEYSPACE_LOCATION = "locations";

	@RedisHash(KEYSPACE_PERSON)
	static class Person {

		@Id String id;
		String firstname;
		Gender gender;

		List<String> nicknames;
		List<Person> coworkers;
		Integer age;
		Boolean alive;
		Date birthdate;

		Address address;

		Map<String, String> physicalAttributes;
		Map<String, Person> relatives;

		@Reference Location location;
		@Reference List<Location> visited;

		Species species;
	}

	static class PersonWithAddressReference extends Person {

		@Reference AddressWithId addressRef;
	}

	static class Address {

		String city;
		@Indexed String country;
	}

	static class AddressWithId extends Address {

		@Id String id;
	}

	static enum Gender {
		MALE, FEMALE
	}

	static class AddressWithPostcode extends Address {

		String postcode;
	}

	static class TaVeren extends Person {

		Object feature;
		Map<String, Object> characteristics;
		List<Object> items;
	}

	@RedisHash(KEYSPACE_LOCATION)
	static class Location {

		@Id String id;
		String name;
		Address address;
	}

	@RedisHash(timeToLive = 5)
	static class ExpiringPerson {

		@Id String id;
		String name;
	}

	static class ExipringPersonWithExplicitProperty extends ExpiringPerson {

		@TimeToLive(unit = TimeUnit.MINUTES) Long ttl;
	}

	static class Species {

		String name;
		List<String> alsoKnownAs;
	}

	@RedisHash(KEYSPACE_TWOT)
	static class TheWheelOfTime {

		List<Person> mainCharacters;
		List<Species> species;
		Map<String, Location> places;
	}

	static class Item {

		@Indexed String type;
		String description;
		Size size;
	}

	static class Size {

		int width;
		int height;
		int length;
	}

}
