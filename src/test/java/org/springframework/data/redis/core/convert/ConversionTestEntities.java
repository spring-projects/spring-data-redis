/*
 * Copyright 2015-2023 the original author or authors.
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
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
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
 * @author Golam Mazid Sajib
 * @author John Blum
 */
public class ConversionTestEntities {

	static final String KEYSPACE_PERSON = "persons";
	static final String KEYSPACE_TWOT = "twot";
	static final String KEYSPACE_LOCATION = "locations";
	static final String KEYSPACE_ACCOUNT = "accounts";

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
	public static class RecursiveConstructorPerson {

		final @Id String id;

		final String firstname;

		String lastname;

		final RecursiveConstructorPerson father;

		public RecursiveConstructorPerson(String id, String firstname, RecursiveConstructorPerson father) {
			this.id = id;
			this.firstname = firstname;
			this.father = father;
		}

		public String getId() {
			return this.id;
		}

		public String getFirstname() {
			return this.firstname;
		}

		public RecursiveConstructorPerson getFather() {
			return this.father;
		}

		public String getLastname() {
			return this.lastname;
		}

		public void setLastname(String lastname) {
			this.lastname = lastname;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof RecursiveConstructorPerson that)) {
				return false;
			}

			return Objects.equals(this.getId(), that.getId())
				&& Objects.equals(this.getFirstname(), that.getFirstname())
				&& Objects.equals(this.getLastname(), that.getLastname())
				&& Objects.equals(this.getFather(), that.getFather());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getId(), getFirstname(), getLastname(), getFather());
		}

		@Override
		public String toString() {

			return "ConversionTestEntities.RecursiveConstructorPerson(id=" + this.getId()
				+ ", firstname=" + this.getFirstname()
				+ ", father=" + this.getFather()
				+ ", lastname=" + this.getLastname() + ")";
		}
	}

	@RedisHash(KEYSPACE_PERSON)
	public static class PersonWithConstructorAndAddress {

		final @Id String id;
		final Address address;

		public PersonWithConstructorAndAddress(String id, Address address) {
			this.id = id;
			this.address = address;
		}

		public String getId() {
			return this.id;
		}

		public Address getAddress() {
			return this.address;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof PersonWithConstructorAndAddress that)) {
				return false;
			}

			return Objects.equals(this.getId(), that.getId())
				&& Objects.equals(this.getAddress(), that.getAddress());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getId(), getAddress());
		}

		@Override
		public String toString() {
			return "ConversionTestEntities.PersonWithConstructorAndAddress(id=" + this.getId()
				+ ", address=" + this.getAddress() + ")";
		}
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

	public enum Gender {

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

	@RedisHash(KEYSPACE_LOCATION)
	public static class Location {

		@Id String id;
		String name;
		Address address;

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof Location that)) {
				return false;
			}

			return Objects.equals(this.id, that.id)
				&& Objects.equals(this.name, that.name)
				&& Objects.equals(this.address, that.address);
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.id, this.name, this.address);
		}
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
		byte[] avatar;
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

	static class Device {

		final Instant now;
		final Set<String> profiles;

		public Device(Instant now, Set<String> profiles) {
			this.now = now;
			this.profiles = profiles;
		}
	}

	public static class JustSomeDifferentPropertyTypes {

		UUID uuid;

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof JustSomeDifferentPropertyTypes that)) {
				return false;
			}

			return Objects.equals(this.uuid, that.uuid);
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.uuid);
		}

		@Override
		public String toString() {
			return "ConversionTestEntities.JustSomeDifferentPropertyTypes(uuid=" + this.uuid + ")";
		}
	}

	static class Outer {
		List<Inner> inners;
		List<String> values;
	}

	static class Inner {
		List<String> values;
	}

	@RedisHash(KEYSPACE_ACCOUNT)
	public static class AccountInfo {

		@Id private String id;
		private String account;
		private String accountName;

		public String getId() {
			return this.id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getAccount() {
			return this.account;
		}

		public void setAccount(String account) {
			this.account = account;
		}

		public String getAccountName() {
			return this.accountName;
		}

		public void setAccountName(String accountName) {
			this.accountName = accountName;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof AccountInfo that)) {
				return false;
			}

			return Objects.equals(this.getId(), that.getId())
				&& Objects.equals(this.getAccount(), that.getAccount())
				&& Objects.equals(this.getAccountName(), that.getAccountName());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getId(), getAccount(), getAccountName());
		}

		@Override
		public String toString() {
			return "ConversionTestEntities.AccountInfo(id=" + this.getId()
				+ ", account=" + this.getAccount()
				+ ", accountName=" + this.getAccountName() + ")";
		}
	}
}
