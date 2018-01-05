/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis;

import java.io.Serializable;

/**
 * Simple serializable class.
 *
 * @author Costin Leau
 */
public class Address implements Serializable {

	private static final long serialVersionUID = 4924045450477798779L;

	private String street;

	private Integer number;

	public Address() {}

	/**
	 * Constructs a new <code>Address</code> instance.
	 *
	 * @param street
	 * @param number
	 */
	public Address(String street, int number) {
		super();
		this.street = street;
		this.number = number;
	}

	/**
	 * Returns the street.
	 *
	 * @return Returns the street
	 */
	public String getStreet() {
		return street;
	}

	/**
	 * @param street The street to set.
	 */
	public void setStreet(String street) {
		this.street = street;
	}

	/**
	 * Returns the number.
	 *
	 * @return Returns the number
	 */
	public Integer getNumber() {
		return number;
	}

	/**
	 * @param number The number to set.
	 */
	public void setNumber(Integer number) {
		this.number = number;
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((number == null) ? 0 : number.hashCode());
		result = prime * result + ((street == null) ? 0 : street.hashCode());
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Address))
			return false;
		Address other = (Address) obj;
		if (number == null) {
			if (other.number != null)
				return false;
		} else if (!number.equals(other.number))
			return false;
		if (street == null) {
			if (other.street != null)
				return false;
		} else if (!street.equals(other.street))
			return false;
		return true;
	}
}
