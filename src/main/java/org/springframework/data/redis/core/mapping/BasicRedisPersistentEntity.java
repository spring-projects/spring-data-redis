/*
 * Copyright 2015-2017 the original author or authors.
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
package org.springframework.data.redis.core.mapping;

import org.springframework.data.annotation.Id;
import org.springframework.data.keyvalue.core.mapping.BasicKeyValuePersistentEntity;
import org.springframework.data.keyvalue.core.mapping.KeySpaceResolver;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentProperty;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.redis.core.TimeToLiveAccessor;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

/**
 * {@link RedisPersistentEntity} implementation.
 * 
 * @author Christoph Strobl
 * @param <T>
 */
public class BasicRedisPersistentEntity<T> extends BasicKeyValuePersistentEntity<T>
		implements RedisPersistentEntity<T> {

	private TimeToLiveAccessor timeToLiveAccessor;

	/**
	 * Creates new {@link BasicRedisPersistentEntity}.
	 * 
	 * @param information must not be {@literal null}.
	 * @param fallbackKeySpaceResolver can be {@literal null}.
	 * @param timeToLiveResolver can be {@literal null}.
	 */
	public BasicRedisPersistentEntity(TypeInformation<T> information, KeySpaceResolver fallbackKeySpaceResolver,
			TimeToLiveAccessor timeToLiveAccessor) {
		super(information, fallbackKeySpaceResolver);

		Assert.notNull(timeToLiveAccessor, "TimeToLiveAccessor must not be null");
		this.timeToLiveAccessor = timeToLiveAccessor;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.mapping.RedisPersistentEntity#getTimeToLiveAccessor()
	 */
	@Override
	public TimeToLiveAccessor getTimeToLiveAccessor() {
		return this.timeToLiveAccessor;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.model.BasicPersistentEntity#returnPropertyIfBetterIdPropertyCandidateOrNull(org.springframework.data.mapping.PersistentProperty)
	 */
	@Override
	protected KeyValuePersistentProperty returnPropertyIfBetterIdPropertyCandidateOrNull(
			KeyValuePersistentProperty property) {

		Assert.notNull(property, "Property must not be null!");

		if (!property.isIdProperty()) {
			return null;
		}

		KeyValuePersistentProperty currentIdProperty = getIdProperty();
		boolean currentIdPropertyIsSet = currentIdProperty != null;

		if (!currentIdPropertyIsSet) {
			return property;
		}

		boolean currentIdPropertyIsExplicit = currentIdProperty.isAnnotationPresent(Id.class);
		boolean newIdPropertyIsExplicit = property.isAnnotationPresent(Id.class);

		if (currentIdPropertyIsExplicit && newIdPropertyIsExplicit) {
			throw new MappingException(String.format(
					"Attempt to add explicit id property %s but already have an property %s registered "
							+ "as explicit id. Check your mapping configuration!",
					property.getField(), currentIdProperty.getField()));
		}

		if (!currentIdPropertyIsExplicit && !newIdPropertyIsExplicit) {
			throw new MappingException(
					String.format("Attempt to add id property %s but already have an property %s registered "
							+ "as id. Check your mapping configuration!", property.getField(), currentIdProperty.getField()));
		}

		if (newIdPropertyIsExplicit) {
			return property;
		}

		return null;
	}
}
