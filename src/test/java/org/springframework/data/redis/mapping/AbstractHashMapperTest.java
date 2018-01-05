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
package org.springframework.data.redis.mapping;

import static org.junit.Assert.*;

import java.util.Map;

import org.hamcrest.core.IsCollectionContaining;
import org.hamcrest.core.IsEqual;
import org.hamcrest.core.IsNot;
import org.hamcrest.core.IsNull;
import org.junit.Test;
import org.springframework.data.redis.Address;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.hash.HashMapper;

/**
 * @author Costin Leau
 * @author Christoph Strobl
 */
public abstract class AbstractHashMapperTest {

	@SuppressWarnings("rawtypes")
	protected abstract <T> HashMapper mapperFor(Class<T> t);

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected void assertBackAndForwardMapping(Object o) {

		HashMapper mapper = mapperFor(o.getClass());
		Map hash = mapper.toHash(o);
		assertThat(mapper.fromHash(hash), IsEqual.equalTo(o));
	}

	@Test
	public void testSimpleBean() throws Exception {
		assertBackAndForwardMapping(new Address("Broadway", 1));
	}

	@Test
	public void testNestedBean() throws Exception {
		assertBackAndForwardMapping(new Person("George", "Enescu", 74, new Address("liveni", 19)));
	}

	@Test // DATAREDIS-421
	public void toHashShouldTreatNullValuesCorrectly() {

		Person source = new Person("rand", null, 19);

		assertBackAndForwardMapping(source);

		assertThat((Iterable<Object>) mapperFor(Person.class).toHash(source).values(),
				IsNot.not(IsCollectionContaining.hasItems(IsNull.nullValue())));

	}
}
