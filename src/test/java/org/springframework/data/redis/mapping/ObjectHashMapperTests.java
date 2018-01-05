/*
 * Copyright 2016-2018 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import java.util.Map;

import org.junit.Test;
import org.springframework.data.redis.hash.ObjectHashMapper;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class ObjectHashMapperTests extends AbstractHashMapperTest {

	protected ObjectHashMapper mapperFor(Class t) {
		return new ObjectHashMapper();
	}

	@Test // DATAREDIS-503
	public void testSimpleType() {
		assertBackAndForwardMapping(new Integer(100));
	}

	@Test // DATAREDIS-503
	public void fromHashShouldCastToType() {

		ObjectHashMapper objectHashMapper = new ObjectHashMapper();
		Map<byte[], byte[]> hash = objectHashMapper.toHash(new Integer(100));

		Integer result = objectHashMapper.fromHash(hash, Integer.class);

		assertThat(result, is(equalTo(new Integer(100))));
	}

	@Test(expected = ClassCastException.class) // DATAREDIS-503
	public void fromHashShouldFailIfTypeDoesNotMatch() {

		ObjectHashMapper objectHashMapper = new ObjectHashMapper();
		Map<byte[], byte[]> hash = objectHashMapper.toHash(new Integer(100));

		String result = objectHashMapper.fromHash(hash, String.class);
	}
}
