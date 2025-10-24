/*
 * Copyright 2011-2025 the original author or authors.
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

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.hash.BeanUtilsHashMapper;

/**
 * @author Costin Leau
 * @author Christoph Strobl
 */
public class BeanUtilsHashMapperTests extends AbstractHashMapperTests {

	protected <T> BeanUtilsHashMapper<T> mapperFor(Class<T> t) {
		return new BeanUtilsHashMapper<>(t);
	}

	@Test
	public void testNestedBean() {
		assertThatExceptionOfType(Exception.class).isThrownBy(super::testNestedBean);
	}

}
