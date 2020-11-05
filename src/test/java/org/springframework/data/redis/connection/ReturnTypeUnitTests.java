/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.redis.connection;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Unit tests for {@link ReturnType}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
public class ReturnTypeUnitTests {

	@Parameter public Class<?> listClass;

	@Parameters
	public static Collection<Object[]> testParams() {
		return Arrays.asList(new Object[][] { { List.class }, { ArrayList.class }, { LinkedList.class } });
	}

	@Test
	public void shouldConsiderListsAsMultiType() {

		assertThat(ReturnType.fromJavaType(listClass)).isEqualTo(ReturnType.MULTI);
	}

}
