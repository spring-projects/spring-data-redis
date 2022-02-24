/*
 * Copyright 2022 the original author or authors.
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
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * Unit tests for {@link BoundOperationsProxyFactory}.
 *
 * @author Mark Paluch
 */
class BoundOperationsProxyFactoryUnitTests {

	private void test(Class<?> operationsTarget, BoundOperationsProxyFactory factory, Method method) {

		Method toCall = factory.lookupMethod(method, operationsTarget, true);
		if (toCall == null) {
			toCall = factory.lookupMethod(method, BoundOperationsProxyFactory.DefaultBoundKeyOperations.class, false);
		}

		assertThat(toCall)
				.describedAs("Backing method for method %s not found in %s".formatted(method, operationsTarget.getName()))
				.isNotNull();
	}

	@TestFactory
	Stream<DynamicContainer> containers() {

		Stream<Tuple2<Class<?>, Class<?>>> input = Stream.of( //
				Tuples.of(BoundGeoOperations.class, GeoOperations.class), //
				Tuples.of(BoundHashOperations.class, HashOperations.class), //
				Tuples.of(BoundListOperations.class, ListOperations.class), //
				Tuples.of(BoundStreamOperations.class, StreamOperations.class), //
				Tuples.of(BoundSetOperations.class, SetOperations.class), //
				Tuples.of(BoundValueOperations.class, ValueOperations.class), //
				Tuples.of(BoundZSetOperations.class, ZSetOperations.class) //
		);

		return input.map(it -> containerFor(it.getT1(), it.getT2()));
	}

	private DynamicContainer containerFor(Class<?> boundInterface, Class<?> operationsTarget) {
		Stream<Method> methods = Arrays.stream(boundInterface.getMethods())
				.filter(it -> !it.getName().equals("getType") && !it.getName().equals("getOperations"))
				.filter(Predicate.not(Method::isDefault));

		BoundOperationsProxyFactory factory = new BoundOperationsProxyFactory();

		Stream<DynamicTest> stream = DynamicTest.stream(methods, Method::toString, method -> {

			test(operationsTarget, factory, method);

		});

		return DynamicContainer.dynamicContainer(boundInterface.getSimpleName(), stream);
	}

}
