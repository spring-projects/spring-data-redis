/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.connection.lettuce.aot;

import static org.assertj.core.api.Assertions.*;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.IntegerOutput;

import java.lang.reflect.Constructor;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.springframework.aot.hint.ExecutableHint;
import org.springframework.aot.hint.ExecutableMode;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.TypeHint;
import org.springframework.aot.hint.TypeReference;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Unit tests for {@link LettuceRuntimeHints}.
 *
 * @author arimu1
 */
class LettuceRuntimeHintsTests {

	@Test // GH-3503
	void registersPublicConstructorsForMappedCommandOutputs() throws Exception {

		RuntimeHints hints = new RuntimeHints();
		new LettuceRuntimeHints().registerHints(hints, getClass().getClassLoader());

		Set<Class<?>> mappedOutputTypes = mappedCommandOutputTypes();
		assertThat(mappedOutputTypes).contains(IntegerOutput.class);

		Set<String> hintedOutputTypes = new LinkedHashSet<>();
		hints.reflection().typeHints().map(typeHint -> typeHint.getType().getName())
				.filter(name -> name.startsWith("io.lettuce.core.output.")).forEach(hintedOutputTypes::add);
		assertThat(hintedOutputTypes).containsExactlyInAnyOrderElementsOf(classNames(mappedOutputTypes));

		for (Class<?> outputType : mappedOutputTypes) {
			assertPublicConstructorsRegistered(hints, outputType);
		}

		TypeHint integerOutput = hints.reflection().getTypeHint(IntegerOutput.class);
		assertThat(integerOutput).isNotNull();
		assertThat(integerOutput.constructors().toList()).anySatisfy(constructor -> {
			assertThat(constructor.getMode()).isEqualTo(ExecutableMode.INVOKE);
			assertThat(constructor.getParameterTypes()).containsExactly(TypeReference.of(RedisCodec.class));
		});
	}

	private static void assertPublicConstructorsRegistered(RuntimeHints hints, Class<?> outputType) {

		TypeHint typeHint = hints.reflection().getTypeHint(outputType);
		assertThat(typeHint).as(outputType.getName()).isNotNull();

		List<ExecutableHint> registered = typeHint.constructors().toList();
		for (Constructor<?> constructor : outputType.getConstructors()) {
			List<TypeReference> parameterTypes = TypeReference.listOf(constructor.getParameterTypes());
			assertThat(registered).as(constructor.toString()).anySatisfy(hint -> {
				assertThat(hint.getMode()).isEqualTo(ExecutableMode.INVOKE);
				assertThat(hint.getParameterTypes()).containsExactlyElementsOf(parameterTypes);
			});
		}
	}

	private static Set<Class<?>> mappedCommandOutputTypes() throws ClassNotFoundException {

		Class<?> typeHints = Class
				.forName("org.springframework.data.redis.connection.lettuce.LettuceConnection$TypeHints");
		Map<?, ?> mapping = (Map<?, ?>) ReflectionTestUtils.getField(typeHints, "COMMAND_OUTPUT_TYPE_MAPPING");
		assertThat(mapping).isNotNull();

		Set<Class<?>> outputTypes = new LinkedHashSet<>();
		for (Object outputType : mapping.values()) {
			assertThat(outputType).isInstanceOf(Class.class);
			Class<?> type = (Class<?>) outputType;
			assertThat(CommandOutput.class).isAssignableFrom(type);
			outputTypes.add(type);
		}
		return outputTypes;
	}

	private static Set<String> classNames(Set<Class<?>> types) {

		Set<String> names = new LinkedHashSet<>();
		for (Class<?> type : types) {
			names.add(type.getName());
		}
		return names;
	}

}
