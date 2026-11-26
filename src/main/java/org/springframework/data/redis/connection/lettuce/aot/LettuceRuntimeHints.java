/*
 * Copyright 2022-present the original author or authors.
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

import java.lang.reflect.Constructor;

import org.jspecify.annotations.Nullable;
import org.springframework.aot.hint.ExecutableMode;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 * @author arimu1
 * @since 3.0
 */
class LettuceRuntimeHints implements RuntimeHintsRegistrar {

	/**
	 * Command output types instantiated reflectively by {@code LettuceConnection.TypeHints}. Keep this aligned with
	 * {@code COMMAND_OUTPUT_TYPE_MAPPING}.
	 */
	private static final String[] COMMAND_OUTPUT_TYPES = { "io.lettuce.core.output.BooleanOutput",
			"io.lettuce.core.output.DateOutput", "io.lettuce.core.output.DoubleListOutput",
			"io.lettuce.core.output.DoubleOutput", "io.lettuce.core.output.EnumSetOutput",
			"io.lettuce.core.output.IntegerListOutput", "io.lettuce.core.output.IntegerOutput",
			"io.lettuce.core.output.KeyListOutput", "io.lettuce.core.output.KeyValueOutput",
			"io.lettuce.core.output.MapOutput", "io.lettuce.core.output.MultiOutput",
			"io.lettuce.core.output.StatusOutput", "io.lettuce.core.output.ValueListOutput",
			"io.lettuce.core.output.ValueOutput", "io.lettuce.core.output.ValueSetOutput" };

	@Override
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {

		if (ClassUtils.isPresent("io.lettuce.core.RedisClient", classLoader)) {

			hints.reflection().registerType(TypeReference.of("io.lettuce.core.RedisClient"),
					it -> it
							.onReachableType(
									TypeReference.of("org.springframework.data.redis.connection.lettuce.StandaloneConnectionProvider"))
							.withMembers(MemberCategory.INVOKE_PUBLIC_METHODS, MemberCategory.ACCESS_DECLARED_FIELDS));

			registerCommandOutputConstructors(hints, classLoader);
		}
	}

	/**
	 * {@code Class.getConstructor} only sees public constructors, which is how {@code LettuceConnection} looks them up.
	 */
	private static void registerCommandOutputConstructors(RuntimeHints hints, @Nullable ClassLoader classLoader) {

		for (String typeName : COMMAND_OUTPUT_TYPES) {
			Class<?> outputType = ClassUtils.resolveClassName(typeName, classLoader);
			for (Constructor<?> constructor : outputType.getConstructors()) {
				hints.reflection().registerConstructor(constructor, ExecutableMode.INVOKE);
			}
		}
	}

}
