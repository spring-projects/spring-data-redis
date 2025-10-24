/*
 * Copyright 2020-2025 the original author or authors.
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
package org.springframework.data.redis.test.extension.parametrized;

import static java.util.stream.Collectors.*;
import static org.junit.jupiter.params.ParameterizedTest.*;

import java.text.Format;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.junit.platform.commons.JUnitException;
import org.junit.platform.commons.util.StringUtils;

/**
 * Copy of {@code org.junit.jupiter.params.ParameterizedTestNameFormatter}.
 */
class ParameterizedTestNameFormatter {

	private static final char ELLIPSIS = '\u2026';

	private final String pattern;
	private final String displayName;
	private final ParameterizedTestContext methodContext;
	private final int argumentMaxLength;

	ParameterizedTestNameFormatter(String pattern, String displayName, ParameterizedTestContext methodContext,
			int argumentMaxLength) {
		this.pattern = pattern;
		this.displayName = displayName;
		this.methodContext = methodContext;
		this.argumentMaxLength = argumentMaxLength;
	}

	String format(int invocationIndex, Object... arguments) {
		try {
			return formatSafely(invocationIndex, arguments);
		} catch (Exception ex) {
			throw new JUnitException("The display name pattern defined for the parameterized test is invalid;"
					+ " See nested exception for further details.", ex);
		}
	}

	private String formatSafely(int invocationIndex, Object[] arguments) {
		String pattern = prepareMessageFormatPattern(invocationIndex, arguments);
		MessageFormat format = new MessageFormat(pattern);
		Object[] humanReadableArguments = makeReadable(format, arguments);
		return format.format(humanReadableArguments);
	}

	private String prepareMessageFormatPattern(int invocationIndex, Object[] arguments) {
		String result = pattern//
				.replace(DISPLAY_NAME_PLACEHOLDER, this.displayName)//
				.replace(INDEX_PLACEHOLDER, String.valueOf(invocationIndex));

		if (result.contains(ARGUMENTS_WITH_NAMES_PLACEHOLDER)) {
			result = result.replace(ARGUMENTS_WITH_NAMES_PLACEHOLDER, argumentsWithNamesPattern(arguments));
		}

		if (result.contains(ARGUMENTS_PLACEHOLDER)) {
			result = result.replace(ARGUMENTS_PLACEHOLDER, argumentsPattern(arguments));
		}

		return result;
	}

	private String argumentsWithNamesPattern(Object[] arguments) {
		return IntStream.range(0, arguments.length) //
				.mapToObj(index -> methodContext.getParameterName(index).map(name -> name + "=").orElse("") + "{" + index + "}") //
				.collect(joining(", "));
	}

	private String argumentsPattern(Object[] arguments) {
		return IntStream.range(0, arguments.length) //
				.mapToObj(index -> "{" + index + "}") //
				.collect(joining(", "));
	}

	private Object[] makeReadable(MessageFormat format, Object[] arguments) {
		Format[] formats = format.getFormatsByArgumentIndex();
		Object[] result = Arrays.copyOf(arguments, Math.min(arguments.length, formats.length), Object[].class);
		for (int i = 0; i < result.length; i++) {
			if (formats[i] == null) {
				result[i] = truncateIfExceedsMaxLength(StringUtils.nullSafeToString(arguments[i]));
			}
		}
		return result;
	}

	private String truncateIfExceedsMaxLength(String argument) {
		if (argument.length() > argumentMaxLength) {
			return argument.substring(0, argumentMaxLength - 1) + ELLIPSIS;
		}
		return argument;
	}

}
