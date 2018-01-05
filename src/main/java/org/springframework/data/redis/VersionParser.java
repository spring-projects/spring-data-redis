/*
 * Copyright 2014-2018 the original author or authors.
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.lang.Nullable;

/**
 * Central class for reading version string (eg. {@literal 1.3.1}) into {@link Version}.
 *
 * @author Christoph Strobl
 * @since 1.3
 * @deprecated since 2.0, use {@link org.springframework.data.util.Version}.
 */
public class VersionParser {

	private static final Pattern VERSION_MATCHER = Pattern.compile("([0-9]+)\\.([0-9]+)(\\.([0-9]+))?(.*)");

	/**
	 * Parse version string {@literal eg. 1.1.1} to {@link Version}.
	 *
	 * @param version can be {@literal null}.
	 * @return never {@literal null}.
	 */
	public static Version parseVersion(@Nullable String version) {

		if (version == null) {
			return Version.UNKNOWN;
		}

		Matcher matcher = VERSION_MATCHER.matcher(version);
		if (matcher.matches()) {
			String major = matcher.group(1);
			String minor = matcher.group(2);
			String patch = matcher.group(4);
			return new Version(Integer.parseInt(major), minor != null ? Integer.parseInt(minor) : 0,
					patch != null ? Integer.parseInt(patch) : 0);
		}

		return Version.UNKNOWN;
	}

}
