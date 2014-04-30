/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.redis.core;

/**
 * Options to be used for with {@literal SCAN} command.
 * 
 * @author Christoph Strobl
 * @since 1.3
 */
public class ScanOptions {

	public static ScanOptions NONE = new ScanOptions();

	private Long count;
	private String pattern;

	private ScanOptions() {

	}

	public static ScanOptionsBuilder count(long count) {
		return new ScanOptionsBuilder().count(count);
	}

	public static ScanOptionsBuilder match(String pattern) {
		return new ScanOptionsBuilder().match(pattern);
	}

	public Long getCount() {
		return count;
	}

	public String getPattern() {
		return pattern;
	}

	public static class ScanOptionsBuilder {

		ScanOptions options;

		public ScanOptionsBuilder() {
			options = new ScanOptions();
		}

		public ScanOptionsBuilder count(long count) {
			options.count = count;
			return this;
		}

		public ScanOptionsBuilder match(String pattern) {
			options.pattern = pattern;
			return this;
		}

		public ScanOptions build() {
			return options;
		}

	}

}
