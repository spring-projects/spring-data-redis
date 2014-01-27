/*
 * Copyright 2011-2013 the original author or authors.
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

/**
 * A {@link Comparable} software version
 * 
 * @author Jennifer Hickey
 */
public class Version implements Comparable<Version> {
	Integer major;
	Integer minor;
	Integer patch;

	public Version(int major, int minor, int patch) {
		this.major = major;
		this.minor = minor;
		this.patch = patch;
	}

	public int compareTo(Version o) {
		if (this.major != o.major) {
			return this.major.compareTo(o.major);
		}
		if (this.minor != o.minor) {
			return this.minor.compareTo(o.minor);
		}
		if (this.patch != o.patch) {
			return this.patch.compareTo(o.patch);
		}
		return 0;
	}

	@Override
	public String toString() {
		return "" + major + "." + minor + "." + patch;
	}
}
