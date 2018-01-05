/*
 * Copyright 2011-2018 the original author or authors.
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
 * @author Christoph Strobl
 * @deprecated since 2.0, use {@link org.springframework.data.util.Version}.
 */
public class Version implements Comparable<Version> {

	public static final Version UNKNOWN = new Version(0, 0, 0);

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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((major == null) ? 0 : major.hashCode());
		result = prime * result + ((minor == null) ? 0 : minor.hashCode());
		result = prime * result + ((patch == null) ? 0 : patch.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof Version)) {
			return false;
		}
		Version other = (Version) obj;
		if (major == null) {
			if (other.major != null) {
				return false;
			}
		} else if (!major.equals(other.major)) {
			return false;
		}
		if (minor == null) {
			if (other.minor != null) {
				return false;
			}
		} else if (!minor.equals(other.minor)) {
			return false;
		}
		if (patch == null) {
			if (other.patch != null) {
				return false;
			}
		} else if (!patch.equals(other.patch)) {
			return false;
		}
		return true;
	}

}
