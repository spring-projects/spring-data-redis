/*
 * Copyright 2022-2022 the original author or authors.
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
package org.springframework.data.redis.serializer;

import java.util.Objects;
/*
 * @author Jos Roseboom
 */
public class NotSerializableClass {

	public String name;

	public NotSerializableClass() {
	}

	public NotSerializableClass(String name) {
		this();
		this.name = name;
	}

	@Override public boolean equals(Object o) {
		if (this == o)
			return true;
		if (!(o instanceof NotSerializableClass))
			return false;
		NotSerializableClass that = (NotSerializableClass) o;
		return Objects.equals(name, that.name);
	}

	@Override public int hashCode() {
		return Objects.hash(name);
	}
}
