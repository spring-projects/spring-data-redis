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
package org.springframework.data.redis.connection;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.mockito.Mockito;

/**
 * @author Christoph Strobl
 */
public abstract class AbstractConnectionUnitTestBase<T> {

	private T nativeRedisConnectionMock;

	protected T getNativeRedisConnectionMock() {

		if (this.nativeRedisConnectionMock == null) {
			Class<T> type = resolveReturnedClassFromGernericType();
			this.nativeRedisConnectionMock = Mockito.mock(type);
		}

		return this.nativeRedisConnectionMock;
	}

	protected T verifyNativeConnectionInvocation() {
		return Mockito.verify(getNativeRedisConnectionMock(), Mockito.times(1));
	}

	protected void setNativeRedisConnectionMock(T nativeRedisConnectionMock) {
		this.nativeRedisConnectionMock = nativeRedisConnectionMock;
	}

	@SuppressWarnings("unchecked")
	private Class<T> resolveReturnedClassFromGernericType() {

		ParameterizedType parameterizedType = resolveReturnedClassFromGernericType(getClass());
		return (Class<T>) parameterizedType.getActualTypeArguments()[0];
	}

	private ParameterizedType resolveReturnedClassFromGernericType(Class<?> clazz) {

		Object genericSuperclass = clazz.getGenericSuperclass();
		if (genericSuperclass instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) genericSuperclass;
			Type rawtype = parameterizedType.getRawType();
			if (AbstractConnectionUnitTestBase.class.equals(rawtype)) {
				return parameterizedType;
			}
		}

		return resolveReturnedClassFromGernericType(clazz.getSuperclass());
	}

}
