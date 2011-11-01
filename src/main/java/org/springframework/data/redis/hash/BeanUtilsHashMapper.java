/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.redis.hash;

import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;

/**
 * HashMapper based on Apache Commons BeanUtils project. Does NOT supports nested properties.
 * 
 * @author Costin Leau
 */
public class BeanUtilsHashMapper<T> implements HashMapper<T, String, String> {

	private Class<T> type;

	public BeanUtilsHashMapper(Class<T> type) {
		this.type = type;
	}

	
	public T fromHash(Map<String, String> hash) {
		T instance = org.springframework.beans.BeanUtils.instantiate(type);
		try {
			BeanUtils.populate(instance, hash);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		return instance;
	}

	
	public Map<String, String> toHash(T object) {
		try {
			return BeanUtils.describe(object);
		} catch (Exception ex) {
			throw new IllegalArgumentException("Cannot describe object " + object);
		}
	}
}
