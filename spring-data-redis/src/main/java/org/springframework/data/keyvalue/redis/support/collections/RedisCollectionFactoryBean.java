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
package org.springframework.data.keyvalue.redis.support.collections;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.keyvalue.redis.connection.DataType;
import org.springframework.data.keyvalue.redis.core.RedisTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Factory bean that facilitates creation of Redis-based collections. Supports list, set, zset (or sortedSet), map (or hash) and properties.
 * Will use the key type if it exists or to create a dedicated collection (Properties vs Map).
 * Otherwise uses the provided type (default is list).
 * 
 * @author Costin Leau
 */
public class RedisCollectionFactoryBean implements InitializingBean, BeanNameAware, FactoryBean<RedisStore> {

	public enum CollectionType {
		LIST {
			@Override
			public DataType dataType() {
				return DataType.LIST;
			}
		},
		SET {
			@Override
			public DataType dataType() {
				return DataType.SET;
			}
		},
		ZSET {
			@Override
			public DataType dataType() {
				return DataType.ZSET;
			}
		},
		MAP {
			@Override
			public DataType dataType() {
				return DataType.HASH;
			}
		},
		PROPERTIES {
			@Override
			public DataType dataType() {
				return DataType.HASH;
			}
		};

		abstract DataType dataType();
	}


	private RedisStore store;
	private CollectionType type = null;
	private RedisTemplate<String, ?> template;
	private String key;
	private String beanName;

	@Override
	public void afterPropertiesSet() {
		if (!StringUtils.hasText(key)) {
			key = beanName;
		}

		Assert.hasText(key, "Collection key is required - no key or bean name specified");
		Assert.notNull(template, "Redis template is required");

		DataType dt = template.type(key);

		// can't create store
		Assert.isTrue(!DataType.STRING.equals(dt), "Cannot create store on keys of type 'string'");

		store = createStore(dt);

		if (store == null) {
			if (type == null) {
				type = CollectionType.LIST;
			}
			store = createStore(type.dataType());
		}
	}

	@SuppressWarnings("unchecked")
	private RedisStore createStore(DataType dt) {
		switch (dt) {
		case LIST:
			return new DefaultRedisList(key, template);

		case SET:
			return new DefaultRedisSet(key, template);

		case ZSET:
			return new DefaultRedisZSet(key, template);

		case HASH:
			if (CollectionType.PROPERTIES.equals(type)) {
				return new RedisProperties(key, template);
			}
			return new DefaultRedisMap(key, template);
		}
		return null;
	}

	@Override
	public RedisStore getObject() {
		return store;
	}

	@Override
	public Class<?> getObjectType() {
		return (store != null ? store.getClass() : RedisStore.class);
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	/**
	 * Sets the store type. Used if the key does not exist.
	 * 
	 * @param type The type to set.
	 */
	public void setType(CollectionType type) {
		this.type = type;
	}

	/**
	 * Sets the template used by the resulting store.
	 * 
	 * @param template The template to set.
	 */
	public void setTemplate(RedisTemplate<String, ?> template) {
		this.template = template;
	}

	/**
	 * Sets the key of the store.
	 *  
	 * @param key The key to set.
	 */
	public void setKey(String key) {
		this.key = key;
	}
}