/*
 * Copyright 2010 the original author or authors.
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

package org.springframework.datastore.redis.core;

import java.util.List;
import java.util.Map;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.datastore.redis.support.RedisUtils;
import org.springframework.datastore.redis.support.converter.DefaultRedisConverter;
import org.springframework.datastore.redis.support.converter.RedisConverter;
import org.springframework.util.Assert;

/**
 * <b>This is the central class in the Redis core package.</b>
 * It simplifies the use of Redis and helps to avoid common errors.
 * 
 * @author Mark Pollack
 *
 */
public class RedisTemplate extends RedisAccessor implements RedisOperations {
	
	// TODO perform validation to see if value size > 1GB
	// TODO perform validation to see if key contains space, newline or whitespace.
	// TODO warning on key size being large > 1024 bytes?
	
	private RedisConverter redisConverter = new DefaultRedisConverter();
	
	private ServerOperations serverOperations;
	
	public RedisTemplate() {
		initDefaults();
	}
	public RedisTemplate(RedisClientFactory redisClientFactory) {
		this();
		this.setRedisClientFactory(redisClientFactory);
		afterPropertiesSet();
	}
	
	public void setRedisConverter(RedisConverter redisConverter) {
		this.redisConverter = redisConverter;
	}

	protected void initDefaults() {
		serverOperations = new DefaultServerOperations(this);		
	}



	public <T> T execute(RedisCallback<T> action) {
		Assert.notNull(action, "Callback object must not be null");
		
		RedisClient clientToClose = null;
		try {
			RedisClient clientToUse = null; //ConnectionFactoryUtils.doGetTransacxtionChannel(getConnectionFactory, this.transactionResourceFactory);
			if (clientToUse == null) {
				clientToClose = createClient();
				clientToUse = clientToClose;
			}
			if (logger.isDebugEnabled()) {
				logger.debug("Executing callback on Redis Client: " + clientToUse);
			}
			return action.doInRedis(clientToUse);
		}		
		catch (Exception e) {			
			throw convertRedisAccessException(e);
		} finally {
			RedisUtils.closeClient(clientToClose);			
		}
	}
	
	protected DataAccessException convertRedisAccessException(Exception ex) {
		//TODO
		return null;
	}
	
	public ServerOperations getServerOperations() {
		return serverOperations; 
	}

	public ListOperations getListOperations() {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}
	
	// Key Value Operations

	public String get(final String key) {
		return execute(new RedisCallback<String>() {
			public String doInRedis(RedisClient redisClient) throws Exception {
				return redisClient.get(key);
			}			
		});		
	}

	public byte[] getAsBytes(final String key) {
		return execute(new RedisCallback<byte[]>() {
			public byte[] doInRedis(RedisClient redisClient) throws Exception {
				return redisClient.getAsBytes(key);
			}			
		});	
	}

	public <T> T getAndConvert(String key, Class<T> requiredType) {
		//TODO deserializer exceptions need to be under DAO exception hierarchy.
		Object object = redisConverter.deserialize(getAsBytes(key));
		if (requiredType != null && object != null && !requiredType.isAssignableFrom(object.getClass())) {
			throw new DataRetrievalFailureException("Can not assign from " + requiredType + " to " + object.getClass());
		}
		return (T) object;
	}

	public String getAndSet(String key, String value) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public byte[] getAndSetBytes(String key, byte[] value) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public <T> T getAndSetObject(String key, T value, Class<T> requiredType) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public void set(final String key, final String value) {
		execute(new RedisCallback<Void>() {
			public Void doInRedis(RedisClient redisClient) throws Exception {
				redisClient.set(key, value);
				return null;
			}			
		});		
	}

	public void set(String key, String value, long expiryInMillis) {
		// TODO Auto-generated method stub
		
	}

	public void setAsBytes(final String key, final byte[] value) {
		execute(new RedisCallback<Void>() {
			public Void doInRedis(RedisClient redisClient) throws Exception {
				redisClient.set(key, value);
				return null;
			}			
		});	
		
	}

	public void setAsBytes(String key, byte[] value, long expiryInMillis) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public void setIfKeyNonExistent(String key, String value) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public void setIfKeyNonExistent(String key, byte[] value) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public void setMultiple(Map<String, String> keysAndValues) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public void setMultipleAsBytes(Map<String, byte[]> keysAndValues) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public void setMultipleAsBytesIfKeysNonExistent(
			Map<String, byte[]> keysAndValues) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public void setMultipleIfKeysNonExistent(Map<String, String> keysAndValues) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public int append(String key, String value) {
		throw new RuntimeException("unimplemented");
	}

	public void convertAndSet(String key, Object value) {
		setAsBytes(key, this.redisConverter.serialize(value));		
	}

	public void convertAndSet(String key, Object value, long expiryInMillis) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");		
	}

	public void convertAndSetIfKeyNonExistent(String key, Object value) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public <T> void convertAndSetMultiple(Map<String, T> keysAndValues) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public <T> void convertAndSetMultipleIfKeysNonExistent(
			Map<String, T> keysAndValues) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public int decrement(String key) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public int decrementBy(String key, int value) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public List<String> getValues(List<String> keys) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public int increment(String key) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public int incrementBy(String key, int value) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}

	public String subString(String key, int fromIndex, int toIndex) {
		// TODO Auto-generated method stub
		throw new RuntimeException("unimplemented");
	}
	public <T> List<T> getAndConvertValues(List<String> keys,
			Class<T> requiredType) {
		// TODO Auto-generated method stub
		return null;
	}
	public String getSubString(String key, int fromIndex, int toIndex) {
		// TODO Auto-generated method stub
		return null;
	}
	public boolean containsKey(String key) {
		// TODO Auto-generated method stub
		return false;
	}
	public boolean deleteKeys(final String... keys) {
		return execute(new RedisCallback<Boolean>() {
			public Boolean doInRedis(RedisClient redisClient) throws Exception {
				Integer intVal = redisClient.del(keys);
				return (intVal == 0) ? false : true; 
			}			
		});
	}
	
	public SetOperations getSetOperations() {
		return new DefaultSetOperations(this);
	}





	
}