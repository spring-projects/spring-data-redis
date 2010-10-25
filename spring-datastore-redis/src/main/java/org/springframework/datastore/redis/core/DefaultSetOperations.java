package org.springframework.datastore.redis.core;

import java.util.Set;

public class DefaultSetOperations implements SetOperations {

	private RedisTemplate redisTemplate;
	public DefaultSetOperations(RedisTemplate redisTemplate) {
		this.redisTemplate = redisTemplate;
	}
	
	public boolean add(final String key, final String member) {
		return redisTemplate.execute(new RedisCallback<Boolean>() {
			public Boolean doInRedis(RedisClient redisClient) throws Exception {
				return (redisClient.sadd(key, member) == 0) ? true : false;
			}			
		}); 
	}

	public Set<String> getAll(final String key) {
		return redisTemplate.execute(new RedisCallback<Set<String>>() {
			public Set<String> doInRedis(RedisClient redisClient) throws Exception {
				return redisClient.smembers(key);
			}			
		}); 
	}

	public boolean remove(String key, String member) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean removeRandom(String key) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean moveBetweenSets(String srckey, String dstkey, String member) {
		// TODO Auto-generated method stub
		return false;
	}

	public int size(String key) {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean contains(String key, String member) {
		// TODO Auto-generated method stub
		return false;
	}

	public Set<String> getIntersectionOfSets(String... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	public void storeIntersectionOfSets(final String dstkey, final String... keys) {
		redisTemplate.execute(new RedisCallback<Void>() {
			public Void doInRedis(RedisClient redisClient) throws Exception {
				redisClient.sinterstore(dstkey, keys);
				return null;
			}			
		});	
	}

	public Set<String> getUnionOfSets(String... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	public void storeUnionOfSets(String dstkey, String... keys) {
		// TODO Auto-generated method stub
		
	}

	public Set<String> getDifferenceBetweenSets(String... keys) {
		// TODO Auto-generated method stub
		return null;
	}

	public void storeDifferenceBetweenSets(String dstkey, String... keys) {
		// TODO Auto-generated method stub
		
	}

	public String getRandom(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	
}
