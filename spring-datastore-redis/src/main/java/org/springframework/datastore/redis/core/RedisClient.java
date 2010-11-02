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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An interface that is a one to one mapping to Redis commands to method names 
 * that is portable across various Redis driver libraries.
 * 
 * @author Mark Pollack
 *
 */
public interface RedisClient {

	// Connection Management
	
	void disconnect() throws IOException;
	
	
	// Database control commands

	String save();
	
	String bgsave();
	
	String bgrewriteaof();
	
	Integer lastsave();
	
	String shutdown();
	
	Map<String, String> info();
	
	//bulk reply callback - monitor
	
	String slaveof(String host, int port);
	
	String slaveofNoOne();
	
	String select(int index);
	
	String flushDb();
	
	String flushAll();
	
	Integer move(String key, int dbIndex);
	
	String auth(String password);
		
	Integer dbSize();
	
	
	// Note: JRedis and the SMA client do not return the response code for set, would probably have to catch exception.
	
	// Commands operating on string values  "StringOperations" or "Operations"
	
	
	/**
	 * Set the string value as value of the key. The string can't be longer than 1073741824 bytes (1 GB). 
	 * <p>Time complexity: O(1)</p> 
	 * <p>Corresponds to Redis command "SET key value"</p>
	 * @see <a href="http://code.google.com/p/redis/wiki/SetCommand">setCommand</a>
	 * @param key key whose associated value is to be returned
	 * @param value value to be associated with the specified key
	 */
	void set(String key, String value);
	
	void set(String key, byte[] value);
	
	String get(String key);
	
	byte[] getAsBytes(String key);
	
	String getSet(String key, String value);
	
	List<String> mget(String... keys);
	
	//TODO mgetAsBytes?  Best to have byte[] overloads somewhere else?
	
	/**
	 * SETNX works exactly like SET with the only difference that if the key already exists no operation is performed. 
	 * SETNX actually means "SET if Not eXists". 
	 * <p>Time complexity: O(1)</p>
	 * <p>Corresponds to command "SETNX key value"</p>
	 * @see <a href="http://code.google.com/p/redis/wiki/SetnxCommand">SetnxCommand</a>
	 * @param key key whose associated value is to be set
	 * @param value value to be associated with the specified key
	 * @return 1 if the key was set, 0 if the key was not set
	 */
	Integer setnx(String key, String value);
	
	/**
	 * The command is exactly equivalent to the following group of commands:
	 * <p>SET key value
	 * EXPIRE key time
	 * </p>
	 * <p>Time complexity: O(1)</p>
	 * @see <a href="http://code.google.com/p/redis/wiki/SetexCommand">SetexCommand</a>
	 * @param key key whose associated value is to be set
	 * @param seconds timeout on the specified key.  After the timeout the key will automatically be deleted by the server
	 * @param value timeout in seconds
	 * @return Status reply code, OK is success
	 */
    String setex(String key, int seconds, String value);
	
    /**
     * Set the the respective keys to the respective values. 
     * <p>Time complexity: O(1) to set every key</p>
     * <p>Corresponds to the command "MSET key1 value1 key2 value2 ... keyN valueN"</p>
     * @see <a href="http://code.google.com/p/redis/wiki/MsetCommand">MsetCommand</a>
     * @param keysvalues key value sequence
     * @return OK as MSET can't fail.
     */
    //TODO Consider Map<string,string> here or in template?  Map<string, byte> ?
    String mset(String... keysvalues);
    
    Integer msetnx(String... keysvalues);
    
    Integer incrBy(String key, int increment);
    
    Integer incr(String key);
    
    Integer decr(String key);
    
    Integer decrBy(String key, int decrement);
    
    //TODO incrementByOne,decrementByOne in template
    
    /**
     * If the key already exists and is a string, this command appends the provided value at the 
     * end of the string. If the key does not exist it is created and set as an empty string, 
     * so APPEND will be very similar to SET in this special case.
     * @see <a href="http://code.google.com/p/redis/wiki/AppendCommand">AppendCommand</a>
	 * @param key key whose associated value is to be appended
	 * @param value value to be appended to end of current value associated with the specified key
     * @return the total length of the string after the append operation. 
     */
    Integer append(String key, String value);
    
    String substr(String key, int start, int end);
	
    
    // Commands operating on all value types "KeySpaceOperations"
    
    Integer exists(String key);
     
    Integer del(String... keys);
    
    String type(String key);
    
    List<String> keys(String pattern);
    
    String randomKey();
    
    String rename(String oldkey, String newkey);
    
    Integer renamenx(String oldkey, String newkey);
        
    Integer expire(String key, int seconds);
    
    Integer expireAt(String key, long unixTime);
    
    Integer ttl(String key);
    
    Integer persist(String key);

    
	
	
	// Probably not possible to abstract at this level across different providers....
	// T sendCommand(String commandName, ReplyTypeMapper mapper, String... commandArgs);
	
	// Commands operating on Sets
    
    Integer sadd(String key, String member);

    Set<String> smembers(String key);

    Integer srem(String key, String member);

    String spop(String key); 

    Integer smove(String srckey, String dstkey, String member); 

    Integer scard(String key); 

    Integer sismember(String key, String member); 

    Set<String> sinter(String... keys); 

    Integer sinterstore(String dstkey, String... keys); 

    Set<String> sunion(String... keys); 

    Integer sunionstore(String dstkey, String... keys); 

    Set<String> sdiff(String... keys); 

    Integer sdiffstore(String dstkey, String... keys); 

    String srandmember(String key); 
	
}
