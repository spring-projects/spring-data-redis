Spring Data Redis
=======================

The primary goal of the [Spring Data](http://www.springsource.org/spring-data) project is to make it easier to build Spring-powered applications that use new data access technologies such as non-relational databases, map-reduce frameworks, and cloud based data services.
This modules provides integration with the [Redis] (http://code.google.com/p/redis/) store. 

# Docs

You can find out more details from the [user documentation](http://static.springsource.org/spring-data/redis/docs/current/reference/) or by browsing the [javadocs](http://static.springsource.org/spring-data/redis/docs/current/api/).

# Examples

For examples on using the Spring Data Key Value, see the dedicated project, also available on [GitHub](https://github.com/SpringSource/spring-data-keyvalue-examples)

# Artifacts

* Maven:

~~~~~ xml

<dependency>
  <groupId>org.springframework.data</groupId>
  <artifactId>spring-data-redis</artifactId>
  <version>${version}</version>
</dependency> 


<repository>
  <id>spring-maven-snapshot</id>
  <snapshots><enabled>true</enabled></snapshots>
  <name>Springframework Maven SNAPSHOT Repository</name>
  <url>http://maven.springframework.org/snapshot</url>
</repository> 
~~~~~

* Gradle: 

~~~~~ groovy
repositories {
   mavenRepo name: "spring-snapshot", urls: "http://maven.springframework.org/snapshot"
}

dependencies {
   compile "org.springframework.data:spring-data-redis:${version}"
}
~~~~~

The latest `version` is _1.0.0.RC1_

# Usage (for the impatient)

* Configure the Redis connector to use (here [jedis](https://github.com/xetorthio/jedis)):

~~~~~ xml
      <beans xmlns="http://www.springframework.org/schema/beans"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance 
        xmlns:p="http://www.springframework.org/schema/p"
        xsi:schemaLocation="
        http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd">
        
        <bean id="jedisFactory" class="org.springframework.data.redis.connection.jedis.JedisConnectionFactory"/>
        
        <bean id="redisTemplate" class="org.springframework.data.redis.core.RedisTemplate"
            p:connection-factory="jedisFactory"/>
      </beans>
~~~~~

* Use `RedisTemplate` to interact with the Redis store:

~~~~~ java
      String random = template.randomKey();
      template.set(random, new Person("John", "Smith"));
~~~~~

* Use Redis 'views' to execute specific operations based on the underlying Redis type:

~~~~~ java
      ListOperations<String, Person> listOps = template.listOps();
      listOps.rightPush(random, new Person("Jane", "Smith"));
      List<Person> peopleOnSecondFloor = listOps.range("users:floor:2", 0, -1);
~~~~~

# Building

Spring Hadoop uses Gradle as its build system. To build the system simply run:

    gradlew

from the project root folder. This will compile the sources, run the tests and create the artifacts.

# Contributing


Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Spring Community Forums.  Please help out on the [forum](http://forum.springsource.org/forumdisplay.php?f=80) by responding to questions and joining the debate.
* Create [JIRA](https://jira.springframework.org/browse/DATAKV) tickets for bugs and new features and comment and vote on the ones that you are interested in.  
* Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/). If you want to contribute code this way, please reference a JIRA ticket as well covering the specific issue you are addressing.
* Watch for upcoming articles on Spring by [subscribing](http://www.springsource.org/node/feed) to springframework.org

Before we accept a non-trivial patch or pull request we will need you to sign the [contributor's agreement](https://support.springsource.com/spring_committer_signup).  Signing the contributor's agreement does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do.  Active contributors might be asked to join the core team, and given the ability to merge pull requests.
