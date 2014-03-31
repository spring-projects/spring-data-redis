Spring Data Redis
=======================

The primary goal of the [Spring Data](http://projects.spring.io/spring-data/) project is to make it easier to build Spring-powered applications that use new data access technologies such as non-relational databases, map-reduce frameworks, and cloud based data services.
This modules provides integration with the [Redis] (http://redis.io/) store. 

# Docs

You can find out more details from the [user documentation](http://docs.spring.io/spring-data/data-redis/docs/current/reference/html/) or by browsing the [javadocs](http://docs.spring.io/spring-data/data-redis/docs/current/api/).

# Examples

For examples on using the Spring Data Key Value, see the dedicated project, also available on [GitHub](https://github.com/spring-projects/spring-data-keyvalue-examples)

# Artifacts

* Maven:

~~~~~ xml

<dependency>
  <groupId>org.springframework.data</groupId>
  <artifactId>spring-data-redis</artifactId>
  <version>${version}</version>
</dependency> 

<!-- used for nightly builds -->
<repository>
  <id>spring-maven-snapshot</id>
  <snapshots><enabled>true</enabled></snapshots>
  <name>Springframework Maven SNAPSHOT Repository</name>
  <url>http://repo.spring.io/libs-release</url>
</repository> 

<!-- used for milestone/rc releases -->
<repository>
  <id>spring-maven-milestone</id>
  <name>Springframework Maven Milestone Repository</name>
  <url>http://repo.spring.io/libs-milestone</url>
</repository> 
~~~~~

* Gradle: 

~~~~~ groovy
repositories {
   maven { url "http://repo.spring.io/libs-milestone" }
   maven { url "http://repo.spring.io/libs-snapshot" }
}

// used for nightly builds
dependencies {
   compile "org.springframework.data:spring-data-redis:${version}"
}
~~~~~

Latest GA release is _1.2.1.RELEASE_  
Latest nightly is _1.3.0.BUILD-SNAPSHOT_

# Usage (for the impatient)

* Configure the Redis connector to use (here [jedis](https://github.com/xetorthio/jedis)):

~~~~~ xml
<beans xmlns="http://www.springframework.org/schema/beans"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
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

Spring Data Redis uses Gradle as its build system. To build the system simply run:

    gradlew

from the project root folder. This will compile the sources, run the tests and create the artifacts.  

To generate IDE-specific files, use

    gradlew eclipse
 
or

    gradlew idea 

depending on your editor.

# Contributing

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Spring Community Forums.  Please help out on the [forum](http://forum.spring.io/forum/spring-projects/data/nosql) by responding to questions and joining the debate.
* Create [JIRA](https://jira.springframework.org/browse/DATAREDIS) tickets for bugs and new features and comment and vote on the ones that you are interested in.  
* Watch for upcoming articles on Spring by [subscribing](https://spring.io/blog) to spring.io.

Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/). If you want to contribute code this way, read the Spring Framework [contributor guidelines] (https://github.com/spring-projects/spring-framework/blob/master/CONTRIBUTING.md).

# Staying in touch

Follow the project team ([@thomasdarimont](http://twitter.com/thomasdarimont), [@stroblchristoph](http://twitter.com/stroblchristoph)) on Twitter. In-depth articles can be
found at the Spring [team blog](https://spring.io/blog), and releases are announced via our [news feed](https://spring.io/blog/category/news).
