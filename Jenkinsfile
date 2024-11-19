def p = [:]
node {
	checkout scm
	p = readProperties interpolate: true, file: 'ci/pipeline.properties'
}

pipeline {
	agent none

	triggers {
		pollSCM 'H/10 * * * *'
		upstream(upstreamProjects: "spring-data-keyvalue/4.0.x", threshold: hudson.model.Result.SUCCESS)
	}

	options {
		disableConcurrentBuilds()
		buildDiscarder(logRotator(numToKeepStr: '14'))
	}

	stages {
		stage("Docker Images") {
			parallel {
				stage('Publish JDK 17 + Redis 6.2 Docker Image') {
					when {
						anyOf {
							changeset "ci/openjdk17-redis-6.2/Dockerfile"
							changeset "Makefile"
							changeset "ci/pipeline.properties"
						}
					}
					agent { label 'data' }
					options { timeout(time: 20, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-with-redis-6.2:${p['java.main.tag']}", "--build-arg BASE=${p['docker.java.main.image']} --build-arg VERSION=${p['docker.redis.6.version']} -f ci/openjdk17-redis-6.2/Dockerfile .")
							docker.withRegistry(p['docker.registry'], p['docker.credentials']) {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK 17 + Redis 7.2 Docker Image') {
					when {
						anyOf {
							changeset "ci/openjdk17-redis-7.2/Dockerfile"
							changeset "Makefile"
							changeset "ci/pipeline.properties"
						}
					}
					agent { label 'data' }
					options { timeout(time: 20, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-with-redis-7.2:${p['java.main.tag']}", "--build-arg BASE=${p['docker.java.main.image']} --build-arg VERSION=${p['docker.redis.7.version']} -f ci/openjdk17-redis-7.2/Dockerfile .")
							docker.withRegistry(p['docker.registry'], p['docker.credentials']) {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK 17 + Valkey 7.2 Docker Image') {
					when {
						anyOf {
							changeset "ci/openjdk17-valkey-7.2/Dockerfile"
							changeset "Makefile"
							changeset "ci/pipeline.properties"
						}
					}
					agent { label 'data' }
					options { timeout(time: 20, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-with-valkey-7.2:${p['java.main.tag']}", "--build-arg BASE=${p['docker.java.main.image']} --build-arg VERSION=${p['docker.redis.7.version']} -f ci/openjdk17-redis-7.2/Dockerfile .")
							docker.withRegistry(p['docker.registry'], p['docker.credentials']) {
								image.push()
							}
						}
					}
				}
				stage('Publish JDK 21 + Redis 6.2 Docker Image') {
					when {
						anyOf {
							changeset "ci/openjdk21-redis-6.2/Dockerfile"
							changeset "Makefile"
							changeset "ci/pipeline.properties"
						}
					}
					agent { label 'data' }
					options { timeout(time: 20, unit: 'MINUTES') }

					steps {
						script {
							def image = docker.build("springci/spring-data-with-redis-6.2:${p['java.next.tag']}", "--build-arg BASE=${p['docker.java.next.image']} --build-arg VERSION=${p['docker.redis.6.version']} -f ci/openjdk21-redis-6.2/Dockerfile .")
							docker.withRegistry(p['docker.registry'], p['docker.credentials']) {
								image.push()
							}
						}
					}
				}
			}
		}

		stage("test: baseline (main)") {
			when {
				beforeAgent(true)
				anyOf {
					branch(pattern: "main|(\\d\\.\\d\\.x)", comparator: "REGEXP")
					not { triggeredBy 'UpstreamCause' }
				}
			}
			agent {
				label 'data'
			}
			options { timeout(time: 30, unit: 'MINUTES') }
			environment {
				ARTIFACTORY = credentials("${p['artifactory.credentials']}")
				DEVELOCITY_ACCESS_KEY = credentials("${p['develocity.access-key']}")
			}
			steps {
				script {
					docker.withRegistry(p['docker.proxy.registry'], p['docker.proxy.credentials']) {
						docker.image("springci/spring-data-with-redis-6.2:${p['java.main.tag']}").inside(p['docker.java.inside.docker']) {
							sh "PROFILE=none LONG_TESTS=true JENKINS_USER_NAME=${p['jenkins.user.name']} ci/test.sh"
						}
					}
				}
			}
		}

		stage("Test other configurations") {
			when {
				beforeAgent(true)
				anyOf {
					branch(pattern: "main|(\\d\\.\\d\\.x)", comparator: "REGEXP")
					not { triggeredBy 'UpstreamCause' }
				}
			}
			parallel {
				stage("test: native-hints") {
					agent {
						label 'data'
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					environment {
						ARTIFACTORY = credentials("${p['artifactory.credentials']}")
						DEVELOCITY_ACCESS_KEY = credentials("${p['develocity.access-key']}")
					}
					steps {
						script {
							docker.withRegistry(p['docker.proxy.registry'], p['docker.proxy.credentials']) {
								docker.image("springci/spring-data-with-redis-6.2:${p['java.main.tag']}").inside(p['docker.java.inside.docker']) {
									sh "PROFILE=runtimehints LONG_TESTS=false JENKINS_USER_NAME=${p['jenkins.user.name']} ci/test.sh"
								}
							}
						}
					}
				}
				stage("test: baseline (next)") {
					agent {
						label 'data'
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					environment {
						ARTIFACTORY = credentials("${p['artifactory.credentials']}")
						DEVELOCITY_ACCESS_KEY = credentials("${p['develocity.access-key']}")
					}
					steps {
						script {
							docker.withRegistry(p['docker.proxy.registry'], p['docker.proxy.credentials']) {
								docker.image("springci/spring-data-with-redis-6.2:${p['java.next.tag']}").inside(p['docker.java.inside.docker']) {
									sh "PROFILE=none LONG_TESTS=true JENKINS_USER_NAME=${p['jenkins.user.name']} ci/test.sh"
								}
							}
						}
					}
				}
				stage("test: Redis 7") {
					agent {
						label 'data'
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					environment {
					   	ARTIFACTORY = credentials("${p['artifactory.credentials']}")
						DEVELOCITY_ACCESS_KEY = credentials("${p['develocity.access-key']}")
					}
					steps {
						script {
							docker.withRegistry(p['docker.proxy.registry'], p['docker.proxy.credentials']) {
								docker.image("springci/spring-data-with-redis-7.2:${p['java.main.tag']}").inside(p['docker.java.inside.docker']) {
									sh "PROFILE=none LONG_TESTS=true JENKINS_USER_NAME=${p['jenkins.user.name']} ci/test.sh"
								}
							}
						}
					}
				}

				stage("test: Valkey 7") {
					agent {
						label 'data'
					}
					options { timeout(time: 30, unit: 'MINUTES') }
					environment {
					   	ARTIFACTORY = credentials("${p['artifactory.credentials']}")
						DEVELOCITY_ACCESS_KEY = credentials("${p['develocity.access-key']}")
					}
					steps {
						script {
							docker.withRegistry(p['docker.proxy.registry'], p['docker.proxy.credentials']) {
								docker.image("springci/spring-data-with-valkey-7.2:${p['java.main.tag']}").inside(p['docker.java.inside.docker']) {
									sh "PROFILE=none LONG_TESTS=true JENKINS_USER_NAME=${p['jenkins.user.name']} ci/test.sh"
								}
							}
						}
					}
				}
			}
		}

		stage('Release to artifactory') {
			when {
				beforeAgent(true)
				anyOf {
					branch(pattern: "main|(\\d\\.\\d\\.x)", comparator: "REGEXP")
					not { triggeredBy 'UpstreamCause' }
				}
			}
			agent {
				label 'data'
			}
			options { timeout(time: 20, unit: 'MINUTES') }

			environment {
				ARTIFACTORY = credentials("${p['artifactory.credentials']}")
				DEVELOCITY_ACCESS_KEY = credentials("${p['develocity.access-key']}")
			}

			steps {
				script {
					docker.withRegistry(p['docker.proxy.registry'], p['docker.proxy.credentials']) {
						docker.image(p['docker.java.main.image']).inside(p['docker.java.inside.docker']) {
							sh 'MAVEN_OPTS="-Duser.name=' + "${p['jenkins.user.name']}" + ' -Duser.home=/tmp/jenkins-home" ' +
									"./mvnw -s settings.xml -Pci,artifactory " +
									"-Ddevelocity.storage.directory=/tmp/jenkins-home/.develocity-root " +
									"-Dartifactory.server=${p['artifactory.url']} " +
									"-Dartifactory.username=${ARTIFACTORY_USR} " +
									"-Dartifactory.password=${ARTIFACTORY_PSW} " +
									"-Dartifactory.staging-repository=${p['artifactory.repository.snapshot']} " +
									"-Dartifactory.build-name=spring-data-redis " +
									"-Dartifactory.build-number=spring-data-redis-${BRANCH_NAME}-build-${BUILD_NUMBER} " +
									"-Dmaven.repo.local=/tmp/jenkins-home/.m2/spring-data-redis " +
									"-Dmaven.test.skip=true clean deploy -U -B"
						}
					}
				}
			}
		}
	}

	post {
		changed {
			script {
				emailext(
						subject: "[${currentBuild.fullDisplayName}] ${currentBuild.currentResult}",
						mimeType: 'text/html',
						recipientProviders: [[$class: 'CulpritsRecipientProvider'], [$class: 'RequesterRecipientProvider']],
						body: "<a href=\"${env.BUILD_URL}\">${currentBuild.fullDisplayName} is reported as ${currentBuild.currentResult}</a>")
			}
		}
	}
}
