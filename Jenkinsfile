def p = [:]
node {
	checkout scm
	p = readProperties interpolate: true, file: 'ci/pipeline.properties'
}

pipeline {
	agent none

	triggers {
		pollSCM 'H/10 * * * *'
		upstream(upstreamProjects: "spring-data-keyvalue/main", threshold: hudson.model.Result.SUCCESS)
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
							def image = docker.build("springci/spring-data-with-redis-6.2:${p['java.main.tag']}", "--build-arg BASE=${p['docker.java.main.image']} --build-arg REDIS=${p['docker.redis.6.version']} -f ci/openjdk17-redis-6.2/Dockerfile .")
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
							def image = docker.build("springci/spring-data-with-redis-6.2:${p['java.next.tag']}", "--build-arg BASE=${p['docker.java.next.image']} --build-arg REDIS=${p['docker.redis.6.version']} -f ci/openjdk20-redis-6.2/Dockerfile .")
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
			}
			steps {
				script {
					docker.image("harbor-repo.vmware.com/dockerhub-proxy-cache/springci/spring-data-with-redis-6.2:${p['java.main.tag']}").inside('-v $HOME:/tmp/jenkins-home') {
						sh 'PROFILE=none LONG_TESTS=true ci/test.sh'
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
					}
					steps {
						script {
							docker.image("harbor-repo.vmware.com/dockerhub-proxy-cache/springci/spring-data-with-redis-6.2:${p['java.main.tag']}").inside('-v $HOME:/tmp/jenkins-home') {
								sh 'PROFILE=runtimehints LONG_TESTS=false ci/test.sh'
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
					}
					steps {
						script {
							docker.image("harbor-repo.vmware.com/dockerhub-proxy-cache/springci/spring-data-with-redis-6.2:${p['java.next.tag']}").inside('-v $HOME:/tmp/jenkins-home') {
								sh 'PROFILE=none LONG_TESTS=true ci/test.sh'
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
			}

			steps {
				script {
					docker.image(p['docker.java.main.image']).inside(p['docker.java.inside.basic']) {
						sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -s settings.xml -Pci,artifactory ' +
								'-Dartifactory.server=https://repo.spring.io ' +
								"-Dartifactory.username=${ARTIFACTORY_USR} " +
								"-Dartifactory.password=${ARTIFACTORY_PSW} " +
								"-Dartifactory.staging-repository=libs-snapshot-local " +
								"-Dartifactory.build-name=spring-data-redis " +
								"-Dartifactory.build-number=${BUILD_NUMBER} " +
								'-Dmaven.test.skip=true clean deploy -U -B'
					}
				}
			}
		}
	}

	post {
		changed {
			script {
				slackSend(
						color: (currentBuild.currentResult == 'SUCCESS') ? 'good' : 'danger',
						channel: '#spring-data-dev',
						message: "${currentBuild.fullDisplayName} - `${currentBuild.currentResult}`\n${env.BUILD_URL}")
				emailext(
						subject: "[${currentBuild.fullDisplayName}] ${currentBuild.currentResult}",
						mimeType: 'text/html',
						recipientProviders: [[$class: 'CulpritsRecipientProvider'], [$class: 'RequesterRecipientProvider']],
						body: "<a href=\"${env.BUILD_URL}\">${currentBuild.fullDisplayName} is reported as ${currentBuild.currentResult}</a>")
			}
		}
	}
}
