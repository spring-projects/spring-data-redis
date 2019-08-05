pipeline {
    agent none

    triggers {
        pollSCM 'H/10 * * * *'
        upstream(upstreamProjects: "spring-data-keyvalue/1.2.x", threshold: hudson.model.Result.SUCCESS)
    }

    options {
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '14'))
    }

    stages {
        stage('Publish OpenJDK 8 + Redis 3.2 docker image') {
            when {
                anyOf {
                    changeset "ci/Dockerfile"
                    changeset "Makefile"
                }
            }
            agent { label 'data' }
            options { timeout(time: 20, unit: 'MINUTES') }

            steps {
                script {
                    def image = docker.build("springci/spring-data-openjdk8-with-redis-3.2", "-f ci/Dockerfile .")
                    docker.withRegistry('', 'hub.docker.com-springbuildmaster') {
                        image.push()
                    }
                }
            }
        }

        stage("Test") {
            when {
                anyOf {
                    branch '1.8.x'
                    not { triggeredBy 'UpstreamCause' }
                }
            }
            parallel {
                stage("test: baseline") {
                    agent {
                        docker {
                            image 'springci/spring-data-openjdk8-with-redis-3.2:latest'
                            label 'data'
                            args '-v $HOME/.m2:/tmp/spring-data-maven-repository'
                        }
                    }
                    steps {
                        sh 'rm -rf ?'

                        // Create link to directory with Redis binaries
                        sh 'ln -sf /work'

                        // Launch Redis in proper configuration
                        sh 'make start'

                        // Execute maven test
                        sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw clean test -DrunLongTests=true -U -B'

                        // Capture resulting exit code from maven (pass/fail)
                        sh 'RESULT=\$?'

                        // Shutdown Redis
                        sh 'make stop'

                        // Return maven results
                        sh 'exit \$RESULT'

                    }
                }
            }
        }

        stage('Release to artifactory') {
            when {
                branch 'issue/*'
                not { triggeredBy 'UpstreamCause' }
            }
            agent {
                docker {
                    image 'adoptopenjdk/openjdk8:latest'
                    label 'data'
                    args '-v $HOME:/tmp/jenkins-home'
                }
            }

            environment {
                ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
            }

            steps {
                sh 'rm -rf ?'
                sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pci,artifactory ' +
                        '-Dartifactory.server=https://repo.spring.io ' +
                        "-Dartifactory.username=${ARTIFACTORY_USR} " +
                        "-Dartifactory.password=${ARTIFACTORY_PSW} " +
                        "-Dartifactory.staging-repository=libs-snapshot-local " +
                        "-Dartifactory.build-name=spring-data-redis-1.8 " +
                        "-Dartifactory.build-number=${BUILD_NUMBER} " +
                        '-Dmaven.test.skip=true clean deploy -U -B'
            }
        }

        stage('Release to artifactory with docs') {
            when {
                branch '1.8.x'
            }
            agent {
                docker {
                    image 'adoptopenjdk/openjdk8:latest'
                    label 'data'
                    args '-v $HOME:/tmp/jenkins-home'
                }
            }

            environment {
                ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
            }

            steps {
                sh 'rm -rf ?'
                sh 'MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" ./mvnw -Pci,artifactory ' +
                        '-Dartifactory.server=https://repo.spring.io ' +
                        "-Dartifactory.username=${ARTIFACTORY_USR} " +
                        "-Dartifactory.password=${ARTIFACTORY_PSW} " +
                        "-Dartifactory.staging-repository=libs-snapshot-local " +
                        "-Dartifactory.build-name=spring-data-redis-1.8 " +
                        "-Dartifactory.build-number=${BUILD_NUMBER} " +
                        '-Dmaven.test.skip=true clean deploy -U -B'
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
