pipeline {
    agent none

    triggers {
        pollSCM 'H/10 * * * *'
        upstream(upstreamProjects: "spring-data-commons/2.1.x", threshold: hudson.model.Result.SUCCESS)
    }

    options {
        disableConcurrentBuilds()
    }

    stages {
        stage("Test") {
            parallel {
                stage("test: baseline") {
                    agent {
                        docker {
                            image 'springci/spring-data-openjdk8-with-redis-5.0:latest'
                            args '-v $HOME/.m2:/root/.m2'
                        }
                    }
                    steps {
                        // Create link to directory with Redis binaries
                        sh 'ln -sf /work'

                        // Launch Redis in proper configuration
                        sh 'make start'

                        // Execute maven test
                        sh "./mvnw clean test -DrunLongTests=true -B"

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
            }
            agent {
                docker {
                    image 'adoptopenjdk/openjdk8:latest'
                    args '-v $HOME/.m2:/root/.m2'
                }
            }

            environment {
                ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
            }

            steps {
                sh "./mvnw -Pci,snapshot -Dmaven.test.skip=true clean deploy -B"
            }
        }

        stage('Release to artifactory with docs') {
            when {
                branch '2.1.x'
            }
            agent {
                docker {
                    image 'adoptopenjdk/openjdk8:latest'
                    args '-v $HOME/.m2:/root/.m2'
                }
            }

            environment {
                ARTIFACTORY = credentials('02bd1690-b54f-4c9f-819d-a77cb7a9822c')
            }

            steps {
                sh "./mvnw -Pci,snapshot -Dmaven.test.skip=true clean deploy -B"
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
