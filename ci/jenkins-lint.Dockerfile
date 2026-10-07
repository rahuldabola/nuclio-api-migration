FROM jenkins/jenkins:lts-jdk17
RUN jenkins-plugin-cli --plugins workflow-aggregator pipeline-model-definition docker-workflow junit timestamper
