# Jenkins controller with the tools the Jenkinsfile's stages need (Python, Go, kubectl) and the
# pipeline plugins. Used by CI to execute the Jenkinsfile on a real Jenkins server.
FROM jenkins/jenkins:lts-jdk17

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv python3-pip git curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL https://go.dev/dl/go1.23.4.linux-amd64.tar.gz | tar -C /usr/local -xz \
 && curl -fsSL -o /usr/local/bin/kubectl https://dl.k8s.io/release/v1.31.0/bin/linux/amd64/kubectl \
 && chmod +x /usr/local/bin/kubectl
ENV PATH="/usr/local/go/bin:${PATH}"

USER jenkins
RUN jenkins-plugin-cli --plugins workflow-aggregator pipeline-model-definition git junit timestamper
