// Declarative Jenkins pipeline mirroring the GitHub Actions workflow (.github/workflows/ci.yml).
// Stages run in containers, so agents only need Docker (the Jenkins Docker Pipeline plugin).
// CI validates this file's syntax with Jenkins' own declarative linter (ci/jenkins-lint job).
pipeline {
    agent none

    options {
        timestamps()
        timeout(time: 20, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '20'))
    }

    stages {
        stage('Python: lint and test') {
            agent { docker { image 'python:3.11-slim' } }
            environment {
                HOME = "${WORKSPACE}"
                PIP_CACHE_DIR = "${WORKSPACE}/.pip-cache"
            }
            steps {
                sh 'pip install --user -r requirements-dev.txt'
                sh 'python -m ruff check .'
                sh 'python -m pytest -p no:warnings --junitxml=pytest-report.xml'
            }
            post {
                always { junit allowEmptyResults: true, testResults: 'pytest-report.xml' }
            }
        }

        stage('Go probe: vet and test') {
            agent { docker { image 'golang:1.23' } }
            environment {
                HOME = "${WORKSPACE}"
                GOCACHE = "${WORKSPACE}/.go-cache"
            }
            steps {
                dir('probe') {
                    sh 'go vet ./...'
                    sh 'go test -race -count=1 ./...'
                }
            }
        }

        stage('Validate Kubernetes manifests') {
            agent { docker { image 'registry.k8s.io/kubectl:v1.31.0'; args '--entrypoint=' } }
            steps {
                sh 'kubectl kustomize k8s > rendered.yaml'
                sh 'grep -c "^kind:" rendered.yaml'
            }
        }
    }

    post {
        failure { echo 'Pipeline failed: check the stage logs above.' }
    }
}
