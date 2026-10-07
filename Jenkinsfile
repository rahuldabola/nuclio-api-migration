// Declarative Jenkins pipeline mirroring the GitHub Actions workflow (.github/workflows/ci.yml).
// CI runs it for real: the `jenkins-pipeline` job builds a Jenkins controller image with the
// needed tools (ci/jenkins.Dockerfile), creates a Pipeline-from-SCM job pointing at this repo,
// triggers a build and requires every stage to pass.
pipeline {
    agent any

    options {
        timestamps()
        timeout(time: 20, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '20'))
    }

    stages {
        stage('Python: lint and test') {
            steps {
                sh '''
                    python3 -m venv .venv
                    . .venv/bin/activate
                    pip install --quiet -r requirements-dev.txt
                    ruff check .
                    pytest -p no:warnings --junitxml=pytest-report.xml
                '''
            }
            post {
                always { junit allowEmptyResults: true, testResults: 'pytest-report.xml' }
            }
        }

        stage('Go probe: vet and test') {
            steps {
                dir('probe') {
                    sh 'go vet ./...'
                    sh 'go test -count=1 ./...'
                }
            }
        }

        stage('Validate Kubernetes manifests') {
            steps {
                sh 'kubectl kustomize k8s > rendered.yaml'
                sh 'test "$(grep -c "^kind:" rendered.yaml)" -ge 5'
            }
        }
    }

    post {
        failure { echo 'Pipeline failed: check the stage logs above.' }
    }
}
