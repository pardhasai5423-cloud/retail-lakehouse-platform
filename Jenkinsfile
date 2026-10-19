pipeline {
    agent any

    options {
        timestamps()
        disableConcurrentBuilds()
        timeout(time: 30, unit: 'MINUTES')
    }

    environment {
        JAVA_HOME = '/usr/lib/jvm/java-17-openjdk-amd64'
        PYSPARK_PYTHON = 'python3'
        PYSPARK_DRIVER_PYTHON = 'python3'
    }

    stages {
        stage('Verify Environment') {
            steps {
                sh '''
                    set -eu
                    echo "=== PYTHON ==="
                    python3 --version

                    echo "=== JAVA ==="
                    "$JAVA_HOME/bin/java" -version

                    echo "=== GIT COMMIT ==="
                    git rev-parse --short HEAD
                '''
            }
        }

        stage('Prepare Python Environment') {
            steps {
                sh '''
                    set -eu
                    python3 -m venv .venv-ci
                    .venv-ci/bin/python -m pip install --upgrade pip
                    .venv-ci/bin/python -m pip install -r requirements.txt
                '''
            }
        }

        stage('Run Unit Tests') {
            steps {
                sh '''
                    set -eu
                    export PYSPARK_PYTHON="$PWD/.venv-ci/bin/python"
                    export PYSPARK_DRIVER_PYTHON="$PWD/.venv-ci/bin/python"

                    .venv-ci/bin/python -m pytest tests/unit/ -v \
                        --junitxml=unit-test-results.xml
                '''
            }
        }
    }

    post {
        always {
            junit allowEmptyResults: true, testResults: 'unit-test-results.xml'
        }

        success {
            echo 'Retail Lakehouse CI tests passed.'
        }

        failure {
            echo 'Retail Lakehouse CI failed. Check the console output.'
        }
    }
}
