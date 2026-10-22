pipeline {
    agent any

    options {
        timestamps()
        disableConcurrentBuilds()
    }

    environment {
        JAVA_HOME = '/usr/lib/jvm/java-17-openjdk-amd64'
        PATH = "${JAVA_HOME}/bin:${env.PATH}"
        PYSPARK_PYTHON = "${WORKSPACE}/.venv/bin/python"
        PYSPARK_DRIVER_PYTHON = "${WORKSPACE}/.venv/bin/python"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Setup Python Environment') {
            steps {
                sh '''
                    set -eu
                    python3 -m venv .venv
                    .venv/bin/python -m pip install -r requirements-ci.txt
                    .venv/bin/python --version
                    java -version
                '''
            }
        }

        stage('Python Syntax Validation') {
            steps {
                sh '''
                    set -eu
                    .venv/bin/python -m compileall -q src tests
                '''
            }
        }

        stage('Unit Tests') {
            steps {
                sh '''
                    set -eu
                    .venv/bin/python -m pytest tests/unit -q \
                        --tb=short \
                        --junitxml=pytest-results.xml
                '''
            }
        }

        stage('Pipeline Dry Run') {
            steps {
                sh '''
                    set -eu
                    .venv/bin/python -m src.pipeline.run_pipeline \
                        --run-id jenkins-test-001 \
                        --dry-run > pipeline-plan.json

                    .venv/bin/python - <<'PY'
import json

with open("pipeline-plan.json") as file:
    plan = json.load(file)

assert len(plan["batches"]) == 12
assert len(plan["silver_tables"]) == 7
assert len(plan["gold_tables"]) == 11
assert plan["execution_enabled"] is False

print("PASS: Jenkins pipeline plan validated")
print("PASS: Execution remains disabled")
PY
                '''
            }
        }
    }

    post {
        always {
            junit testResults: 'pytest-results.xml',
                  allowEmptyResults: true

            archiveArtifacts artifacts: 'pipeline-plan.json,pytest-results.xml',
                             allowEmptyArchive: true
        }
    }
}
