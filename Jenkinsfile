#!/usr/bin/env groovy

image_name = "plaidcloud/plaidtools"

podTemplate(label: 'plaidtools',
  containers: [
    containerTemplate(name: 'docker', image: 'docker:18.09.9-git', ttyEnabled: true, command: 'cat'),
    containerTemplate(name: 'kubectl', image: "lachlanevenson/k8s-kubectl:v1.15.9", ttyEnabled: true, command: 'cat')
  ],
  serviceAccount: 'jenkins'
)
{
  node(label: 'plaidtools') {
    properties([
      parameters([
        booleanParam(name: 'no_cache', defaultValue: false, description: 'Adds --no-cache flag to docker build command(s).'),
        booleanParam(name: 'full_lint', defaultValue: true, description: 'Perform full lint on a PR build.')
      ])
    ])
    container('docker') {
      withCredentials([string(credentialsId: 'docker-server-ip', variable: 'host')]) {
        docker.withServer("$host", "docker-server") {
          withCredentials([dockerCert(credentialsId: 'docker-server', variable: "DOCKER_CERT_PATH")]) {
            docker.withRegistry("", "plaid-docker") {
              // Checkout source before doing anything else
              scm_map = checkout scm

              // When building from a PR event, we want to read the branch name from the CHANGE_BRANCH binding. This binding does not exist on branch events.
              CHANGE_BRANCH = env.CHANGE_BRANCH ?: scm_map.GIT_BRANCH.minus(~/^origin\//)

              docker_args = ''

              // Add any extra docker build arguments here.
              if (params.no_cache) {
                docker_args += '--no-cache'
              }

              stage('Build Image') {
                image = docker.build("${image_name}:test", "--pull ${docker_args} .")
              }

              stage('Run Linter') {
                if (CHANGE_BRANCH == 'master' || params.full_lint) {
                  image.withRun('-t', 'bash -c "pylint plaidtools -j 0 -f parseable -r no>pylint.log"') {c ->
                    sh """
                      docker wait ${c.id}
                      docker cp ${c.id}:/home/plaid/src/plaidtools/pylint.log pylint.log
                    """
                  }
                } else {
                  image.withRun('-t') {c ->
                    sh """
                      docker wait ${c.id}
                      docker cp ${c.id}:/home/plaid/src/plaidtools/pylint.log pylint.log
                    """
                  }
                }
                if (CHANGE_BRANCH == 'master') {
                  recordIssues tool: pyLint(pattern: 'pylint.log')
                } else {
                  recordIssues tool: pyLint(pattern: 'pylint.log'), qualityGates: [[threshold: 1, type: 'TOTAL_HIGH', unstable: true]]
                }
              }

              stage('Run Tests') {
                image.withRun("-t", "pytest") {c ->
                  sh """
                    docker wait ${c.id}
                    docker cp ${c.id}:/home/plaid/src/plaidtools/pytestresult.xml pytestresult.xml
                    docker cp ${c.id}:/home/plaid/src/plaidtools/coverage.xml coverage.xml
                  """
                }
                junit 'pytestresult.xml'
                cobertura coberturaReportFile: 'coverage.xml', onlyStable: false, failUnhealthy:false, failUnstable: false, failNoReports: false
              }

              if (CHANGE_BRANCH == 'master') {
                stage('Trigger Downstream Jobs') {
                  build job: 'auth-service/master', parameters: [booleanParam(name: 'no_cache', value: false)], wait: false
                  build job: 'cron/master', parameters: [booleanParam(name: 'no_cache', value: false)], wait: false
                  build job: 'data-explorer-service/master', parameters: [booleanParam(name: 'no_cache', value: false)], wait: false
                  build job: 'git-service/master', parameters: [booleanParam(name: 'no_cache', value: false)], wait: false
                  build job: 'plaid/master', parameters: [booleanParam(name: 'no_cache', value: false)], wait: false
                  build job: 'workflow-runner/master', parameters: [booleanParam(name: 'no_cache', value: false)], wait: false
                }
              }
            }
          }
        }
      }
    }
  }
}
