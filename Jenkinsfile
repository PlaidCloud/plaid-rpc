#!/usr/bin/env groovy
podTemplate(label: 'plaid-rpc',
  containers: [
    containerTemplate(name: 'build', image: "gcr.io/plaidcloud-build/tools/python-build:latest", ttyEnabled: true, command: 'cat', alwaysPullImage: true, workingDir: '/home/jenkins/agent')
  ],
  serviceAccount: 'jenkins',
  imagePullSecrets: ['gcr-key']
)
{
  node(label: 'plaid-rpc') {
    properties([
      [$class: 'JiraProjectProperty'], buildDiscarder(logRotator(artifactDaysToKeepStr: '', artifactNumToKeepStr: '10', daysToKeepStr: '', numToKeepStr: '50')),
      parameters([
        booleanParam(name: 'no_cache', defaultValue: true, description: 'Adds --no-cache flag to docker build command(s).'),
        booleanParam(name: 'skip_lint', defaultValue: false, description: 'Do not lint.'),
        booleanParam(name: 'full_lint', defaultValue: false, description: 'Lint all files.'),
        stringParam(name: 'target_lint_dir', defaultValue: 'plaidcloud', description: 'Name of directory to run linter against.')
      ])
    ])
    container('build') {
      scm_map = checkout([
        $class: 'GitSCM',
        branches: scm.branches,
        doGenerateSubmoduleConfigurations: false,
        extensions: [[$class: 'SubmoduleOption', disableSubmodules: false, parentCredentials: true, recursiveSubmodules: true, reference: '', trackingSubmodules: true]],
        submoduleCfg: [],
        userRemoteConfigs: scm.userRemoteConfigs
      ])

      branch = env.CHANGE_BRANCH ?: scm_map.GIT_BRANCH.minus(~/^origin\//)

      stage("Run Checks") {
        if (!params.skip_lint) {
          sh """
            lint --target-dir=$params.target_lint_dir --branch=$branch --full-lint=$params.full_lint
          """

          if (branch == 'master') {
            recordIssues tool: pyLint(pattern: 'pylint.log')
          } else {
            recordIssues tool: pyLint(pattern: 'pylint.log'), qualityGates: [[threshold: 1, type: 'TOTAL_HIGH', unstable: true]]
          }

          // Check licenses on all python packages.
          license_errors = sh (
            returnStatus: true,
            script: '''
              set +x 
              cat license-report.txt | grep "UNAUTHORIZED" > /dev/null
            '''
          ) == 0
          if (license_errors) {
              output = sh returnStdout: true, script: '''
                set +x 
                cat license-report.txt | grep "UNAUTHORIZED"
              '''
              echo "\nThe following python package licenses are unauthorized:\n\n$output"
              currentBuild.result = 'UNSTABLE'
          } else {
            echo "No licensing issues found."
          }
        }
      }
    }
  }
}
