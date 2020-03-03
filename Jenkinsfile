@Library('ecdc-pipeline')
import ecdcpipeline.ContainerBuildNode
import ecdcpipeline.PipelineBuilder

project = "python-streaming-data-types"

container_build_nodes = [
  'centos7-release': ContainerBuildNode.getDefaultContainerBuildNode('centos7'),
]

// Define number of old builds to keep.
num_artifacts_to_keep = '1'

// Set number of old builds to keep.
properties([[
  $class: 'BuildDiscarderProperty',
  strategy: [
    $class: 'LogRotator',
    artifactDaysToKeepStr: '',
    artifactNumToKeepStr: num_artifacts_to_keep,
    daysToKeepStr: '',
    numToKeepStr: num_artifacts_to_keep
  ]
]]);


pipeline_builder = new PipelineBuilder(this, container_build_nodes)
pipeline_builder.activateEmailFailureNotifications()

builders = pipeline_builder.createBuilders { container ->
  pipeline_builder.stage("${container.key}: Checkout") {
    dir(pipeline_builder.project) {
      scm_vars = checkout scm
    }
    container.copyTo(pipeline_builder.project, pipeline_builder.project)
  }  // stage

  pipeline_builder.stage("${container.key}: Conda") {
    container.sh """
      curl https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh --output miniconda.sh
      sh miniconda.sh -b -p /home/jenkins/miniconda
      /home/jenkins/miniconda/bin/conda update -n base -c defaults conda -y
      /home/jenkins/miniconda/bin/conda init bash
      export PYTHONPATH=
    """
  } // stage

  pipeline_builder.stage("${container.key}: Dependencies") {
    container.sh """
      export PYTHONPATH=
      export PATH=/home/jenkins/miniconda/bin:$PATH
      python --version
      python -m pip install --user -r ${project}/requirements.txt
      python -m pip install --user -r ${project}/requirements-dev.txt
    """
  } // stage

  pipeline_builder.stage("${container.key}: Test") {
    def test_output = "TestResults.xml"
    container.sh """
      export PYTHONPATH=
      export PATH=/home/jenkins/miniconda/bin:$PATH
      cd ${project}
      python -m tox -- --junitxml=${test_output}
    """
    container.copyFrom("${project}/${test_output}", ".")
    xunit thresholds: [failed(unstableThreshold: '0')], tools: [JUnit(deleteOutputFiles: true, pattern: '*.xml', skipNoTestFiles: false, stopProcessingIfError: true)]
  } // stage
}  // createBuilders

node {
  dir("${project}") {
    scm_vars = checkout scm
  }

  try {
    parallel builders
  } catch (e) {
    throw e
  }

  // Delete workspace when build is done
  cleanWs()
}
