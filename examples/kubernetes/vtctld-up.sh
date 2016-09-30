#!/bin/bash

# This is an example script that starts vtctld.

set -e

script_root=`dirname "${BASH_SOURCE}"`
source $script_root/env.sh

service_type=${VTCTLD_SERVICE_TYPE:-'ClusterIP'}
image=${IMAGE:-'vitess/lite'}
CELLS=${CELLS:-'test'}
VITESS_NAME=${VITESS_NAME:-'default'}
TEST_MODE=${TEST_MODE:-'0'}
VTCTLD_CONTROLLER_TEMPLATE=${VTCTLD_CONTROLLER_TEMPLATE:-'vtctld-controller-template.yaml'}
VTCTLD_SERVICE_TEMPLATE=${VTCTLD_SERVICE_TEMPLATE:-'vtctld-service-template.yaml'}

test_flags=`[[ $TEST_MODE -gt 0 ]] && echo '-enable_queries' || echo ''`

cells=`echo $CELLS | tr ',' ' '`

for cell in $cells; do
  echo "Creating vtctld $service_type service..."
  sed_script=""
  for var in service_type; do
    sed_script+="s,{{$var}},${!var},g;"
  done
  cat $VTCTLD_SERVICE_TEMPLATE | sed -e "$sed_script" | $KUBECTL create --namespace=$VITESS_NAME -f -

  echo "Creating vtctld replicationcontroller..."
  # Expand template variables
  sed_script=""
  for var in backup_flags test_flags cell image; do
    sed_script+="s,{{$var}},${!var},g;"
  done

  # Instantiate template and send to kubectl.
  cat $VTCTLD_CONTROLLER_TEMPLATE | sed -e "$sed_script" | $KUBECTL create --namespace=$VITESS_NAME -f -

  echo
  echo "To access vtctld web UI, start kubectl proxy in another terminal:"
  echo "  kubectl proxy --port=8001"
  echo "Then visit http://localhost:8001/api/v1/proxy/namespaces/$VITESS_NAME/services/vtctld:web/"
done