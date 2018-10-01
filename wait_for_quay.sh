#!/bin/bash

QUAY_BUILD_STATUS=""

while getopts r:b:d: option
do
  case "${option}"
      in
    r) REPOSITORY=${OPTARG} ;;
    b) BRANCH=${OPTARG} ;;
    d) DELAY=${OPTARG} ;;
  esac
done

until [ "$QUAY_BUILD_STATUS" == "complete" ]
do
  RESPONSE=$(curl -s -XGET https://quay.io/api/v1/repository/${REPOSITORY}/build/)
  QUAY_BUILD_STATUS=`echo ${RESPONSE} | jq -r '.[] | map(select(.tags[] | contains("'${BRANCH}'")) | {started,phase}) | sort_by(.started) | last | .phase'`
  echo "Quay build status: ${QUAY_BUILD_STATUS}"
  sleep $DELAY
done
