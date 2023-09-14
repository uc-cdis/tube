#!/bin/bash

QUAY_BUILD_STATUS=""
ATTEMPTS=5

while getopts :r:b:d:a: option
do
  case "${option}"
      in
    r) REPOSITORY=${OPTARG} ;;
    b) BRANCH=${OPTARG} ;;
    d) DELAY=${OPTARG} ;;
    a) ATTEMPTS=${OPTARG} ;;
  esac
done

CURRENT_ATTEMPT=1
until [ "$QUAY_BUILD_STATUS" == "complete" ] || [ $CURRENT_ATTEMPT -gt $ATTEMPTS ]
do
  RESPONSE=$(curl -s -XGET https://quay.io/api/v1/repository/${REPOSITORY}/build/)
  FORMATED_RES=$(echo ${RESPONSE} | jq -r '.builds[].started |= (strptime("%A, %d %B %Y %H:%M:%S") | strftime("%Y-%m-%d %H:%M:%S"))')
  QUAY_BUILD_STATUS=`echo ${FORMATED_RES} | jq -r '.[] | map(select(.tags[] | contains("'${BRANCH}'")) | {started,phase}) | sort_by(.started) | last | .phase'`
  echo "Quay build status: ${QUAY_BUILD_STATUS}"
  echo "Attempt number: ${CURRENT_ATTEMPT}"
  CURRENT_ATTEMPT=$((CURRENT_ATTEMPT + 1))
  sleep $DELAY
done

echo $QUAY_BUILD_STATUS
