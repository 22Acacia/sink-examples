#!/bin/bash

sudo /opt/google-cloud-sdk/bin/gcloud auth activate-service-account --key-file account.json
ret_var=$?
if [ $ret_var -ne 0 ]; then
  echo "gcloud auth command failed.  abort"
  exit $ret_var
fi

#  push the application image to gcr
sudo /opt/google-cloud-sdk/bin/gcloud docker push gcr.io/$GOOGLE_PROJECT/sink-examples-master
