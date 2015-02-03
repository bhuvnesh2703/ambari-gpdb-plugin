#!/bin/sh
DIR=`dirname "$0"`
DIR=`cd "${DIR}">/dev/null; pwd`

#This script will create the repo file named the same as the containing folder;
#For example, if the file located at /tmp/PHD-1.4.0, then the created repo file will be /etc/yum.repos.d/PHD-1.4.0.repo;
YUM_REPO_DIR=/etc/yum.repos.d
REPO_NAME=`basename ${DIR}`
FQDN=`hostname -f`

#Setup the www link so that this repo is accessible from other nodes
`ln -s ${DIR} /var/www/html/${REPO_NAME}`

if [ -w ${YUM_REPO_DIR} ]; then
  cat <<EOF >${YUM_REPO_DIR}/${REPO_NAME}.repo
[${REPO_NAME}]
name=${REPO_NAME}
baseurl=http://${FQDN}/${REPO_NAME}
gpgcheck=0
EOF

RETVAL=$?

else
  RETVAL=1
  MESSAGE="Unable to write to /etc/yum.repos.d/"
fi


RETVAL=$?
if [ ${RETVAL} -eq 0 ]; then
  echo "${REPO_NAME} YUM Repo file successfully created at ${YUM_REPO_DIR}/${REPO_NAME}.repo"
  echo "Use http://${FQDN}/${REPO_NAME} to access the repository."
else
  echo "Failed to created ${REPO_NAME} YUM Repo: ${MESSAGE}"
fi

