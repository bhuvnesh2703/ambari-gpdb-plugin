#!/bin/sh

RHEL_APACHE_USER="apache"
SUSE_APACHE_USER="wwwrun"
export PATH=$PATH:/sbin

IS_RHEL=$(test -e /etc/redhat-release &>/dev/null && echo "true")
IS_SUSE=$(test -e /etc/SuSE-release &>/dev/null  && echo "true")

DIR=$(dirname "$0")
DIR=$(cd "${DIR}">/dev/null; pwd)

# Link the root of repodata directory
REPO_PATH=$(dirname $(readlink -nf $(find $DIR -name repodata | head -1)))
test -z "$REPO_PATH" && error_exit "repodata directory doesn't exist under $DIR. Please verify if the original tarball had a repodata directory inside it."

error_exit() {
  if [ $# -gt 0 ]; then 
    echo "[ERROR] $1"
  fi
  echo "Local repo setup failed. Please fix the above problem and rerun this script"
  exit -1
}

print_warn() {
  if [ $# -gt 0 ]; then 
    echo "[WARNING] $1"
  fi
}


check_privilege() {
  if [ "$EUID" -ne 0 ]; then
    error_exit "Please run as root or with sudo"
  fi
}

check_dir_access(){
  if [[ -n "$IS_RHEL" ]]; then
    if sudo -u $RHEL_APACHE_USER [ ! -r "$REPO_PATH" ]; then
      error_exit "$DIR is not accessible by $RHEL_APACHE_USER user. Please ensure that all the parent directories have atleast r+x permissions."
    fi
  elif [[ -n "$IS_SUSE" ]]; then
    if sudo -u $SUSE_APACHE_USER [ ! -r "$REPO_PATH" ]; then
      error_exit "$DIR is not accessible by $SUSE_APACHE_USER user. Please ensure that all the parent directories have atleast r+x permissions."
    fi
  fi
}

check_apache(){
  if [[ -n "$IS_RHEL" ]]; then

    yum list installed httpd>/dev/null 2>&1
    if [ $? -ne 0 ]; then
      error_exit "HTTPD is not installed. Please install using 'yum install httpd'."
    fi

    service httpd status >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      error_exit "httpd is not started. Please start httpd service using 'service httpd start'."
   fi

  elif [[ -n "$IS_SUSE" ]]; then

    zypper -n search --match-exact -i  apache2 >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      error_exit "Apache2 is not installed. Please install using 'zypper in apache2'."
    fi

    service apache2 status >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      error_exit "apache2 is not started. Please start the apache2 service using 'service apache2 start'."
    fi

  fi
}


check_apache_conf(){

  APACHE_CONF_FILE="/etc/httpd/conf/httpd.conf"
  DOCUMENT_ROOT=$1
  if [[ -n "$IS_SUSE" ]]; then
    APACHE_CONF_FILE="/etc/apache2/default-server.conf"
  fi

  grep -A20 "[[:space:]]*DocumentRoot[[:space:]]*\"$DOCUMENT_ROOT\"" $APACHE_CONF_FILE | egrep "^[[:space:]]*Options[[:space:]]*None" >/dev/null 2>&1
  if [ $? -eq 0 ]; then
    print_warn "FollowSymLinks has not been enabled in the $APACHE_CONF_FILE. Please enable FollowSymLinks and restart Apache/HTTPD Server before accessing the repository."
  fi

}

check_doc_root() {
  if [ ! -d $1 ]; then
    error_exit "$1 doesn't exist. If you have a custom DOCUMENT_ROOT for Apache please set the same using export DOCUMENT_ROOT=<custom dir>."
  fi
}


# Script needs to be run as root
check_privilege

# Check if the repodata directory has access to apache user
check_dir_access

# Check if apache is installed
check_apache


if [[ -n "$IS_RHEL" ]]; then
  if [ -z "$DOCUMENT_ROOT" ]; then
    DOCUMENT_ROOT="/var/www/html"
  fi
  REPO_DIR="/etc/yum.repos.d"
fi

if [[ -n "$IS_SUSE" ]]; then
  if [ -z "$DOCUMENT_ROOT" ]; then
    DOCUMENT_ROOT="/srv/www/htdocs"
  fi
  REPO_DIR="/etc/zypp/repos.d"
fi

# Check if the DOC ROOT directories exists. If it doesn't it is possible that the user might have a custom DOC_ROOT
check_doc_root $DOCUMENT_ROOT

# Check if apache or httdp conf has FollowSymLinks enabled
check_apache_conf $DOCUMENT_ROOT


REPO_NAME=$(basename ${DIR})
FQDN=$(hostname -f)
REPO_FILE_NAME=${REPO_NAME}

# For Ambari it is always ambari.repo
shopt -s nocasematch
if [[ $REPO_FILE_NAME == AMBARI* ]]; then
  REPO_FILE_NAME="ambari"
fi
shopt -u nocasematch


#Setup the www link so that this repo is accessible from other nodes
if [ ! -e ${DOCUMENT_ROOT}/${REPO_NAME} ]; then
  ln -sf ${REPO_PATH} ${DOCUMENT_ROOT}/${REPO_NAME}
  if [ $? -ne 0 ]; then
    error_exit "Unable to create a link at ${DOCUMENT_ROOT}/${REPO_NAME}. Please check the ownership and directory permissions."
  fi
fi

if [ -w ${REPO_DIR} ]; then
  cat <<EOF >${REPO_DIR}/${REPO_FILE_NAME}.repo
[${REPO_NAME}]
name=${REPO_NAME}
baseurl=http://${FQDN}/${REPO_NAME}
gpgcheck=0
EOF

else
  error_exit "Unable to write to ${REPO_DIR}. Please fix the ownership or directory permissions."
fi

echo "${REPO_NAME} Repo file successfully created at ${REPO_DIR}/${REPO_FILE_NAME}.repo."
echo "Use http://${FQDN}/${REPO_NAME} to access the repository."
exit 0


