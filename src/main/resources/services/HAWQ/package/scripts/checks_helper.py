from resource_management import *
import subprocess
from common import subprocess_command_with_results, print_to_stderr_and_exit
import sys

def check_standby_activation_prereq():
  """
  Check the state of hawq standby master (i.e params.hawq_standby), and return an exception if it does not appear to be in a valid state.
  """
  import params
  GPACTIVATESTANDBY_INSTRUCTIONS = "\nSteps to execute gpactivatestandby.\n1. ssh to the standby host\n2. Login as gpadmin user: su - gpadmin\n3. Execute the command: gpactivatestandby -a -f -d {0}".format(params.hawq_master_data_dir)
  if not params.hawq_standby:
    print_to_stderr_and_exit("Standby is not configured in this cluster, activate standby command is used in clusters which has standby configured.")

  # Check if there is postgres process running on master port, if running it indicates that standby has already been activated to active master
  command = "netstat -tulpn | grep ':{0}\s' | grep postgres".format(params.hawq_master_port)
  returncode, stdoutdata, _ = subprocess_command_with_results(params.root_user, command, params.hostname)
  if returncode == 0:
    print_to_stderr_and_exit("Active hawq master process appears to be already running on host {0} on port {1}, standby activation is not required. Process details:\n{2}".format(params.hawq_standby, params.hawq_master_port, stdoutdata))

  # Check if postmaster.opts file exists, it helps to identify if hawq_standby is acting as standby
  if not os.path.isfile(params.postmaster_opts_filepath):
    EXCEPTION_MSG = "{0} file does not exists on the host {1}, cannot continue with activating standby. Please activate standby manually from command line until its fixed.".format(params.postmaster_opts_filepath, params.hostname) + GPACTIVATESTANDBY_INSTRUCTIONS + "\nNote: postmaster.opts is created automatically during start of hawq master."
    print_to_stderr_and_exit(EXCEPTION_MSG)

  # Read postmaster.opts file into a list
  with open(params.postmaster_opts_filepath, "r") as fh:
    postmaster_content = fh.read().split()

  """
  Raise exception if postmaster.opts content indicate that this host is not configured as standby

  Case 1. If contents of postmaster.opts has flag "-x", it indicates that it is not configured as a standby.
  Example contents of standby postmaster.opts:
    postgres "-D" "/data/hawq/master/gpseg-1" "-p" "5432" "-b" "12" "-C" "-1" "-z" "10" "-i"

  Case 2. If standby activation was triggered but it failed, content of postmaster.opts will include "-x" flag but with another option "gp_role=utility". In this case, we should allow gpactivatestandby to be executed again
  /usr/local/hawq-1.3.0.0/bin/postgres "-D" "/data/hawq/master/gpseg-1" "-p" "5433" "-b" "1" "-z" "1" "--silent-mode=true" "-i" "-M" "master" "-C" "-1" "-x" "0" "-c" "gp_role=utility"
  """
  if '"-x"' in postmaster_content and not '"gp_role=utility"' in postmaster_content:
    """
    raise Exception("Contents of {0} on host {1} indicate that it is not currently acting as standby hawq master.\nActivateStandby from UI can only be run if host {1} is acting as standby hawq master. Please verify if you have already activated it to active hawq master.\nIf host {2} is acting as standby hawq master and needs to be activated, please execute gpactivatestandby manually from command line.".format(params.postmaster_opts_filepath, params.hawq_standby, params.hawq_master) + GPACTIVATESTANDBY_INSTRUCTIONS )
    """
    print_to_stderr_and_exit("Contents of {0} on host {1} indicate that it is not currently acting as standby hawq master.\nActivateStandby from UI can only be run if host {1} is acting as standby hawq master. Please verify if you have already activated it to active hawq master.\nIf host {2} is acting as standby hawq master and needs to be activated, please execute gpactivatestandby manually from command line.".format(params.postmaster_opts_filepath, params.hawq_standby, params.hawq_master) + GPACTIVATESTANDBY_INSTRUCTIONS)
  print "hello"

  """
  If a lock file /tmp/.s.PGSQL.<master_port>.lock is found on hawq_master host, activate standby will fail stating that an active postgres process in running on it.
  Ensure that gpactivatestandby is triggered only if the lock file is not existing. If not checked, gpactivatestandby will be executed and the contents
  of postmaster.opts will get inconsistent impacting start / stop operation from ambari ui, so avoid such cases.
  """
  lock_file_name = "/tmp/.s.PGSQL.{0}.lock".format(params.hawq_master_port)
  # ssh to the hawq_master and identify if file exists or not
  if not active_master_helper.is_file_missing(params.hawq_master, lock_file_name):
    print_to_stderr_and_exit("Lock file /tmp/.s.PGSQL.{0}.lock exists on host {1} suggesting that active postgres process is running on it.\nIf hawq master process is running on host {1}, please stop the database before retrying activate standby.\nIf hawq master is not running on host {1}, please delete /tmp/.s.PGSQL.{0}, /tmp/.s.PGSQL.{0}.* and {2}/postmaster.pid files on it before retrying activate standby.".format(params.hawq_master_port, params.hawq_master, params.hawq_master_data_dir))
