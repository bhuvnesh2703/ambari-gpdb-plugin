from resource_management import *
from Hardware import Hardware
from verifications import Verifications
from common import subprocess_command_with_results
import specs
import os
import subprocess
import pwd
import time
import filecmp
import active_master_helper
import custom_params

DFS_ALLOW_TRUNCATE_ERROR_MESSAGE = "dfs.allow.truncate property in hdfs-site.xml file should be set to True. Please review HAWQ installation guide for more information."

def verify_segments_state(env, active_master_host):
  import params
  env.set_params(params)
  command = "source /usr/local/hawq/greenplum_path.sh; gpstate -t -d {0}/gpseg-1".format(params.hawq_master_dir)
  (retcode, out, err) = subprocess_command_with_results(params.hawq_user, command, active_master_host)
  if retcode:
    raise Exception("gpstate command returned non-zero result: {0}. Out: {1} Error: {2}".format(retcode, out, err))

  Logger.info("Service check results:\nOutput of gpstate -t -d {0}\n".format(params.hawq_master_data_dir) + str(out) + "\n")

  if [status_line for status_line in out.split('\n') if (status_line.startswith('gpseg') and status_line.split(" ")[1 ] == 'd')]:
    raise Exception("Service check detected that some of the HAWQ segments are down. run 'gpstate -t' on master for more info")

def check_port_conflict():
  import params
  import subprocess
  command = "netstat -tulpn | grep ':{0}\\b'".format(params.hawq_master_port)
  (r,o) = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True).communicate()
  if (len(r)):
    # we have a conflict with the hawq master port.
    message = "Conflict with HAWQ Master port. Either the service is already running or some other service is using port: {0}.\nProcess running on master port:\n{1}".format(params.hawq_master_port, r)
    raise Exception(message)

def hawq_user_exists():
  import params
  try:
      pwd.getpwnam(params.hawq_user)
      return True
  except KeyError:
      return False

def system_verification(env, component):
  import params
  hardware = Hardware().get()
  requirements = specs.requirements
  verify = Verifications(hardware, requirements)
  verify.mandatory_checks(component)

  if not params.skip_preinstall_verification:
    verify.preinstall_checks(component)

  if verify.get_messages():
    raise Fail( verify.get_notice() )

def set_osparams(env):
  import params
  #Suse doesn't supports loading values from files in /etc/sysctl.d
  #Very few customers have Suse, so this should not affect most of customers
  if System.get_instance().os_family == "suse":
    #Update /etc/sysctl.conf
    update_sysctl_file_suse()
  else:
    #Update /etc/sysctl.d/hawq.conf
    update_sysctl_file()

  update_limits_file()

def update_limits_file():
  import params
  #Ensure limits directory exists
  Directory(params.limits_conf_dir,
            recursive=True,
            owner='root',
            group='root'
  )

  #Generate limits for hawq user, all processes under this user will use those limits
  File('{0}/{1}.conf'.format(params.limits_conf_dir, params.hawq_user),
     content=Template("hawq.limits.conf.j2"),
     owner=params.hawq_user,
     group=params.hawq_group)

def update_sysctl_file():
  import params
  #Ensure sys ctl sub-directory exists
  Directory(params.sysctl_conf_dir,
    recursive=True,
    owner='root',
    group='root')

  sysctl_tmp_file = "{0}/hawq.conf".format(params.hawq_tmp_dir)
  sysctl_file = "{0}/hawq.conf".format(params.sysctl_conf_dir)

  #Generate temporary file with kernel parameters needed by hawq
  File(sysctl_tmp_file,
    content=Template("hawq.sysctl.conf.j2"),
    owner=params.hawq_user,
    group=params.hawq_group)

  try:
    is_changed = not filecmp.cmp(sysctl_file, sysctl_tmp_file)
  except Exception:
    is_changed = True

  if is_changed:

    #Generate file with kernel parameters needed by hawq, only if something has been changed by user
    File(sysctl_file,
      content=Template("hawq.sysctl.conf.j2"),
      owner=params.hawq_user,
      group=params.hawq_group)

    #Reload kernel sysctl parameters from hawq file. On system reboot this file will be automatically loaded.
    #Only if some parameters has been changed
    Execute("sysctl -e -p {0}/hawq.conf".format(params.sysctl_conf_dir), timeout=600)

  #Wipe out temp file
  File(sysctl_tmp_file, action = 'delete')

def update_sysctl_file_suse():
    import params
    #Backup file
    backup_file_name = params.hawq_sysctl_conf_backup.format(str(int(time.time())))
    try:
      #Generate file with kernel parameters needed by hawq to temp file
      File(params.hawq_sysctl_conf_tmp,
         content=Template("hawq.sysctl.conf.j2"),
         owner=params.hawq_user,
         group=params.hawq_group)

      sysctl_file_dict = read_file_to_dict(params.sysctl_conf_suse)
      sysctl_file_dict_original = sysctl_file_dict.copy()
      hawq_sysctl_dict = read_file_to_dict(params.hawq_sysctl_conf_tmp)

      #Merge common system file with hawq specific file
      sysctl_file_dict.update(hawq_sysctl_dict)

      if sysctl_file_dict_original != sysctl_file_dict:

        #Backup file
        backup_file(params.sysctl_conf_suse, backup_file_name)

        #Write merged properties to file
        write_dict_to_file(sysctl_file_dict, params.sysctl_conf_suse)

        #Reload kernel sysctl parameters from /etc/sysctl.conf
        Execute("sysctl -e -p", timeout=600)

    except Exception as e:
      Logger.error("Error occurred while updating sysctl.conf file " + str(e))
      restore_file(backup_file_name, params.sysctl_conf_suse)
      raise Fail("Error occurred while updating sysctl.conf file " + str(e))
    finally:
      #Wipe out temp file
      File(params.hawq_sysctl_conf_tmp, action = 'delete')

def read_file_to_dict(file_name):
  result_dict = dict()
  try:
    f = open(file_name, "r")
    lines = f.readlines()
    #Filter lines, leave only key=values items
    lines = [item for item in lines if '=' in item]
    #Convert key=value list to dictionary
    result_dict = dict(item.split("=") for item in lines)
  finally:
    f.close()
    return result_dict

def write_dict_to_file(source_dict, dest_file):
  try:
    f = open(dest_file, "w")
    for property_key, property_value in source_dict.items():
      if property_value is not None:
        f.write("{0}={1}\n".format(property_key, property_value))
      else:
        f.write(property_key + "\n")
  finally:
    f.close()

def backup_file(source_file, backup_file):
  backup_command = "cp {0} {1}".format(source_file, backup_file)
  Execute(backup_command, timeout=600)
  Logger.info("{0} has been backed up to {1}".format(source_file, backup_file))

def restore_file(backup_file_name, destination_file_name):
  Logger.info("Restoring file {0} from {1}".format(destination_file_name, backup_file_name))
  restore_file_command = "mv {0} {1}".format(backup_file_name, destination_file_name)
  Execute(restore_file_command, timeout=600)

def common_setup(env):
  import params
  import crypt

  # if hawq admin user already exists then skip user/group create step
  if not hawq_user_exists():
    Group(params.hawq_group, ignore_failures = True)
    User(params.hawq_user,
        gid=params.hawq_group,
        password=crypt.crypt(params.hawq_password, "salt"),
        groups=[params.hawq_group, params.user_group],
        ignore_failures = True)

  #GPHOME owned by gpadmin
  command="chown -R %s:%s %s" % (params.hawq_user, params.user_group, params.hawq_gphome)
  Execute(command, timeout=600)

  File("{0}/etc/hdfs-client.xml".format(params.hawq_gphome),
     content=Template("hdfs-client.j2"),
     owner=params.hawq_user,
     group=params.hawq_group)

  Directory([params.hawq_data_dir.split(), params.hawq_tmp_dir],
            owner=params.hawq_user,
            group=params.hawq_group,
            recursive=True)

  if params.hawq_temp_directory:
     Directory(params.hawq_temp_directory.split(),
                owner=params.hawq_user,
                group=params.hawq_group,
                recursive=True)

  if params.set_os_parameters:
    set_osparams(env)

def standby_configure(env):
    import params
    Directory([params.hawq_master_dir],
          owner=params.hawq_user,
          group=params.hawq_group,
          recursive=True)
    command = "echo {0} > {1}/master-dir".format(params.hawq_master_dir, os.path.expanduser('~' + params.hawq_user))
    Execute(command, user=params.hawq_user, timeout=600)

def master_configure(env):
  import params
  if params.security_enabled:
    command  = "chown %s:%s %s &&" % (params.hawq_user, params.user_group, params.hawq_keytab_file)
    command += "chmod 400 %s" % (params.hawq_keytab_file)
    Execute(command, timeout=600)

  Directory([params.hawq_master_dir],
            owner=params.hawq_user,
            group=params.hawq_group,
            recursive=True)

  File("{0}/gpinitsystem_config".format(params.hawq_tmp_dir),
     content=Template("gpinit.j2"),
     owner=params.hawq_user,
     group=params.hawq_group)

  File("{0}/hostfile".format(params.hawq_tmp_dir),
       owner=params.hawq_user,
       group=params.hawq_group,
       content=Template("hostfile.j2"))

  File(params.hawq_bashrc,
     content=Template("hawq.bashrc.j2"),
     owner=params.hawq_user,
     group=params.hawq_group)
  command = "cat {0} >> {1}/.bashrc".format(params.hawq_bashrc, os.path.expanduser('~' + params.hawq_user))
  Execute(command, user=params.hawq_user, timeout=600)

  command = "echo {0} > {1}/master-dir".format(params.hawq_master_dir, os.path.expanduser('~' + params.hawq_user))
  Execute(command, user=params.hawq_user, timeout=600)

def is_truncate_exception_required(dfs_allow_truncate):
  return (not dfs_allow_truncate and custom_params.enforce_hdfs_truncate)

def init_hawq(env=None):
  import params
  dfs_allow_truncate = check_truncate_setting()
  if is_truncate_exception_required(dfs_allow_truncate):
    raise Exception(DFS_ALLOW_TRUNCATE_ERROR_MESSAGE)
  if params.security_enabled:
    kinit = "/usr/bin/kinit -kt {0} {1};".format(params._hdfs_headless_keytab, params._hdfs_headless_princpal_name_with_realm)
    cmd_setup_dir = "hdfs dfs -mkdir -p /user/gpadmin && hdfs dfs -chown -R gpadmin:gpadmin /user/gpadmin && hdfs dfs -chmod 777 /user/gpadmin;"
    cmd_setup_dir += "hdfs dfs -mkdir -p {0} && hdfs dfs -chown -R postgres:gpadmin {0} && hdfs dfs -chmod 755 {0};".format(params.hawq_hdfs_data_dir)
    command = kinit+cmd_setup_dir
    Execute(command, user=params.hdfs_superuser, timeout=600)
  else:
    # create the hawq_hdfs_data_dir directory first
    cmd_hawq_hdfs_data_dir =  "hdfs dfs -mkdir -p {0} && hdfs dfs -chown -R gpadmin:gpadmin {0} && hdfs dfs -chmod 755 {0};".format(params.hawq_hdfs_data_dir)
    Execute(cmd_hawq_hdfs_data_dir, user=params.hdfs_superuser, timeout=600)

  # ex-keys with segment hosts
  source = "source /usr/local/hawq/greenplum_path.sh;"
  try:
    cmd = "gpssh-exkeys -f {0}/hostfile -p {1}".format(params.hawq_tmp_dir, params.hawq_password)
    command = source + cmd
    Execute(command, user=params.hawq_user, timeout=600)
  except:
    # ignore error for now since this can fail because hawq user is created with a different password
    pass

  # exchange keys with standby host
  if params.hawq_standby:
    try:
      cmd = "gpssh-exkeys -h {0} -p {1}".format(params.hawq_standby, params.hawq_password)
      command = source + cmd
      Execute(command, user=params.hawq_user, timeout=600)
    except:
      # ignore error for now since this can fail because hawq user is created with a different password
      pass

  # gpinit
  cmd = "gpinitsystem -a -c {0}/gpinitsystem_config -h {0}/hostfile".format(params.hawq_tmp_dir)
  if params.hawq_standby:
      cmd = cmd + " -s {0}".format(params.hawq_standby)
  command = source + cmd
  try:
    Execute(command, user=params.hawq_user, timeout=3600)
  except Fail as ex:
    if 'returned 1' in ex.message:
      print ex.message
    else:
      raise ex
  if is_truncate_warning_required(dfs_allow_truncate):
    print "**WARNING** " + DFS_ALLOW_TRUNCATE_ERROR_MESSAGE

# The below function returns current state of parameters enable_secure_filesystem and krb_server_keyfile
def get_postgres_secure_param_statuses():
  import params
  enable_secure_filesystem = False
  krb_server_keyfile = False
  with open("{0}/gpseg-1/postgresql.conf".format(params.hawq_master_dir)) as fh:
    for line in fh.readlines():
      data_excluding_comments = line.partition('#')[0].strip('\n')
      if len(data_excluding_comments) != 0:
        if re.search('\s*enable_secure_filesystem\s*=\s*on\s*', data_excluding_comments):
          enable_secure_filesystem = True
        if re.search("\s*krb_server_keyfile\s*=\s*\'{0}\'\s*".format(params.hawq_keytab_file), data_excluding_comments):
          krb_server_keyfile = True
  return enable_secure_filesystem, krb_server_keyfile
                                                                            
def execute_hawq_cmd(cmd):
  import params
  source = "export MASTER_DATA_DIRECTORY={0}/gpseg-1; source /usr/local/hawq/greenplum_path.sh;".format(params.hawq_master_dir)
  command = source + cmd
  Execute(command, user=params.hawq_user, timeout=600)

def set_postgresql_conf(mode):
  import params
  try:
    execute_hawq_cmd("echo 'Y' | gpstart -m")
    execute_hawq_cmd("gpconfig --masteronly -c enable_secure_filesystem -v '{0}'".format(mode))
    if params.security_enabled:
      execute_hawq_cmd("gpconfig --masteronly -c krb_server_keyfile -v \"'{0}'\"".format(params.hawq_keytab_file))
    execute_hawq_cmd("gpstop -m")
  except:
    raise Exception("\nSecurity parameters cannot be set properly. Please verify postgresql.conf file for the parameters:\n1. enable_secure_filesystem\n2. krb_server_keyfile. \nIf hawq master is running, please stop it using 'gpstop -a' command before issuing start from Ambari UI.")

def set_security():
  import params
  enable_secure_filesystem, krb_server_keyfile = get_postgres_secure_param_statuses()
  if params.security_enabled:
    if not (enable_secure_filesystem and krb_server_keyfile):
      set_postgresql_conf('on')
    kinit = "/usr/bin/kinit -kt {0} {1};".format(params._hdfs_headless_keytab, params._hdfs_headless_princpal_name_with_realm)
    owner = "postgres:gpadmin"
  else:
    if enable_secure_filesystem:
      set_postgresql_conf('off')
    kinit = " "
    owner = "gpadmin:gpadmin"
  cmd_setup_dir = "hdfs dfs -chown -R {0} {1}".format(owner, params.hawq_hdfs_data_dir)
  command = kinit+cmd_setup_dir
  Execute(command, user=params.hdfs_superuser, timeout=600)

def start_hawq(env=None):
  import params
  if is_hawq_initialized():
    execute_start_command(env=None)
  else:
    init_hawq(env=None)

def is_hawq_initialized():
  import params
  if params.hawq_standby is None:
    return os.path.exists(params.hawq_master_data_dir)
  return active_master_helper.is_datadir_existing_on_master_hosts()
  
def check_truncate_setting():
  """
  Check dfs.allow.truncate in hdfs-site.xml and enforce_hdfs_truncate in custom_params.py for hawq start
  if dfs.allow.truncate=True --> starting hawq succeeds
  if dfs.allow.truncate=False
      and enforce_hdfs_truncate=False -> starting hawq succeeds with a warning.
      and enforce_hdfs_truncate=True -> starting hawq fails
  """
  config = Script.get_config()
  dfs_allow_truncate = False
  if "dfs.allow.truncate" in config["configurations"]["hdfs-site"]:
    dfs_allow_truncate=config["configurations"]["hdfs-site"]["dfs.allow.truncate"] in ["true", "True", True]
  return dfs_allow_truncate

def is_truncate_warning_required(dfs_allow_truncate):
  return (not dfs_allow_truncate and not custom_params.enforce_hdfs_truncate)

def execute_start_command(env=None):
  import params
  dfs_allow_truncate = check_truncate_setting()
  if is_truncate_exception_required(dfs_allow_truncate):
    raise Exception(DFS_ALLOW_TRUNCATE_ERROR_MESSAGE)
  set_security()
  command = "source /usr/local/hawq/greenplum_path.sh; gpstart -a -d {0}/gpseg-1".format(params.hawq_master_dir)
  Execute(command, user=params.hawq_user, timeout=600)
  if is_truncate_warning_required(dfs_allow_truncate):
    print "**WARNING** " + DFS_ALLOW_TRUNCATE_ERROR_MESSAGE

def stop_hawq(env=None):
  """
  Case 1: If database is running, gpsyncmaster process LISTENS on hawq master port on standby
  [root@hdm1 ~]# netstat -tupln | egrep 5432
  tcp        0      0 0.0.0.0:5432                0.0.0.0:*                   LISTEN      279699/gpsyncmaster
  tcp        0      0 :::5432                     :::*                        LISTEN      279699/gpsyncmaster

  Case 2: If database is running, postgres process LISTENS on hawq master port on active master
  [root@hdw3 ~]# netstat -tupln | egrep 5432
  tcp        0      0 0.0.0.0:5432                0.0.0.0:*                   LISTEN      380366/postgres
  tcp        0      0 :::5432                     :::*                        LISTEN      380366/postgres

  Based on the process (gpsyncagent vs postgres) we can identify the active master and execute stop operation on it.
  """
  import params
  command = "source /usr/local/hawq/greenplum_path.sh; gpstop -af -d {0}/gpseg-1".format(params.hawq_master_dir)
  # Note: {0}\s captures processes running on port 5432 and not on 5432[0-9]
  Execute(command, user=params.hawq_user, timeout=600, only_if="netstat -tupln | egrep ':{0}\s' | egrep postgres".format(params.hawq_master_port))

def metrics_start(env=None):
  import params
  gmetrics_path = os.path.join(env.config.basedir, 'scripts/metrics_service.py') 
  command = "crontab -l | {{ cat; echo \"* * * * * python {0} {1}\"; }} | crontab -".format(gmetrics_path, params.segments_per_node)
  Execute(command, user=params.hawq_user, timeout=60)

def metrics_stop(env=None):
  import params
  command = "crontab -r"
  try:
    Execute(command, user=params.hawq_user, timeout=60)
  except:
    pass

def segment_configure(env=None):
  import params
  # Write hawq segment directory in an output file with each directory in a new line
  # Example : Input = /data1/hawq/segment /data2/hawq/segment
  #           Output file will contain 2 lines each holding one directory present in the input 
  command = "echo -e {0} > {1}/segments-dir".format("\\\\n".join(params.hawq_data_dir.split()), os.path.expanduser('~' + params.hawq_user))
  Execute(command, user=params.hawq_user, timeout=600)

def try_activate_standby(env):
  import params
  if not params.hawq_standby:
    raise Exception("Standby is not configured")

  source = "source /usr/local/hawq/greenplum_path.sh;"
  cmd = "export MASTER_DATA_DIRECTORY={0}/gpseg-1;gpactivatestandby -a -f -d {0}/gpseg-1".format(params.hawq_master_dir)
  command = source + cmd
  Execute(command, user=params.hawq_user, timeout=600)
