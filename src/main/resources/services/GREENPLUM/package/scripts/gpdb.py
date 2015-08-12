from resource_management import *
from Hardware import Hardware
from verifications import Verifications
from common import subprocess_command_with_results, print_to_stderr_and_exit
import specs
import os
import subprocess
import pwd
import time
import filecmp
import active_master_helper
import checks_helper

def verify_segments_state(env, active_master_host):
  import params
  env.set_params(params)
  command = "source /usr/local/greenplum-db/greenplum_path.sh; gpstate -d {0}/gpseg-1".format(params.greenplum_master_dir)
  (retcode, out, err) = subprocess_command_with_results(params.greenplum_user, command, active_master_host)
  if retcode:
    print_to_stderr_and_exit("gpstate command returned non-zero result: {0}. Out: {1} Error: {2}".format(retcode, out, err))

  Logger.info("Service check results:\nOutput of gpstate -d {0}\n".format(params.greenplum_master_data_dir) + str(out) + "\n")

  if [status_line for status_line in out.split('\n') if (status_line.startswith('gpseg') and status_line.split(" ")[1 ] == 'd')]:
    print_to_stderr_and_exit("Service check detected that some of the Greenplum segments are down. run 'gpstate -t' on master for more info")

def check_port_conflict():
  import params
  import subprocess
  command = "netstat -tulpn | grep ':{0}\\b'".format(params.greenplum_master_port)
  (stdoutdata, _) = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True).communicate()
  if (len(stdoutdata)):
    # we have a conflict with the greenplum master port.
    message = "Conflict with Greenplum Master port. Either the service is already running or some other service is using port: {0}.\nProcess running on master port:\n{1}".format(params.greenplum_master_port, stdoutdata)
    print_to_stderr_and_exit(message)

def greenplum_user_exists():
  import params
  try:
      pwd.getpwnam(params.greenplum_user)
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
    #Update /etc/sysctl.d/greenplum.conf
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

  #Generate limits for greenplum user, all processes under this user will use those limits
  File('{0}/{1}.conf'.format(params.limits_conf_dir, params.greenplum_user),
     content=Template("greenplum.limits.conf.j2"),
     owner=params.greenplum_user,
     group=params.greenplum_group)

def update_sysctl_file():
  import params
  #Ensure sys ctl sub-directory exists
  Directory(params.sysctl_conf_dir,
    recursive=True,
    owner='root',
    group='root')

  sysctl_tmp_file = "{0}/greenplum.conf".format(params.greenplum_tmp_dir)
  sysctl_file = "{0}/greenplum.conf".format(params.sysctl_conf_dir)

  #Generate temporary file with kernel parameters needed by greenplum
  File(sysctl_tmp_file,
    content=Template("greenplum.sysctl.conf.j2"),
    owner=params.greenplum_user,
    group=params.greenplum_group)

  try:
    is_changed = not filecmp.cmp(sysctl_file, sysctl_tmp_file)
  except Exception:
    is_changed = True

  if is_changed:

    #Generate file with kernel parameters needed by greenplum, only if something has been changed by user
    File(sysctl_file,
      content=Template("greenplum.sysctl.conf.j2"),
      owner=params.greenplum_user,
      group=params.greenplum_group)

    #Reload kernel sysctl parameters from greenplum file. On system reboot this file will be automatically loaded.
    #Only if some parameters has been changed
    Execute("sysctl -e -p {0}/greenplum.conf".format(params.sysctl_conf_dir), timeout=600)

  #Wipe out temp file
  File(sysctl_tmp_file, action = 'delete')

def update_sysctl_file_suse():
    import params
    #Backup file
    backup_file_name = params.greenplum_sysctl_conf_backup.format(str(int(time.time())))
    try:
      #Generate file with kernel parameters needed by greenplum to temp file
      File(params.greenplum_sysctl_conf_tmp,
         content=Template("greenplum.sysctl.conf.j2"),
         owner=params.greenplum_user,
         group=params.greenplum_group)

      sysctl_file_dict = read_file_to_dict(params.sysctl_conf_suse)
      sysctl_file_dict_original = sysctl_file_dict.copy()
      greenplum_sysctl_dict = read_file_to_dict(params.greenplum_sysctl_conf_tmp)

      #Merge common system file with greenplum specific file
      sysctl_file_dict.update(greenplum_sysctl_dict)

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
      File(params.greenplum_sysctl_conf_tmp, action = 'delete')

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

  # if greenplum admin user already exists then skip user/group create step
  if not greenplum_user_exists():
    Group(params.greenplum_group, ignore_failures = True)
    User(params.greenplum_user,
        gid=params.greenplum_group,
        password=crypt.crypt(params.greenplum_password, "salt"),
        groups=[params.greenplum_group, params.user_group],
        ignore_failures = True)

  #GPHOME owned by gpadmin
  command="chown -R %s:%s %s" % (params.greenplum_user, params.greenplum_group, params.greenplum_gphome)
  Execute(command, timeout=600)

  Directory([params.greenplum_data_dir.split(), params.greenplum_tmp_dir],
            owner=params.greenplum_user,
            group=params.greenplum_group,
            recursive=True)

  if params.greenplum_temp_directory:
     Directory(params.greenplum_temp_directory.split(),
                owner=params.greenplum_user,
                group=params.greenplum_group,
                recursive=True)

  if params.set_os_parameters:
    set_osparams(env)

def standby_configure(env):
    import params
    Directory([params.greenplum_master_dir],
          owner=params.greenplum_user,
          group=params.greenplum_group,
          recursive=True)
    command = "echo {0} > {1}/master-dir".format(params.greenplum_master_dir, os.path.expanduser('~' + params.greenplum_user))
    Execute(command, user=params.greenplum_user, timeout=600)

def master_configure(env):
  import params
  greenplum_home = os.path.expanduser('~' + params.greenplum_user)

  Directory([params.greenplum_master_dir],
            owner=params.greenplum_user,
            group=params.greenplum_group,
            recursive=True)

  File("{0}/gpinitsystem_config".format(params.greenplum_tmp_dir),
     content=Template("gpinit.j2"),
     owner=params.greenplum_user,
     group=params.greenplum_group)

  File("{0}/hostfile".format(params.greenplum_tmp_dir),
       owner=params.greenplum_user,
       group=params.greenplum_group,
       content=Template("hostfile.j2"))

  File(params.greenplum_profile.format(greenplum_home),
     content=Template("greenplum-profile.sh.j2"),
     owner=params.greenplum_user,
     group=params.greenplum_group)

  source_command = params.source_command_tmpl.format(greenplum_home)
  already_appended_command = params.already_appended_command_tmpl.format(greenplum_home)
  Execute(source_command, user=params.greenplum_user, timeout=600, not_if=already_appended_command)

  command = "echo {0} > {1}/master-dir".format(params.greenplum_master_dir, greenplum_home)
  Execute(command, user=params.greenplum_user, timeout=600)

def init_greenplum(env=None):
  import params
  # ex-keys with segment hosts
  source = "source /usr/local/greenplum-db/greenplum_path.sh;"
  try:
    cmd = "gpssh-exkeys -f {0}/hostfile -p {1}".format(params.greenplum_tmp_dir, params.greenplum_password)
    command = source + cmd
    Execute(command, user=params.greenplum_user, timeout=600)
  except:
    # ignore error for now since this can fail because greenplum user is created with a different password
    pass

  # exchange keys with standby host
  if params.greenplum_standby:
    try:
      cmd = "gpssh-exkeys -h {0} -p {1}".format(params.greenplum_standby, params.greenplum_password)
      command = source + cmd
      Execute(command, user=params.greenplum_user, timeout=600)
    except:
      # ignore error for now since this can fail because greenplum user is created with a different password
      pass

  # gpinit
  cmd = "gpinitsystem -a -c {0}/gpinitsystem_config -h {0}/hostfile".format(params.greenplum_tmp_dir)
  if params.greenplum_standby:
      cmd = cmd + " -s {0}".format(params.greenplum_standby)
  command = source + cmd
  try:
    Execute(command, user=params.greenplum_user, timeout=3600)
  except Fail as ex:
    if 'returned 1' in ex.message:
      print ex.message
    else:
      raise ex

def start_greenplum(env=None):
  with open("/tmp/active_master_host", "w") as fh:
    fh.write(str(active_master_helper.get_active_master_host()))
  if active_master_helper.is_localhost_active_master():
    check_port_conflict() # Proceed only if there is no port conflict
    if is_greenplum_initialized():
      execute_start_command(env=None)
    else:
      init_greenplum(env=None) # Init will start the database as well
  else:
    Logger.info("This host is not the active master, skipping requested operation.")

def is_greenplum_initialized():
  import params
  if params.greenplum_standby is None:
    return os.path.exists(params.greenplum_master_data_dir)
  return active_master_helper.is_datadir_existing_on_master_hosts()
  
def execute_start_command(env=None):
  import params
  command = "source /usr/local/greenplum-db/greenplum_path.sh; gpstart -a -d {0}/gpseg-1".format(params.greenplum_master_dir)
  Execute(command, user=params.greenplum_user, timeout=600)

def stop_greenplum(env=None):
  """
  Case 1: If database is running, gpsyncmaster process LISTENS on greenplum master port on standby
  [root@hdm1 ~]# netstat -tupln | egrep 5432
  tcp        0      0 0.0.0.0:5432                0.0.0.0:*                   LISTEN      279699/gpsyncmaster
  tcp        0      0 :::5432                     :::*                        LISTEN      279699/gpsyncmaster

  Case 2: If database is running, postgres process LISTENS on greenplum master port on active master
  [root@hdw3 ~]# netstat -tupln | egrep 5432
  tcp        0      0 0.0.0.0:5432                0.0.0.0:*                   LISTEN      380366/postgres
  tcp        0      0 :::5432                     :::*                        LISTEN      380366/postgres

  Based on the process (gpsyncagent vs postgres) we can identify the active master and execute stop operation on it.
  """
  import params
  command = "source /usr/local/greenplum-db/greenplum_path.sh; gpstop -af -d {0}/gpseg-1".format(params.greenplum_master_dir)
  # Note: {0}\s captures processes running on port 5432 and not on 5432[0-9]
  Execute(command, user=params.greenplum_user, timeout=600, only_if="netstat -tupln | egrep ':{0}\s' | egrep postgres".format(params.greenplum_master_port))

def metrics_start(env=None):
  import params
  gmetrics_path = os.path.join(env.config.basedir, 'scripts/metrics_service.py') 
  command = "crontab -l | {{ cat; echo \"* * * * * python {0} {1}\"; }} | crontab -".format(gmetrics_path, params.segments_per_node)
  Execute(command, user=params.greenplum_user, timeout=60)

def metrics_stop(env=None):
  import params
  command = "crontab -r"
  try:
    Execute(command, user=params.greenplum_user, timeout=60)
  except:
    pass

def segment_configure(env=None):
  import params
  # Write Greenplum segment directory in an output file with each directory in a new line
  # Example : Input = /data1/greenplum/segment /data2/greenplum/segment
  #           Output file will contain 2 lines each holding one directory present in the input 
  command = "echo -e {0} > {1}/segments-dir".format("\\\\n".join(params.greenplum_data_dir.split()), os.path.expanduser('~' + params.greenplum_user))
  Execute(command, user=params.greenplum_user, timeout=600)

def activate_standby(env):
  import params
  checks_helper.check_standby_activation_prereq()
  command = "source /usr/local/greenplum/greenplum_path.sh && export MASTER_DATA_DIRECTORY={0}/gpseg-1 && gpactivatestandby -a -f -d {0}/gpseg-1".format(params.greenplum_master_dir)
  Execute(command, user=params.greenplum_user, timeout=600)
