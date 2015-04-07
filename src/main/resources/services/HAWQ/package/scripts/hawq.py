from resource_management import *
from Hardware import Hardware
from verifications import Verifications
import specs
import os
import subprocess
import pwd

def verify_segments_state(env):
  import params
  env.set_params(params)
  command = "su - {0} -c 'source /usr/local/hawq/greenplum_path.sh; gpstate -t -d {1}/gpseg-1'".format(params.hawq_user, params.hawq_master_dir)
  remote_hostname = None
  if not os.path.exists(params.hawq_master_dir):
    remote_hostname = params.hawq_master

  (retcode, out, err) = subprocess_command_with_results(command, remote_hostname)
  print (retcode, out, err)
  if retcode:
    raise Exception("gpstate command returned non-zero result: {0}. Out: {1} Error: {2}".format(retcode, out, err))

  if [status_line for status_line in out.split('\n') if (status_line.startswith('gpseg') and status_line.split(" ")[1 ] == 'd')]:
    raise Exception("Service check detected that some of the HAWQ segments are down. run 'gpstate -t' on master for more info")

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
  File(params.hawq_sysctl_conf,
     content=Template("hawq.sysctl.conf.j2"),
     owner=params.hawq_user,
     group=params.hawq_group)
  command = "cat %s >> /etc/sysctl.conf && sysctl -e -p &&" % params.hawq_sysctl_conf

  File(params.hawq_limits_conf,
     content=Template("hawq.limits.conf.j2"),
     owner=params.hawq_user,
     group=params.hawq_group)
  command += "cat %s >> /etc/security/limits.conf && ulimit -n 2900000" % params.hawq_limits_conf

  Execute(command, timeout=600)

def common_setup(env):
  import params
  import crypt

  # if hawq admin user already exists then skip user/group create step
  if not hawq_user_exists():
    Group(params.hawq_group, ignore_failures = True)
    User(params.hawq_user,
        gid=params.hawq_group,
        password=crypt.crypt(params.hawq_password, ''),
        groups=[params.hawq_group, params.user_group],
        ignore_failures = True)

  #GPHOME owned by gpadmin
  command="chown -R %s:%s %s" % (params.hawq_user, params.user_group, params.hawq_gphome)
  Execute(command, timeout=600)

  File("{0}/etc/hdfs-client.xml".format(params.hawq_gphome),
     content=Template("hdfs-client.j2"),
     owner=params.hawq_user,
     group=params.hawq_group)

  Directory([params.hawq_data_dir, params.hawq_tmp_dir],
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
    command = "echo {0} > /home/{1}/master-dir".format(params.hawq_master_dir, params.hawq_user)
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
  command = "cat %s >> /home/gpadmin/.bashrc" % params.hawq_bashrc
  Execute(command, user=params.hawq_user, timeout=600)

  command = "echo {0} > /home/{1}/master-dir".format(params.hawq_master_dir, params.hawq_user)
  Execute(command, user=params.hawq_user, timeout=600)

def master_dbinit(env=None):
  import params
  master_dbid_path = params.hawq_master_dir + params.hawq_master_dbid_path_suffix
  if not os.path.exists(master_dbid_path):
    if params.security_enabled:
      kinit = "/usr/bin/kinit -kt /etc/security/keytabs/hdfs.headless.keytab hdfs;"
      cmd_setup_dir = "hdfs dfs -mkdir -p /user/gpadmin && hdfs dfs -chown -R gpadmin:gpadmin /user/gpadmin && hdfs dfs -chmod 777 /user/gpadmin;"
      cmd_setup_dir += "hdfs dfs -mkdir -p /hawq_data && hdfs dfs -chown -R postgres:gpadmin /hawq_data && hdfs dfs -chmod 755 /hawq_data;"
      command = kinit+cmd_setup_dir
      Execute(command, user=params.hdfs_superuser, timeout=600)
    else:
      # create the hawq_data directory first
      cmd_hawq_data_dir =  "hdfs dfs -mkdir -p /hawq_data && hdfs dfs -chown -R gpadmin:gpadmin /hawq_data && hdfs dfs -chmod 755 /hawq_data;"
      Execute(cmd_hawq_data_dir, user=params.hdfs_superuser, timeout=600)

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
      Execute(command, user=params.hawq_user, timeout=600)
    except Fail as ex:
      if 'returned 1' in ex.message:
        print ex.message
        pass # gpinitsystem returns 1 when warnings are present, even if install is successful
      else:
        raise ex
    return False
  return True

def master_start(env=None):
  import params
  source = "source /usr/local/hawq/greenplum_path.sh;"
  if master_dbinit(env): # check we have initialized. If not do it. dbinit will also start
    cmd = "gpstart -a -d {0}/gpseg-1".format(params.hawq_master_dir)
    command = source + cmd
    Execute(command, user=params.hawq_user, timeout=600)

def master_stop(env=None):
  import params
  source = "source /usr/local/hawq/greenplum_path.sh;"
  cmd = "gpstop -af -d {0}/gpseg-1".format(params.hawq_master_dir)
  command = source + cmd
  Execute(command, user=params.hawq_user, timeout=600)

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
  command = "echo {0} > /home/{1}/segments-dir".format(params.hawq_data_dir, params.hawq_user)
  Execute(command, user=params.hawq_user, timeout=600)

def try_activate_standby(env):
  import params
  if not params.hawq_standby:
    raise Exception("Standby is not configured")

  source = "source /usr/local/hawq/greenplum_path.sh;"
  cmd = "export MASTER_DATA_DIRECTORY={0}/gpseg-1;gpactivatestandby -a -f -d {0}/gpseg-1".format(params.hawq_master_dir)
  command = source + cmd
  Execute(command, user=params.hawq_user, timeout=600)

def subprocess_command_with_results(cmd, remote_hostname=None):
  if remote_hostname:
    cmd = "ssh -o StrictHostKeyChecking=no {0} \" {1} \"".format(remote_hostname, cmd)
  process = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stdin=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             shell=True)
  (stdoutdata, stderrdata) = process.communicate()
  return process.returncode, stdoutdata, stderrdata
