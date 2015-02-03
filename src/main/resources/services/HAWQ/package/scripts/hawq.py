from resource_management import *
from Hardware import Hardware
from verifications import Verifications
import specs
import os
import pwd

def hawq_user_exists():
  import params
  try:
      pwd.getpwnam(params.hawq_user)
      return True
  except KeyError:
      return False

def system_verification(env, component):
  import params
  if params.skip_preinstall_verification:
    return

  hardware = Hardware().get()
  Logger.info("Fluffy: " + str(hardware))
  requirements = specs.requirements
  verify = Verifications(hardware, requirements)

  if component == "master":
    verify.master_preinstall_checks()
  if component == "segment":
    verify.segment_preinstall_checks()

  if verify.get_messages()>0:
    message = "Host system verification failed:\n\n"
    message += "(NOTE: To skip preinstall check (e.g. installing on test cluster), "
    message += "set skip.preinstall.verification to TRUE in 'Custom hawq-site' at "
    message += "the configuration step during the install.)\n\n"
    message += '\n'.join(verify.get_messages())
    raise Fail(message)

def common_setup(env):
  import params
  import crypt

  # if hawq admin user already exists then skip user/group create step
  if not hawq_user_exists():
    Group(params.hawq_group, ignore_failures = True)
    User(params.hawq_user,
        gid=params.hawq_group,
        password=crypt.crypt(params.hawq_password),
        groups=[params.hawq_group, params.user_group],
        ignore_failures = True)

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


def standby_configure(env):
    import params
    Directory([params.hawq_master_dir],
          owner=params.hawq_user,
          group=params.hawq_group,
          recursive=True)

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

def master_dbinit(env=None):
  import params
  if not os.path.exists(params.hawq_master_dbid_path):
    if params.security_enabled:
      kinit = "/usr/bin/kinit -kt /etc/security/keytabs/hdfs.headless.keytab hdfs;"
      cmd_setup_dir = "hdfs dfs -mkdir -p /user/gpadmin && hdfs dfs -chown -R gpadmin:gpadmin /user/gpadmin && hdfs dfs -chmod 777 /user/gpadmin;"
      cmd_setup_dir += "hdfs dfs -mkdir -p /gpsql && hdfs dfs -chown -R postgres:gpadmin /gpsql && hdfs dfs -chmod 755 /gpsql;"
      command = kinit+cmd_setup_dir
      Execute(command, user=params.hdfs_superuser, timeout=600)
    else:
      # create the gpsql directory first
      cmd_gpsql_dir =  "hdfs dfs -mkdir -p /gpsql && hdfs dfs -chown -R gpadmin:gpadmin /gpsql && hdfs dfs -chmod 755 /gpsql;"
      Execute(cmd_gpsql_dir, user=params.hdfs_superuser, timeout=600)

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
  pass
