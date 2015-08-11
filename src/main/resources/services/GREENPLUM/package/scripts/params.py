from resource_management import *
import os

config = Script.get_config()

hdfs_supergroup = config["configurations"]["hdfs-site"]["dfs.permissions.superusergroup"]
user_group      = "hadoop"
greenplum_standby = None
with open("/tmp/gpdb-configs", "w") as fh:
  fh.write(str(config))

with open("/tmp/gpdb-clusterhostinfo", "w") as fh:
  fh.write(str(config["clusterHostInfo"]))

greenplum_master_dir = '/data/greenplum/master'
greenplum_data_dir   = '/data/greenplum/segments'
greenplum_master_dbid_path_suffix = '/gpseg-1/gp_dbid'
greenplum_tmp_dir    = '/tmp/greenplum/'
greenplum_user       = 'gpadmin'
greenplum_group      = 'gpadmin'
greenplum_gphome     = '/usr/local/greenplum-db/'
greenplum_bashrc = "{0}/.bashrc"
greenplum_profile    = "{0}/.greenplum-profile.sh"
source_command_tmpl = "echo 'source " + greenplum_profile + "' >> " + greenplum_bashrc
already_appended_command_tmpl = "cat " + greenplum_bashrc + " | grep 'source " + greenplum_profile + "'"
limits_conf_dir = "/etc/security/limits.d"
sysctl_conf_dir = "/etc/sysctl.d"
root_user = "root"

#Suse doesn't support loading values from /etc/sysctl.d, that is why we are touching this system level file
sysctl_conf_suse = "/etc/sysctl.conf"

greenplum_sysctl_conf_tmp = "/tmp/greenplum.conf"
greenplum_sysctl_conf_backup = "/etc/sysctl.conf.backup.{0}"

if config["commandType"] == 'EXECUTION_COMMAND':
  hdfs_superuser  = config["configurations"]["hdfs-site"]["dfs.cluster.administrators"].strip()
  dfs_url       = config["configurations"]["core-site"]["fs.defaultFS"].replace("hdfs://", "")
  greenplum_master   = config["clusterHostInfo"]["gpdbmaster_hosts"][0]
  greenplum_segments = config["clusterHostInfo"]["gpdbsegment_hosts"]
  
  if "gpdbstandby_hosts" in config["clusterHostInfo"]: # existence of the key should be explicitly checked. Otherwise, (like 'if config["clusterHostInfo"]["greenplumstandby_hosts"]:') throws an error from config_dictionary.py's UnknownConfiguration class
    if len(config["clusterHostInfo"]["gpdbstandby_hosts"]) > 0:
      greenplum_standby = config["clusterHostInfo"]["gpdbstandby_hosts"][0]
  greenplum_master = config["clusterHostInfo"]["gpdbmaster_hosts"][0]

  greenplum_password = "gpadmin"
  greenplum_cluster_name = "hackathon-demo"
  greenplum_master_port = 5432
  greenplum_segment_base_port=40000
  greenplum_temp_directory = ""
  set_os_parameters = False
  skip_preinstall_verification = True
  seg_prefix = "gpseg"
  hostname = config['hostname']

  greenplum_site_config = config["configurations"].get("greenplum-site")
  if greenplum_site_config:
    if greenplum_site_config.get("greenplum.user.password"):
      greenplum_password = greenplum_site_config.get("greenplum.user.password").strip()

    if greenplum_site_config.get("greenplum.master.directory"):
      greenplum_master_dir = greenplum_site_config.get("greenplum.master.directory").strip()

    if greenplum_site_config.get("greenplum.segment.data.directory"):
      greenplum_data_dir = greenplum_site_config.get("greenplum.segment.data.directory").strip()
      
    if greenplum_site_config.get("greenplum.cluster.name"):
      greenplum_cluster_name = greenplum_site_config.get("greenplum.cluster.name")

    if greenplum_site_config.get("greenplum.segment.base.port"):
      greenplum_segment_base_port = int(greenplum_site_config.get("greenplum.segment.base.port"))

    if greenplum_site_config.get("greenplum.master.port"):
      greenplum_master_port = int(greenplum_site_config.get("greenplum.master.port"))

    if greenplum_site_config.get("set.os.parameters"):
      set_os_parameters = greenplum_site_config.get("set.os.parameters").strip().lower() == "true"

    if greenplum_site_config.get("skip.preinstall.verification"):
      skip_preinstall_verification = greenplum_site_config.get("skip.preinstall.verification").strip().lower() == "true"

    if greenplum_site_config.get("sysctl.vm.overcommit_memory"):
      sysctl_vm_overcommit_memory = greenplum_site_config.get("sysctl.vm.overcommit_memory").strip()

  segments_per_node = len(greenplum_data_dir.split())
  greenplum_master_data_dir = os.path.join(greenplum_master_dir, seg_prefix + "-1")
  postmaster_opts_filepath = os.path.join(greenplum_master_data_dir, "postmaster.opts")
  master_hosts = [greenplum_master]
  if greenplum_standby is not None:
    master_hosts.append(greenplum_standby)
