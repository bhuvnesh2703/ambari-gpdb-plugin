from resource_management import *
import os
from master_checks_helper import HawqMaster

config = Script.get_config()

hdfs_supergroup = config["configurations"]["hdfs-site"]["dfs.permissions.superusergroup"]
user_group      = "hadoop"
hawq_standby = None
security_enabled = config['configurations']['cluster-env']['security_enabled']

hawq_master_dir = '/data/hawq/master'
hawq_data_dir   = '/data/hawq/segments'
hawq_hdfs_data_dir = '/hawq_data'
hawq_master_dbid_path_suffix = '/gpseg-1/gp_dbid'
hawq_tmp_dir    = '/tmp/hawq/'
hawq_user       = 'gpadmin'
hawq_group      = 'gpadmin'
hawq_gphome     = '/usr/local/hawq'
hawq_bashrc      = "{0}/etc/hawq.bashrc".format(hawq_gphome)
limits_conf_dir = "/etc/security/limits.d"
sysctl_conf_dir = "/etc/sysctl.d"

#Suse doesn't support loading values from /etc/sysctl.d, that is why we are touching this system level file
sysctl_conf_suse = "/etc/sysctl.conf"

hawq_sysctl_conf_tmp = "/tmp/hawq.conf"
hawq_sysctl_conf_backup = "/etc/sysctl.conf.backup.{0}"

if config["commandType"] == 'EXECUTION_COMMAND':
  hdfs_superuser  = config["configurations"]["hdfs-site"]["dfs.cluster.administrators"].strip()
  dfs_url       = config["configurations"]["core-site"]["fs.defaultFS"].replace("hdfs://", "")
  hawq_master   = config["clusterHostInfo"]["hawqmaster_hosts"][0]
  hawq_segments = config["clusterHostInfo"]["hawqsegment_hosts"]
  
  ha_enabled = config['configurations']['hdfs-site'].get('dfs.nameservices')
  if ha_enabled:
    _nn_dfs_nameservices = config['configurations']['hdfs-site']['dfs.nameservices']
    _nn_nameservices_namenodes = config['configurations']['hdfs-site']['dfs.ha.namenodes.{0}'.format(_nn_dfs_nameservices)]
    _nn_nameservices_nodelist = _nn_nameservices_namenodes.split(',')
    _nn_nameservices_nn1_rpc_address = config['configurations']['hdfs-site']['dfs.namenode.rpc-address.{0}.{1}'.format(_nn_dfs_nameservices,_nn_nameservices_nodelist[0])]
    _nn_nameservices_nn1_http_address = config['configurations']['hdfs-site']['dfs.namenode.http-address.{0}.{1}'.format(_nn_dfs_nameservices,_nn_nameservices_nodelist[0])]
    _nn_nameservices_nn2_rpc_address = config['configurations']['hdfs-site']['dfs.namenode.rpc-address.{0}.{1}'.format(_nn_dfs_nameservices,_nn_nameservices_nodelist[1])]
    _nn_nameservices_nn2_http_address = config['configurations']['hdfs-site']['dfs.namenode.http-address.{0}.{1}'.format(_nn_dfs_nameservices,_nn_nameservices_nodelist[1])]
  
  if security_enabled:
    _nn_principal_name = config['configurations']['hdfs-site']['dfs.namenode.kerberos.principal']
    _hdfs_headless_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']
    _hdfs_headless_principal_name = config['configurations']['hadoop-env']['hdfs_principal_name']
    _realm_name = config['configurations']['cluster-env']['kerberos_domain']
    _hdfs_headless_princpal_name_with_realm = _hdfs_headless_principal_name + '@' + _realm_name

  if "hawqstandby_hosts" in config["clusterHostInfo"]: # existence of the key should be explicitly checked. Otherwise, (like 'if config["clusterHostInfo"]["hawqstandby_hosts"]:') throws an error from config_dictionary.py's UnknownConfiguration class
    if len(config["clusterHostInfo"]["hawqstandby_hosts"]) > 0:
      hawq_standby = config["clusterHostInfo"]["hawqstandby_hosts"][0]
  hawq_master = config["clusterHostInfo"]["hawqmaster_hosts"][0]

  hawq_password = "gpadmin"
  hawq_cluster_name = "hawq"
  hawq_master_port = 5432
  hawq_segment_base_port=40000
  hawq_temp_directory = ""
  hawq_keytab_file = "/etc/security/keytabs/hawq.service.keytab"
  set_os_parameters = False
  skip_preinstall_verification = True
  seg_prefix = "gpseg"

  hawq_site_config = config["configurations"].get("hawq-site")
  if hawq_site_config:
    if hawq_site_config.get("hawq.user.password"):
      hawq_password = hawq_site_config.get("hawq.user.password").strip()

    if hawq_site_config.get("hawq.master.directory"):
      hawq_master_dir = hawq_site_config.get("hawq.master.directory").strip()

    if hawq_site_config.get("hawq.data.directory"):
      hawq_data_dir = hawq_site_config.get("hawq.data.directory").strip()
      
    if hawq_site_config.get("hawq.hdfs.data.directory"):
      hawq_hdfs_data_dir = hawq_site_config.get("hawq.hdfs.data.directory").strip()

    if hawq_site_config.get("hawq.cluster.name"):
      hawq_cluster_name = hawq_site_config.get("hawq.cluster.name")

    if hawq_site_config.get("hawq.segment.base.port"):
      hawq_segment_base_port = int(hawq_site_config.get("hawq.segment.base.port"))

    if hawq_site_config.get("hawq.master.port"):
      hawq_master_port = int(hawq_site_config.get("hawq.master.port"))

    if hawq_site_config.get("hawq.temp.directory"):
      hawq_temp_directory = hawq_site_config.get("hawq.temp.directory").strip()

    if hawq_site_config.get("hawq.master.keytab.file"):
      hawq_keytab_file = hawq_site_config.get("hawq.master.keytab.file").strip()

    if hawq_site_config.get("set.os.parameters"):
      set_os_parameters = hawq_site_config.get("set.os.parameters").strip().lower() == "true"

    if hawq_site_config.get("skip.preinstall.verification"):
      skip_preinstall_verification = hawq_site_config.get("skip.preinstall.verification").strip().lower() == "true"

    if hawq_site_config.get("sysctl.vm.overcommit_memory"):
      sysctl_vm_overcommit_memory = hawq_site_config.get("sysctl.vm.overcommit_memory").strip()

  segments_per_node = len(hawq_data_dir.split())
  hawq_master_data_dir = os.path.join(hawq_master_dir, seg_prefix + "-1")
  master_obj = HawqMaster(hawq_master, hawq_master_data_dir, hawq_user)
  if hawq_standby is not None:
    standby_obj = HawqMaster(hawq_standby, hawq_master_data_dir, hawq_user)
