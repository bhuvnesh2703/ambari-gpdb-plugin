from resource_management import *

config = Script.get_config()

hdfs_supergroup = config["configurations"]["hdfs-site"]["dfs.permissions.superusergroup"]
user_group      = "hadoop"

hawq_standby = None
security_enabled = config['configurations']['cluster-env']['security_enabled']

hawq_master_dir = '/data/hawq/master'
hawq_data_dir   = '/data/hawq/segments'
hawq_master_dbid_path_suffix = '/gpseg-1/gp_dbid'
hawq_tmp_dir    = '/tmp/hawq/'
hawq_user       = 'gpadmin'
hawq_group      = 'gpadmin'
hawq_gphome     = '/usr/local/hawq/'
hawq_sysctl_conf = "{0}/etc/hawq.sysctl.conf".format(hawq_gphome)
hawq_limits_conf = "{0}/etc/hawq.limits.conf".format(hawq_gphome)
hawq_bashrc      = "{0}/etc/hawq.bashrc".format(hawq_gphome)
sysctl_vm_overcommit_memory = '1'

if config["commandType"] == 'EXECUTION_COMMAND':
  hdfs_superuser  = config["configurations"]["hdfs-site"]["dfs.cluster.administrators"].strip()
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

  if config["clusterHostInfo"]["hawqstandby_hosts"] and len(config["clusterHostInfo"]["hawqstandby_hosts"]) > 0:
      hawq_standby = config["clusterHostInfo"]["hawqstandby_hosts"][0]

  hawq_password = "gpadmin"
  hawq_cluster_name = "hawq"
  hawq_master_port = 5432
  hawq_segment_base_port=40000
  hawq_temp_directory = ""
  segments_per_node = 2
  hawq_temp_directory = ""
  hawq_keytab_file = "/etc/security/keytabs/hawq.service.keytab"
  set_os_parameters = False
  skip_preinstall_verification = True

  hawq_site_config = config["configurations"].get("hawq-site")
  if hawq_site_config:
    if hawq_site_config.get("hawq.user.password"):
      hawq_password = hawq_site_config.get("hawq.user.password").strip()

    if hawq_site_config.get("hawq.master.directory"):
      hawq_master_dir = hawq_site_config.get("hawq.master.directory").strip()

    if hawq_site_config.get("hawq.data.directory"):
      hawq_data_dir = hawq_site_config.get("hawq.data.directory").strip()

    if hawq_site_config.get("hawq.cluster.name"):
      hawq_cluster_name = hawq_site_config.get("hawq.cluster.name")

    if hawq_site_config.get("hawq.segment.base.port"):
      hawq_segment_base_port = int(hawq_site_config.get("hawq.segment.base.port"))

    if hawq_site_config.get("hawq.master.port"):
      hawq_master_port = int(hawq_site_config.get("hawq.master.port"))

    if hawq_site_config.get("hawq.segments.per.node"):
      segments_per_node = int(hawq_site_config.get("hawq.segments.per.node"))

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
