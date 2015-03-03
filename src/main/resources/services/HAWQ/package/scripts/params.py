from resource_management import *

config = Script.get_config()

hdfs_supergroup = config["configurations"]["hdfs-site"]["dfs.permissions.superusergroup"]
user_group      = "hadoop"

hawq_standby = None
security_enabled = config['configurations']['cluster-env']['security_enabled']

hawq_master_dir = '/data/hawq/master'
hawq_data_dir   = '/data/hawq/segments'
hawq_master_dbid_path = '/data/hawq/master/gpseg-1/gp_dbid'
hawq_tmp_dir    = '/tmp/hawq/'
hawq_user       = 'gpadmin'
hawq_group      = 'gpadmin'
hawq_gphome   = '/usr/local/hawq/'

if config["commandType"] == 'EXECUTION_COMMAND':
  hdfs_superuser  = config["configurations"]["hdfs-site"]["dfs.cluster.administrators"].strip()
  dfs_url       = config["configurations"]["core-site"]["fs.defaultFS"].replace("hdfs://", "")
  hawq_master   = config["clusterHostInfo"]["hawqmaster_hosts"][0]
  hawq_segments = config["clusterHostInfo"]["hawqsegment_hosts"]
  hostname = dfs_url.split(':')[0]
  if security_enabled:
    _nn_principal_name = config['configurations']['hdfs-site']['dfs.namenode.kerberos.principal']
    _nn_principal_name = _nn_principal_name.replace('_HOST', hostname.lower())
    # e.g. nn/c6401.ambari.apache.org@EXAMPLE.COM

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
  skip_preinstall_verification = False

  hawq_site_config = config["configurations"].get("hawq-site")
  if hawq_site_config:
    if hawq_site_config.get("hawq.user.password"):
      hawq_password = hawq_site_config.get("hawq.user.password").strip()

    if hawq_site_config.get("hawq.master.directory"):
      hawq_master_dir = hawq_site_config.get("hawq.master.directory").strip()
      hawq_master_dbid_path = hawq_master_dir + '/gpseg-1/gp_dbid'

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

    if hawq_site_config.get("hawq.master.dbid.path"):
      hawq_master_dbid_path = hawq_site_config.get("hawq.master.dbid.path").strip()

    if hawq_site_config.get("hawq.temp.directory"):
      hawq_temp_directory = hawq_site_config.get("hawq.temp.directory").strip()

    if hawq_site_config.get("hawq.master.keytab.file"):
      hawq_keytab_file = hawq_site_config.get("hawq.master.keytab.file").strip()

    if hawq_site_config.get("skip.preinstall.verification"):
      skip_preinstall_verification = hawq_site_config.get("skip.preinstall.verification").strip().lower() == "true"
