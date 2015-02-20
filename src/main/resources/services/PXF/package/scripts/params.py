from resource_management.libraries.functions.version import format_hdp_stack_version, compare_versions
from resource_management import *

config = Script.get_config()

hdfs_superuser_group = config["configurations"]["hdfs-site"]["dfs.permissions.superusergroup"]
pxf_user = "pxf"
user_group = "hadoop"
security_enabled = config['configurations']['cluster-env']['security_enabled']
tcserver_pid_file = "/var/gphd/pxf/pxf-service/logs/tcserver.pid"

pxf_keytab_file = "/etc/security/keytabs/pxf.service.keytab"

stack_version_unformatted = str(config['hostLevelParams']['stack_version'])
hdp_stack_version = format_hdp_stack_version(stack_version_unformatted)
stack_is_hdp22_or_further = hdp_stack_version != "" and compare_versions(hdp_stack_version, '2.2') >= 0

#hadoop params
if stack_is_hdp22_or_further:
  hdfs_client_home = "/usr/hdp/current/hadoop-hdfs-client"
  mapreduce_libs_path = "/usr/hdp/current/hadoop-mapreduce-client"
  hadoop_home = "/usr/hdp/current/hadoop-client"
  hbase_home = "/usr/hdp/current/hbase-client"
  zookeeper_home = "/usr/hdp/current/zookeeper-client"
  hive_home = "/usr/hdp/current/hive-client"
else:
  hdfs_client_home = "/usr/lib/hadoop-hdfs"
  mapreduce_libs_path = "/usr/lib/hadoop-mapreduce"
  hadoop_home = "/usr/lib/hadoop"
  hbase_home = "/usr/lib/hbase"
  zookeeper_home = "/usr/lib/zookeeper"
  hive_home = "/usr/lib/hive"

pxf_home = "/usr/lib/gphd/pxf"
pxf_conf_dir = '/etc/gphd/pxf/conf'
hadoop_conf_dir = "/etc/hadoop/conf"
hive_conf_dir = "/etc/hive/conf"

if config["commandType"] == 'EXECUTION_COMMAND':
  pxf_keytab_file = config["configurations"]["pxf-site"]["pxf.keytab.file"]
  _pxf_principal_name = ''
  if security_enabled:
	  _nn_principal_name = config['configurations']['hdfs-site']['dfs.namenode.kerberos.principal']
	  _pxf_principal_name = _nn_principal_name.replace('nn', 'pxf')
	  # e.g. pxf/_HOST@EXAMPLE.COM
  java_home = config["hostLevelParams"]["java_home"]

