from resource_management.libraries.functions.version import *
from resource_management import *

config = Script.get_config()

hdfs_superuser_group = config["configurations"]["hdfs-site"]["dfs.permissions.superusergroup"]
pxf_user = "pxf"
fabric_group = "vfabric"
user_group = config['configurations']['cluster-env']['user_group']
security_enabled = config['configurations']['cluster-env']['security_enabled']
pxf_instance_dir = "/var/gphd/pxf"
tcserver_pid_file = "/var/gphd/pxf/pxf-service/logs/tcserver.pid"

pxf_keytab_file = "/etc/security/keytabs/pxf.service.keytab"
_pxf_principal_name = ''

stack_name = str(config['hostLevelParams']['stack_name'])
stack_version_unformatted = str(config['hostLevelParams']['stack_version'])
if stack_name == 'HDP':
  stack_version = format_hdp_stack_version(stack_version_unformatted)
elif stack_name == 'PHD':
  stack_version = format_stack_version(stack_version_unformatted)

#hadoop params
if stack_name == 'HDP' and compare_versions(stack_version, '2.2') >= 0:
  hdfs_client_home = "/usr/hdp/current/hadoop-hdfs-client"
  mapreduce_libs_path = "/usr/hdp/current/hadoop-mapreduce-client"
  hadoop_home = "/usr/hdp/current/hadoop-client"
  hbase_home = "/usr/hdp/current/hbase-client"
  zookeeper_home = "/usr/hdp/current/zookeeper-client"
  hive_home = "/usr/hdp/current/hive-client"
elif stack_name == 'PHD' and compare_versions(stack_version, '3.0') >= 0:
  hdfs_client_home = "/usr/phd/current/hadoop-hdfs-client"
  mapreduce_libs_path = "/usr/phd/current/hadoop-mapreduce-client"
  hadoop_home = "/usr/phd/current/hadoop-client"
  hbase_home = "/usr/phd/current/hbase-client"
  zookeeper_home = "/usr/phd/current/zookeeper-client"
  hive_home = "/usr/phd/current/hive-client"

pxf_home = "/usr/lib/gphd/pxf"
pxf_conf_dir = '/etc/gphd/pxf/conf'
hadoop_conf_dir = "/etc/hadoop/conf"
hive_conf_dir = "/etc/hive/conf"
hbase_conf_dir = "/etc/hbase/conf"

if config["commandType"] == 'EXECUTION_COMMAND':
  java_home = config["hostLevelParams"]["java_home"]
  pxf_site_config = config["configurations"].get("pxf-site")
  if pxf_site_config:
    pxf_keytab_file = config["configurations"]["pxf-site"]["pxf.service.kerberos.keytab"]
  if security_enabled:
    _nn_principal_name = config['configurations']['hdfs-site']['dfs.namenode.kerberos.principal']
    # In AMBR 2.0, the column used to derive realm name has changed, so AMBR 1.7 and AMBR 2.0 has different derivation. 
    # Using the below hack to avoid incompatibilty issues between 2 version.
    _pxf_principal_name = _nn_principal_name.replace('nn', 'pxf')
    # TODO - In AMBR 2.0, realm derivation can be done as below.
    # _realm_name = config['configurations']['kerberos-env']['realm']
    # _pxf_principal_name = pxf_user + '/_HOST@' + _realm_name
