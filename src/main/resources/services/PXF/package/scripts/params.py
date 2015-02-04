from resource_management import *

config = Script.get_config()

hdfs_superuser_group = config["configurations"]["hdfs-site"]["dfs.permissions.superusergroup"]
pxf_user = "pxf"
user_group = "hadoop"
security_enabled = config['configurations']['cluster-env']['security_enabled']
pxf_conf_dir = '/etc/gphd/pxf/conf'
tcserver_pid_file = "/var/gphd/pxf/pxf-service/logs/tcserver.pid"

pxf_keytab_file = "/etc/security/keytabs/pxf.service.keytab"

if config["commandType"] == 'EXECUTION_COMMAND':
  pxf_keytab_file = config["configurations"]["pxf-site"]["pxf.keytab.file"]
  java_home = config["hostLevelParams"]["java_home"]

