from resource_management import *

def init(env):
  import params
  #TODO add "init" action to Service resource https://github.com/Pivotal-Hadoop/ambari/blob/trunk/ambari-common/src/main/python/resource_management/core/resources/service.py#L38
  #TODO change Execute to Service
  command = "service pxf-service init"
  pxf_instance_dir_exists_cmd = "test -e {0}".format(params.pxf_instance_dir)
  #Run init only if pxf instance directory doesn't exists
  Execute(command, timeout=600, not_if=pxf_instance_dir_exists_cmd)

def setup_user_group(env):
  import params
  #Ensure pxf user exists and belongs to all required groups
  #Ensure default shell of pxf user is /bin/bash
  User(params.pxf_user, groups = [params.hdfs_superuser_group, params.fabric_group, params.user_group], shell='/bin/bash')

def grant_permissions(env):
  import params
  if params.security_enabled:
    command  = "chown %s:%s %s &&" % (params.pxf_user, params.user_group, params.pxf_keytab_file)
    command += "chmod 440 %s" % (params.pxf_keytab_file)
    Execute(command, timeout=600)
  # Ensure that instance directory is owned by pxf:pxf.
  # Directory() functions takes care of the permission only while executing makedirs, however if the directories are already available, it doesnot changes the permission, thus using chown here.
  # This behavior appears to be fixed in Ambari 2.0.
  command = "chown {0}:{0} -R {1}".format(params.pxf_user, params.pxf_instance_dir)
  Execute(command, timeout=600)

def generate_config_files(env):
  import params
  File("{0}/pxf-env.sh".format(params.pxf_conf_dir),
     content=Template("pxf-env.j2"))

  File("{0}/pxf-private.classpath".format(params.pxf_conf_dir),
       content=Template("pxf-private-classpath.j2"))

  if params.security_enabled:
    pxf_site_dict = dict(params.config['configurations']['pxf-site'])
    pxf_site_dict['pxf.service.kerberos.principal'] = params._pxf_principal_name
    pxf_site = ConfigDictionary(pxf_site_dict)
  else:
    pxf_site = params.config['configurations']['pxf-site']

  XmlConfig("pxf-site.xml",
    conf_dir=params.pxf_conf_dir,
    configurations=pxf_site,
    configuration_attributes=params.config['configuration_attributes']['pxf-site'])

def start(env):
  #TODO: Change Execute to Service after "service pxf-service start" will handle case,
  #when tcServer is up, but PXF webapp is down. As for now it returns error: "ERROR: Instance is already running as PID=12345."
  import params
  command = "service {0} restart".format(params.pxf_service_name)
  Execute(command, timeout=600)

def stop(env):
  import params
  Service(params.pxf_service_name, action="stop")

def status(env):
  import params
  try:
    Execute("service {0} status".format(params.pxf_service_name))
  except Exception:
    raise ComponentIsNotRunning()