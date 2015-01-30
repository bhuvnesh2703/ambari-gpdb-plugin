"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

from resource_management import *

def init(env):
  import params
  File("{0}/pxf-env.sh".format(params.pxf_conf_dir),
     content=Template("pxf-env.j2"))

  File("{0}/pxf-private.classpath".format(params.pxf_conf_dir),
       content=Template("pxf-private-classpath.j2"))

  command = "service pxf-service init"
  Execute(command, timeout=600)

  command = "usermod -a -G {0} pxf".format(params.hdfs_superuser_group)
  Execute(command, timeout=600)

def start(env):
  import params
  command = "service pxf-service start"
  Execute(command, timeout=600)

def stop(env):
  import params
  command = "service pxf-service stop"
  Execute(command, timeout=600)

def status(env):
  import params
  check_process_status(params.tcserver_pid_file)
