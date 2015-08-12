#!/usr/bin/env python
from resource_management import *
import gpdb 

class GreenplumMaster(Script):
  def install(self, env):
    self.install_packages(env)
    self.configure(env)

  def configure(self, env):
    import params
    env.set_params(params)
    gpdb.common_setup(env)
    gpdb.master_configure(env)
    gpdb.system_verification(env, "master")

  def start(self, env):
    self.configure(env)
    gpdb.start_greenplum(env)

  def stop(self, env):
    gpdb.stop_greenplum(env)

  def status(self, env):
    import status_params
    check_process_status(status_params.pid_gpdbmaster)

if __name__ == "__main__":
  GreenplumMaster().execute()
