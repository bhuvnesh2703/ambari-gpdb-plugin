#!/usr/bin/env python
from resource_management import *
import gpdb 

class GreenplumStandby(Script):
  def install(self, env):
    self.install_packages(env)
    self.configure(env)

  def configure(self, env):
    import params
    env.set_params(params)
    gpdb.common_setup(env)
    gpdb.standby_configure(env)
    gpdb.system_verification(env, "master")

  def start(self, env):
    self.configure(env)
    gpdb.start_greenplum(env)

  def stop(self, env):
    gpdb.stop_greenplum(env)

  def status(self, env):
    import status_params
    check_process_status(status_params.pid_gpdbmaster)

  def activatestandby(self, env):
    import params
    env.set_params(params)
    gpdb.activate_standby(env)

if __name__ == "__main__":
    GreenplumStandby().execute()
