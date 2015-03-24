#!/usr/bin/env python
from resource_management import *
import hawq

class HawqStandby(Script):
  def install(self, env):
    self.install_packages(env)
    self.configure(env)

  def configure(self, env):
    import params
    env.set_params(params)
    hawq.common_setup(env)
    hawq.standby_configure(env)
    hawq.system_verification(env, "master")

  def start(self, env):
    pass

  def stop(self, env):
    pass

  def status(self, env):
    import status_params
    check_process_status(status_params.pid_hawqmaster)

  def activatestandby(self, env):
    import params
    env.set_params(params)
    hawq.try_activate_standby(env)

if __name__ == "__main__":
    HawqStandby().execute()
