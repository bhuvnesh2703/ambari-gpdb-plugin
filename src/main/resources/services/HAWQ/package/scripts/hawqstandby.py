#!/usr/bin/env python
from resource_management import *
import hawq
import active_master_helper

class HawqStandby(Script):
  def install(self, env):
    self.install_packages(env)
    self.configure(env)

  def configure(self, env):
    import params
    env.set_params(params)
    hawq.common_setup(env)
    hawq.standby_configure(env)

  def start(self, env):
    self.configure(env)
    if active_master_helper.is_localhost_active_master():
      hawq.system_verification(env, "master")
      hawq.start_hawq(env)
    else:
      Logger.info("This host is not the active master, skipping requested operation.")

  def stop(self, env):
    hawq.stop_hawq(env)

  def status(self, env):
    import status_params
    check_process_status(status_params.pid_hawqmaster)

  def activatestandby(self, env):
    import params
    env.set_params(params)
    hawq.try_activate_standby(env)

if __name__ == "__main__":
    HawqStandby().execute()
