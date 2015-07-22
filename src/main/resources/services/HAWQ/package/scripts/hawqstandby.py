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

  def is_localhost_active_master(self):
    import params
    active_master_host = hawq.get_active_master_host()
    if active_master_host not in params.master_hosts:
      raise Exception("Host {0} not in the list of configured master hosts {1}".format(" and ".join(params.master_hosts)))
    if active_master_host == params.hostname:
      return True
    return False

  def start(self, env):
    self.configure(env)
    if self.is_localhost_active_master():
      hawq.system_verification(env, "master")
      hawq.start_hawq()
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
