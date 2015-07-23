#!/usr/bin/env python
from resource_management import *
import hawq

class HawqMaster(Script):
  def install(self, env):
    self.install_packages(env)
    self.configure(env)

  def configure(self, env):
    import params
    env.set_params(params)
    hawq.common_setup(env)
    hawq.master_configure(env)
    hawq.system_verification(env, "master")

  def start(self, env):
    self.configure(env)
    hawq.start_hawq(env)

  def stop(self, env):
    hawq.stop_hawq(env)

  def status(self, env):
    import status_params
    check_process_status(status_params.pid_hawqmaster)

if __name__ == "__main__":
  HawqMaster().execute()
