#!/usr/bin/env python
from resource_management import *
import hawq

class HawqSegment(Script):
  def install(self, env):
    self.install_packages(env)
    self.configure(env)

  def configure(self, env):
    import params
    env.set_params(params)
    hawq.common_setup(env)
    hawq.segment_configure(env)
    hawq.system_verification(env, "segment")

  def start(self, env):
    self.configure(env)
    self.display_help(env, "start")

  def stop(self, env):
    self.display_help(env, "stop")

  def status(self, env):
    import status_params
    for pid_hawqsegment in status_params.pid_hawqsegments:
      check_process_status(pid_hawqsegment)

  def display_help(self, env, operation):
    Logger.info("Segment {0} operation is controlled by active hawq master, Please wait for master {0} operation to complete.".format(operation))

if __name__ == "__main__":
  HawqSegment().execute()
