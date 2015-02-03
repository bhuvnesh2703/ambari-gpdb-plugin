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
    hawq.metrics_start(env)
    
  def stop(self, env):
    hawq.metrics_stop(env)

  def status(self, env):
    import status_params
    for pid_hawqsegment in status_params.pid_hawqsegments:
      check_process_status(pid_hawqsegment)

if __name__ == "__main__":
  HawqSegment().execute()
