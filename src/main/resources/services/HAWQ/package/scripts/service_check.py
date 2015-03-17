from resource_management import *
import hawq

class HAWQServiceCheck(Script):
  def service_check(self, env):
    hawq.verify_segments_state(env)

if __name__ == "__main__":
  HAWQServiceCheck().execute()