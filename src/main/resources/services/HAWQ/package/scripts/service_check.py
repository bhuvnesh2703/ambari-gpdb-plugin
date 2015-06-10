from resource_management import *
from common import *
import hawq

class HAWQServiceCheck(Script):
  sqlCommand = None
  remote_hostname = None

  def __init__(self):
    self.sqlCommand = "su gpadmin -c \\\"source /usr/local/hawq/greenplum_path.sh && psql -c '{0};'\\\""

  def service_check(self, env):
    hawq.verify_segments_state(env)

    import params
    self.remote_hostname = params.hawq_master

    self.drop_table()
    self.create_table()
    self.insert_data()
    self.query_data()
    self.cleanup()


  def drop_table(self):
    print "Testing Drop Table"
    command = self.sqlCommand.format("drop table if exists ambari_smoke_t1")
    (retcode, out, err) = subprocess_command_with_results(command, self.remote_hostname)
    if retcode:
      raise Exception("HAWQ Smoke Tests Failed. Return Code: {0}. Out: {1} Error: {2}".format(retcode, out, err))
    print out

    
  def create_table(self):
    print "Testing Create Table"
    command = self.sqlCommand.format("create table ambari_smoke_t1(a int)")
    (retcode, out, err) = subprocess_command_with_results(command, self.remote_hostname)
    if retcode:
      raise Exception("HAWQ Smoke Tests Failed. Return Code: {0}. Out: {1} Error: {2}".format(retcode, out, err))
    print out


  def insert_data(self):
    print "Testing Insert Data"
    command = self.sqlCommand.format("insert into ambari_smoke_t1 select * from generate_series(1,10)")
    (retcode, out, err) = subprocess_command_with_results(command, self.remote_hostname)
    if retcode:
      raise Exception("HAWQ Smoke Tests Failed. Return Code: {0}. Out: {1} Error: {2}".format(retcode, out, err))
    print out


  def query_data(self):
    print "Testing Querying Data"
    command = self.sqlCommand.format("select * from ambari_smoke_t1")
    (retcode, out, err) = subprocess_command_with_results(command, self.remote_hostname)
    if retcode:
      raise Exception("HAWQ Smoke Tests Failed. Return Code: {0}. Out: {1} Error: {2}".format(retcode, out, err))
    print out


  def cleanup(self):
    self.drop_table()


if __name__ == "__main__":
  HAWQServiceCheck().execute()
