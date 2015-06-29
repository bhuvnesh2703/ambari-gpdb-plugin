from resource_management import *
from common import subprocess_command_with_results
import hawq

class HAWQServiceCheck(Script):
  sql_command = None
  sql_noheader_command = None
  remote_hostname = None
  temp_table_name = "ambari_smoke_t1"
  test_user = "gpadmin"

  def __init__(self):
    self.sql_command = "su - " + self.test_user + " -c \\\"source /usr/local/hawq/greenplum_path.sh && psql -c '{0};'\\\""
    self.sql_noheader_command = "su - " + self.test_user + " -c \\\"source /usr/local/hawq/greenplum_path.sh && psql -t -c '{0};'\\\""

  def service_check(self, env):
    hawq.verify_segments_state(env)

    import params
    self.remote_hostname = params.hawq_master

    self.drop_table()
    try:
      self.create_table()
      self.insert_data()
      self.query_data()
      self.check_data_correctness()
      self.cleanup()
    except:
      self.cleanup()
      raise


  def drop_table(self):
    print "Testing Drop Table"
    command = self.sql_command.format("drop table if exists " + self.temp_table_name)
    (retcode, out, err) = subprocess_command_with_results(command, self.remote_hostname)
    if retcode:
      raise Exception("HAWQ Smoke Tests Failed. Return Code: {0}. Out: {1} Error: {2}".format(retcode, out, err))
    print out

    
  def create_table(self):
    print "Testing Create Table"
    command = self.sql_command.format("create table " + self.temp_table_name + "(a int)")
    (retcode, out, err) = subprocess_command_with_results(command, self.remote_hostname)
    if retcode:
      raise Exception("HAWQ Smoke Tests Failed. Return Code: {0}. Out: {1} Error: {2}".format(retcode, out, err))
    print out


  def insert_data(self):
    print "Testing Insert Data"
    command = self.sql_command.format("insert into " + self.temp_table_name + " select * from generate_series(1,10)")
    (retcode, out, err) = subprocess_command_with_results(command, self.remote_hostname)
    if retcode:
      raise Exception("HAWQ Smoke Tests Failed. Return Code: {0}. Out: {1} Error: {2}".format(retcode, out, err))
    print out


  def query_data(self):
    print "Testing Querying Data"
    command = self.sql_command.format("select * from " + self.temp_table_name)
    (retcode, out, err) = subprocess_command_with_results(command, self.remote_hostname)
    if retcode:
      raise Exception("HAWQ Smoke Tests Failed. Return Code: {0}. Out: {1} Error: {2}".format(retcode, out, err))
    print out


  def check_data_correctness(self):
    expected_data = "55"
    print "Testing Correctness of Data. Finding sum of all the inserted entries. Expected output: " + expected_data
    command = self.sql_noheader_command.format("select sum(a) from " + self.temp_table_name)
    (retcode, out, err) = subprocess_command_with_results(command, self.remote_hostname)
    if retcode:
      raise Exception("HAWQ Smoke Tests Failed. Return Code: {0}. Out: {1} Error: {2}".format(retcode, out, err))
    if expected_data != out.strip():
      raise Exception("HAWQ Smoke Tests Failed. Incorrect Data Returned. Expected Data: {0}. Acutal Data: {1} ".format(expected_data, out))
    print out


  def cleanup(self):
    self.drop_table()


if __name__ == "__main__":
  HAWQServiceCheck().execute()
