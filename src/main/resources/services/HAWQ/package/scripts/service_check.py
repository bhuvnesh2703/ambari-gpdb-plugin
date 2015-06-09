from resource_management import *
from common import *
import hawq

class HAWQServiceCheck(Script):
  sqlCommand = None
  remote_hostname = None

  def __init__():
    this.sqlCommand = "su -c \"source /usr/local/hawq/greenplum_path.sh && psql -c '{0};'\" gpadmin";
    this.remote_hostname = params.hawq_master

  def service_check(self, env):
    hawq.verify_segments_state(env)

    drop_table()
    create_table()
    insert_data()
    query_data()
    cleanup()


  def drop_table():
    command = this.sqlCommand.format("drop table if exists ambari_smoke_t1")
    (retcode, out, err) = subprocess_command_with_results(command, this.remote_hostname)
    if retcode:
      raise Exception("HAWQ Smoke Tests Failed. Return Code: {0}. Out: {1} Error: {2}".format(retcode, out, err))

    
  def create_table():
    command = this.sqlCommand.format("create table ambari_smoke_t1(a int)")
    (retcode, out, err) = subprocess_command_with_results(command, this.remote_hostname)
    if retcode:
      raise Exception("HAWQ Smoke Tests Failed. Return Code: {0}. Out: {1} Error: {2}".format(retcode, out, err))


  def insert_data():
    command = this.sqlCommand.format("insert into ambari_smoke_t1 select * from generate_series(1,10)")
    (retcode, out, err) = subprocess_command_with_results(command, this.remote_hostname)
    if retcode:
      raise Exception("HAWQ Smoke Tests Failed. Return Code: {0}. Out: {1} Error: {2}".format(retcode, out, err))


  def query_data():
    command = this.sqlCommand.format("select * from ambari_smoke_t1")
    (retcode, out, err) = subprocess_command_with_results(command, this.remote_hostname)
    if retcode:
      raise Exception("HAWQ Smoke Tests Failed. Return Code: {0}. Out: {1} Error: {2}".format(retcode, out, err))


  def cleanup():
    drop_table()


if __name__ == "__main__":
  HAWQServiceCheck().execute()
