import sys
from metrics import manager

if __name__=="__main__":
    import socket
    conn_params = dict(local_segment_hostname=socket.gethostname(), 
                       num_segments=int(sys.argv[1]), 
                       connection_host="localhost", 
                       base_port=40000, 
                       dbname="template0", 
                       username="gpadmin")
    ag = manager.StatsManager(conn_params=conn_params)
    ag.run()