Ambari HAWQ plugin
=================================

How to install
--------------------
You can install hawq plugin onto ambari-server.

1. stop ambari-server
1. Make sure you execute setup_repo.sh for both PADS and hawq-plugin
1. Install the plugin
   ```
   sudo yum install -y hawq-plugin
   ```
1. start ambari-server


Whati if you are installing your cluster in a single node.
--------------------

1. Before restarting ambari-server in the installation steps, remove HAWQSTANDBY component from metainfo.xml
1. During Cluster Install Wizard, make sure hawq.master.port in hawq-site is changed through Ambari Wizard to 10432 (or anything other than 5432) to avoid a port conflict.

#### metainfo.xml change
e.g /var/lib/ambari-server/resources/stacks/PHD/3.0/services/HAWQ/metainfo.xml

```
<component>
    <name>HAWQSTANDBY</name>
    <displayName>HAWQ Standby Master</displayName>
    <category>MASTER</category>
    <cardinality>1</cardinality>
    <commandScript>
        <script>scripts/hawqstandby.py</script>
        <scriptType>PYTHON</scriptType>
        <timeout>600</timeout>
    </commandScript>
    <customCommands>
        <customCommand>
            <name>ActivateStandby</name>
            <commandScript>
                <script>scripts/hawqstandby.py</script>
                <scriptType>PYTHON</scriptType>
                <timeout>600</timeout>
            </commandScript>
        </customCommand>
    </customCommands>
    <dependencies>
      <dependency>
          <name>HDFS/HDFS_CLIENT</name>
          <scope>host</scope>
          <auto-deploy>
              <enabled>true</enabled>
          </auto-deploy>
      </dependency>
    </dependencies>
</component>

```


