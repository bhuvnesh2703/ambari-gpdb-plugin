class Verifications:
    def __init__(self, hardware, requirements):
        self.hardware = hardware
        self.requirements = requirements
        self.messages = []

    def mandatory_checks(self, component):
        self.check_hawq_user()
        if component == "master":
            self.check_port_conflicts()
            self.check_segment_count()
        if component == "segment":
            pass

    def preinstall_checks(self, component):
        self.common_preinstall_checks()
        if component == "master":
            pass
        if component == "segment":
            self.check_disk_segment()
        
    def common_preinstall_checks(self):
        self.check_memory()
        self.check_osversion()
        self.check_osparams()

    def check_memory(self):
        required = self.requirements.get("minimum_memory").get("value")
        actual   = int(self.hardware.get("memorytotal"))
        if actual < required:
            message = self.requirements.get("minimum_memory").get("message")
            message += " - System value: %.2fGB" % (actual/1024.0/1024.0)
            self.messages.append(message)

    def check_osparams(self):
        osparams = self.requirements.get("osparams")
        for osparam in osparams:
            required = osparams.get(osparam).get("value")
            actual   = self.hardware.get('osparams').get(osparam)
            if osparam == "kernel.sem" or osparam == "net.ipv4.ip_local_port_range":
                required = required.split()
                actual   = actual.split()
            if actual != required:
                message = osparams.get(osparam).get("message")
                message += " - System value: %s" % actual
                self.messages.append(message)

    def check_osversion(self):
        import re
        def vcmp(version1, version2):
            def normalize(v):
                return [int(x) for x in re.sub(r'(\.0+)*$','', v).split(".")]
            return cmp(normalize(version1), normalize(version2))
        required = self.requirements.get("minimum_version").get("value")
        actual   = self.hardware.get("osversion")
        if not actual:
            return
        if vcmp(actual, required) == -1: # actual < required
            message = self.requirements.get("minimum_version").get("message")
            message += " - System version: %s" % actual
            self.messages.append(message)

    def check_disk_segment(self):
        required = self.requirements.get("minimum_disk").get("value")
        actual   = int( self.hardware.get("mounts_hawq").get("segment").get("available") )
        if actual < required:
            message = self.requirements.get("minimum_disk").get("message")
            message += " - System value: %.2fGB" % (actual/1024.0/1024.0)
            self.messages.append(message)

    def check_port_conflicts(self):
        import params
        import subprocess
        command = "netstat -tulpn | grep ':{0}\\b'".format(params.hawq_master_port)
        (r,o) = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True).communicate()
        if (len(r)):
            # we have a conflict with the hawq master port.
            message = "Conflict with HAWQ Master port. Either the service is already running or some other service is using port: {0}".format(params.hawq_master_port)
            self.messages.append(message)

    def check_segment_count(self):
        import params
        required = self.requirements.get("max_segments_per_node").get("value")
        actual   = params.segments_per_node
        if actual > required:
            message = self.requirements.get("max_segments_per_node").get("message")
            self.messages.append(message)

    def check_hawq_user(self):
        required = self.requirements.get("user_groups").get("value")
        actual   = self.hardware.get("user_groups")
        missing  = filter(lambda x: x not in actual, required)
        if missing:
            message = self.requirements.get("user_groups").get("message")
            message += " - missing groups: %s" % str(missing)
            self.messages.append(message)

    def get_messages(self):
        return self.messages

    def get_notice(self):
        notice = "Host system verification failed:\n\n"
        notice += 'x '+'\nx '.join(self.get_messages())

        notice += "\n\nPlease run the following command to delete the HAWQ service and reinstall:"
        notice += '\n\tcurl -u USERNAME:PASSWORD -H "X-Requested-By: ambari" -X DELETE  http://AMBARI_SERVER_HOST:8080/api/v1/clusters/CLUSTER_NAME/services/HAWQ'
        notice += '\nIf you also tried to install PXF, leaving it in an invalid state:'
        notice += '\n\tcurl -u USERNAME:PASSWORD -H "X-Requested-By: ambari" -X DELETE  http://AMBARI_SERVER_HOST:8080/api/v1/clusters/CLUSTER_NAME/services/PXF'

        notice += "\n\n(NOTE: To skip the optional preinstall checks (e.g. if you are installing "
        notice += "on a test cluster) during your next install, set skip.preinstall.verification "
        notice += "to TRUE in 'Advanced hawq-site' at the configuration step during the install.)"

        return notice

