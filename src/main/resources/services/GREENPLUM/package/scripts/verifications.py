class Verifications:
    def __init__(self, hardware, requirements):
        self.hardware = hardware
        self.requirements = requirements
        self.messages = []

    def mandatory_checks(self, component):
        self.check_greenplum_user()
        if component == "master":
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
        def vcmp(version1, version2):
            import re
            v1Array = [int(i) for i in re.sub(r'(\.0+)*$','', version1).split(".")]
            v2Array = [int(i) for i in re.sub(r'(\.0+)*$','', version2).split(".")]
            return cmp(v1Array, v2Array)

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
        actual   = int( self.hardware.get("mounts_greenplum").get("segment").get("available") )
        if actual < required:
            message = self.requirements.get("minimum_disk").get("message")
            message += " - System value: %.2fGB" % (actual/1024.0/1024.0)
            self.messages.append(message)

    def check_segment_count(self):
        import params
        required = self.requirements.get("max_segments_per_node").get("value")
        actual   = params.segments_per_node
        if actual > required:
            message = self.requirements.get("max_segments_per_node").get("message")
            self.messages.append(message)

    def check_greenplum_user(self):
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

        notice += "\n\nPlease run the following command to delete the Greenplum service and reinstall:"
        notice += '\n\tcurl -u USERNAME:PASSWORD -H "X-Requested-By: ambari" -X DELETE  http://AMBARI_SERVER_HOST:8080/api/v1/clusters/CLUSTER_NAME/services/GREENPLUM'
        notice += "\n\n(NOTE: To skip the optional preinstall checks (e.g. if you are installing "
        notice += "on a test cluster) during your next install, set skip.preinstall.verification "
        notice += "to TRUE in 'Advanced greenplum-site' at the configuration step during the install.)"

        return notice

