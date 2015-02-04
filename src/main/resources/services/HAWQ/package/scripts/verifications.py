class Verifications:
    def __init__(self, hardware, requirements):
        self.hardware = hardware
        self.requirements = requirements
        self.messages = []

    def master_preinstall_checks(self):
        self.common_preinstall_checks()
        self.check_port_conflicts()

    def segment_preinstall_checks(self):
        self.common_preinstall_checks()
        self.check_disk_segment()

    def common_preinstall_checks(self):
        self.check_memory()
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
            if actual != required:
                message = osparams.get(osparam).get("message")
                message += " - System value: %s" % actual
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

    def get_messages(self):
        return self.messages
