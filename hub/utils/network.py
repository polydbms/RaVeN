import json
import subprocess
from datetime import datetime
from subprocess import Popen
from typing import Any

from hub.evaluation.measure_time import measure_time
from hub.utils.system import System


class NetworkManager:
    socks_proxy: Popen[bytes] | Popen[Any]
    measure_docker: Popen[bytes] | Popen[Any]

    def __init__(self, system: System) -> None:
        self.ssh_connection = system.ssh_connection
        self._system = system
        self.private_key_path = system.public_key_path.with_suffix("")
        print(self.private_key_path)
        self.socks_proxy = None
        self.measure_docker = None

        self.ssh_options = f"-o 'StrictHostKeyChecking=no' -o 'IdentitiesOnly=yes' -i {self.private_key_path}"
        self.ssh_command = (
            f"ssh {self.ssh_connection} {self.ssh_options}"
        )

        self.file_prepend = f"{system.name}_{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    @property
    def system(self):
        return self._system.name

    @property
    def system_full(self):
        return self._system

    @staticmethod
    @measure_time
    def run_command(command, **kwargs):
        try:
            print(f"Running {command}")
            process = subprocess.Popen(
                command, stdout=subprocess.PIPE, universal_newlines=True, shell=True
            )
            while True:
                output = process.stdout.readline()
                if not output == "":
                    print(output.strip())
                return_code = process.poll()
                if return_code is not None:
                    print("RETURN CODE", return_code)
                    print("\n\n")
                    # Process has finished, read rest of the output
                    for output in process.stdout.readlines():
                        print(output.strip())
                    break
        except Exception as e:
            print(e)

    @measure_time
    def run_ssh(self, command, **kwargs):
        self.run_command(
            f"{self.ssh_command} '{command}'"
        )

    @measure_time
    def run_remote_mkdir(self, dir, **kwargs):
        self.run_ssh(
            f"mkdir -p {dir}"
        )

    def open_socks_proxy(self, port=59123) -> str:
        print(f"starting new SOCKS5 proxy at port {port}")
        self.socks_proxy = subprocess.Popen(f"{self.ssh_connection} -D {port} -N -v",
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            universal_newlines=True,
                                            shell=True)

        while True:
            output = self.socks_proxy.stderr.readline()

            if f"Local connections to LOCALHOST:{port} forwarded" in str(output):
                print(f"SOCKS5 connection established at port {port}")
                break

        return f"socks5://localhost:{port}"

    def stop_socks_proxy(self):
        print("stopping SOCKS5 proxy")

        self.socks_proxy.terminate()

    def start_measure_docker(self, stage: str, prerecord=True):
        print(f"starting measuring docker")
        measurement_folder = self.system_full.host_base_path \
            .joinpath("measurements") \
            .joinpath(self.file_prepend)
        self.run_ssh(f"mkdir -p {measurement_folder}")

        measurement_file = measurement_folder.joinpath(f"{stage}.csv")
        init_measurement_flag = "initialized measuring docker"
        self.run_ssh(
            f"echo \"timestamp\tID\tName\tCPUPerc\tMemUsage\tMemPerc\tNetIO\tBlockIO\tPIDs\" | tee {measurement_file}")
        command_docker = """docker stats --no-stream --format "{{.ID}}\\t{{.Name}}\\t{{.CPUPerc}}\\t{{.MemUsage}}\\t{{.MemPerc}}\\t{{.NetIO}}\\t{{.BlockIO}}\\t{{.PIDs}}" | while read -r line; do printf "%s\\t%s\\n" "$(date +%s.%06N)" "$line"; done"""
        command = f"ssh {self.ssh_connection} 'echo \"{init_measurement_flag}\"; while true; do {command_docker} | tee --append {measurement_file}; sleep 0; done'"

        print(command)
        self.measure_docker = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            shell=True)

        init_line_counter = 0
        while True:
            output = self.measure_docker.stdout.readline()

            if output.strip() != "":
                print(output.strip(), "\n")
                init_line_counter += 1

            if (prerecord and init_line_counter > 3) or (not prerecord and init_measurement_flag in str(output)):
                print(f"initialized and pre-loaded docker measurements for stage {stage}")
                break

    def stop_measure_docker(self):
        print("stopping measuring docker")

        outro_line_counter = 0

        while True:
            output = self.measure_docker.stdout.readline()

            if output.strip() != "":
                print(output.strip(), "\n")
                outro_line_counter += 1

            if outro_line_counter > 20:
                print(f"completed docker measurements")
                self.measure_docker.terminate()
                break
