import json
import subprocess
from subprocess import Popen
from typing import Any

from hub.evaluation.measure_time import measure_time
from hub.utils.system import System


class NetworkManager:
    socks_proxy: Popen[bytes] | Popen[Any]

    def __init__(self, system: System) -> None:
        self.ssh_connection = system.ssh_connection
        self._system = system
        self.private_key_path = system.public_key_path.with_suffix("")
        print(self.private_key_path)
        self.socks_proxy = None

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
            f"{self.ssh_connection} -o 'StrictHostKeyChecking=no' -o 'IdentitiesOnly=yes' -i {self.private_key_path} '{command}'"
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
