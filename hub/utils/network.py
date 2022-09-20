import json
import subprocess
from hub.evaluation.main import measure_time
from hub.utils.system import System


class NetworkManager:
    def __init__(self, system: System) -> None:
        self.ssh_connection = system.ssh_connection
        self.system = system.name
        self.private_key_path = system.public_key_path.with_suffix("")
        print(self.private_key_path)

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
