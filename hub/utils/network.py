import json
import subprocess
from hub.evaluation.main import measure_time


class NetworkManager:
    def __init__(self, system) -> None:
        try:
            with open(f"{system}.json") as f:
                resource = json.load(f)
            self.ssh_connection = resource["ssh_connection"]
            self.system = resource["system"]
            self.private_key_path = resource["public_key_path"].split(".pub")[0]
        except FileNotFoundError:
            print(f"{system}.json not found")

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
                print(output.strip())
                return_code = process.poll()
                if return_code is not None:
                    print("RETURN CODE", return_code)
                    # Process has finished, read rest of the output
                    for output in process.stdout.readlines():
                        print(output.strip())
                    break
        except Exception as e:
            print(e)

    @measure_time
    def run_ssh(self, command, **kwargs):
        self.run_command(
            f"{self.ssh_connection} -o 'StrictHostKeyChecking=no' -i {self.private_key_path} '{command}'"
        )
