import math
import subprocess
import time
from pathlib import Path
from subprocess import Popen
from typing import Any

from hub.benchmarkrun.host_params import HostParameters
from hub.benchmarkrun.measurementslocation import MeasurementsLocation
from hub.duckdb.submit_data import DuckDBRunCursor
from hub.evaluation.measure_time import measure_time


class NetworkManager:
    _host_params: HostParameters
    _measurements_loc: MeasurementsLocation | None
    socks_proxy: Popen[bytes] | Popen[Any]
    measure_docker: Popen[bytes] | Popen[Any]

    def __init__(self, host_params: HostParameters, system_name: str, measurements_loc: MeasurementsLocation | None,
                 run_cursor: DuckDBRunCursor | None) -> None:
        self._host_params = host_params
        self.ssh_connection = host_params.ssh_connection
        self._measurements_loc = measurements_loc
        self.private_key_path = host_params.public_key_path.with_suffix("")
        print(self.private_key_path)
        self.system_name = system_name
        self.socks_proxy = None
        self.measure_docker = None
        self.run_cursor = run_cursor

        self.ssh_options = f"" \
                           f"-F ssh/config " \
                           f"-o 'StrictHostKeyChecking=no' " \
                           f"-o 'IdentitiesOnly=yes' " \
                           f"-i {self.private_key_path}"
        self.ssh_command = (
            f"ssh {self.ssh_connection} {self.ssh_options}"
        )

        self.run_remote_mkdir(self.host_params.host_base_path.joinpath("data").joinpath("results"))

    @property
    def host_params(self) -> HostParameters:
        return self._host_params

    @property
    def measurements_loc(self) -> MeasurementsLocation:
        return self._measurements_loc

    @measure_time
    def run_command(self, command, **kwargs):
        try:
            print(f"Running {command}")
            process = subprocess.Popen(
                command, stdout=subprocess.PIPE, universal_newlines=True, shell=True
            )
            last_line = ""
            last_line_cntr = 1
            while True:
                output = str(process.stdout.readline())
                if "benchi_marker" in output:
                    self.write_timings_marker(output)

                if not output == "":
                    if output == last_line:
                        if last_line_cntr % (10 ** (math.floor(math.log10(last_line_cntr)))) == 0:
                            print(f"{last_line_cntr} ", end="")

                        last_line_cntr += 1
                    else:
                        if last_line_cntr > 1:
                            print(last_line_cntr)
                            last_line_cntr = 1

                        print(output.strip())

                return_code = process.poll()
                if return_code is not None:
                    print("RETURN CODE", return_code)
                    print("\n\n")
                    # Process has finished, read rest of the output
                    for output in process.stdout.readlines():
                        print(output.strip())
                    break

                last_line = output
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

    @measure_time
    def run_remote_rm_file(self, file: Path):
        self.run_ssh(
            f"rm {file}"
        )

    def open_socks_proxy(self, port=59123) -> str:
        print(f"starting new SOCKS5 proxy at port {port}")
        self.socks_proxy = subprocess.Popen(f"{self.ssh_command} -D {port} -N -v",
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            universal_newlines=True,
                                            shell=True)

        while True:
            output = self.socks_proxy.stderr.readline()

            # if output.strip() != "":
            #     print(output.strip(), "\n")

            if f"Local connections to LOCALHOST:{port} forwarded" in str(output) or \
                    f"mux_client_request_session: master session id:" in str(output):
                print(f"SOCKS5 connection established at port {port}")
                break

        return f"socks5://localhost:{port}"

    def stop_socks_proxy(self):
        print("stopping SOCKS5 proxy")

        self.socks_proxy.terminate()

    def start_measure_docker(self, stage: str, prerecord=True):
        print(f"starting measuring docker for stage {stage}")
        self.run_ssh(f"mkdir -p {self.measurements_loc.host_measurements_folder}")

        measurement_file = self.measurements_loc.host_measurements_folder.joinpath(f"{stage}.csv")
        init_measurement_flag = "initialized measuring docker"
        self.run_ssh(
            f"echo \"timestamp\tID\tName\tCPUPerc\tMemUsage\tMemPerc\tNetIO\tBlockIO\tPIDs\" | tee {measurement_file}")
        command_docker = """docker stats --no-stream --format "{{.ID}}\\t{{.Name}}\\t{{.CPUPerc}}\\t{{.MemUsage}}\\t{{.MemPerc}}\\t{{.NetIO}}\\t{{.BlockIO}}\\t{{.PIDs}}" | while read -r line; do printf "%s\\t%s\\n" "$(date +%s.%06N)" "$line"; done"""
        command = f"{self.ssh_command} 'echo \"{init_measurement_flag}\"; while true; do {command_docker} | tee --append {measurement_file}; sleep 0; done'"

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
                if init_line_counter == 0:
                    print(output.strip(), "\n")

                print(f"{init_line_counter} ", end="")
                init_line_counter += 1

            if (prerecord and init_line_counter > 3) or (not prerecord and init_measurement_flag in str(output)):
                print("")
                print(f"initialized and pre-loaded docker measurements for stage {stage}")
                break

    def stop_measure_docker(self):
        print("stopping measuring docker")

        outro_line_counter = 0

        while True:
            output = self.measure_docker.stdout.readline()

            if output.strip() != "":
                if outro_line_counter == 0:
                    print(output.strip(), "\n")

                print(f"{outro_line_counter} ", end="")
                outro_line_counter += 1

            if outro_line_counter > 20:
                print("\n", output.strip(), "\n")
                print(f"completed docker measurements")
                self.measure_docker.terminate()
                break

    def write_timings_marker(self, marker: str):
        # print(marker)
        self.run_cursor.write_timings_marker(marker)
        # print("wrote timing to db")

        time_now = time.time()
        timings_line = f"{marker.strip()},{time_now}"

        with self.measurements_loc.timings_file.open("a") as f:
            f.write(timings_line)
            f.write("\n")
