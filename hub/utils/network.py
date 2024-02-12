import math
import subprocess
import time
from pathlib import Path
from subprocess import Popen
from typing import Any

from hub.benchmarkrun.host_params import HostParameters
from hub.benchmarkrun.measurementslocation import MeasurementsLocation
from hub.zsresultsdb.submit_data import DuckDBRunCursor
from hub.evaluation.measure_time import measure_time


class NetworkManager:
    """
    wrapper around all remote operations the controller may execute
    """
    _host_params: HostParameters
    _measurements_loc: MeasurementsLocation | None
    socks_proxy: Popen[bytes] | Popen[Any]
    measure_docker: Popen[bytes] | Popen[Any]

    def __init__(self, host_params: HostParameters, system_name: str, measurements_loc: MeasurementsLocation | None,
                 run_cursor: DuckDBRunCursor | None, query_timeout: int = 0) -> None:
        """
        the init function
        :param host_params: the host parameters
        :param system_name: the name of the system-under-test
        :param measurements_loc: the measurements location
        :param run_cursor: the database cursor
        :param query_timeout: the query timeout
        """
        self._host_params = host_params
        self.ssh_connection = host_params.ssh_connection
        self._measurements_loc = measurements_loc
        self.private_key_path = host_params.public_key_path.with_suffix("")
        print(self.private_key_path)
        self.system_name = system_name
        self.socks_proxy = None
        self.measure_docker = None
        self.run_cursor = run_cursor
        self.warm_start_no = 0
        self.query_timeout = query_timeout

        self.ssh_options = f"" \
                           f"-F /hub/ssh/config " \
                           f"-o 'StrictHostKeyChecking=no' " \
                           f"-o 'IdentitiesOnly=yes' " \
                           f"-i {self.private_key_path}" #FIXME config path
        self.ssh_command = (
            f"ssh {self.ssh_connection} {self.ssh_options}"
        )

        self.run_remote_mkdir(self.host_params.host_base_path.joinpath("data").joinpath("results"))

    @property
    def host_params(self) -> HostParameters:
        """
        gets the host parameters
        :return: the host paramters
        """
        return self._host_params

    @property
    def measurements_loc(self) -> MeasurementsLocation:
        """
        gets the measurements location
        :return: the measurements location
        """
        return self._measurements_loc

    @measure_time
    def run_command(self, command, **kwargs) -> int:
        """
        executes a command on the controller. reads the output of the command and prints it in the logs of benchi.
        if the output is identical between immediately following lines, it is truncated and only a count of the amount of
        repeated lines is printed.

        if either a "benchi_marker" or "benchi_meta" is printed, the line is interpreted as a timings marker and inserted
        into the database
        :param command: the command to execute
        :param kwargs:
        :return: the return code of the remotely executed command
        """
        try:
            print(f"Running {command}")
            process = subprocess.Popen(
                command, stdout=subprocess.PIPE, universal_newlines=True, shell=True
            )
            last_line = ""
            last_line_cntr = 1
            while True:
                output = str(process.stdout.readline())
                if "benchi_marker" in output or "benchi_meta" in output:
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
                    if "ssh" in command and return_code == 255:
                        raise Exception("Unable to establish SSH connection")

                    for output in process.stdout.readlines():
                        print(output.strip())
                    return return_code

                last_line = output
        except Exception as e:
            print(e)

    @measure_time
    def run_ssh(self, command, **kwargs):
        """
        prefixes a command with the ssh command to run it remotely on the host
        :param command: the command
        :param kwargs:
        :return: the return code
        """
        return self.run_command(
            f"{self.ssh_command} '{command}'"
        )

    def run_ssh_with_timeout(self, command, timeout, **kwargs):
        """
        prefixes a command with the ssh command and a timeout to run it remotely on the host
        :param command: the command
        :param kwargs:
        :return: the return code
        """
        return self.run_ssh(
            f"timeout -k 1m {timeout} {command}"
        )

    def run_query_ssh(self, command, **kwargs):
        """
        runs a query remotely on the host
        :param command: the command containing the query
        :param kwargs:
        :return: the return code
        """
        returncode = self.run_ssh_with_timeout(command, self.query_timeout)

        if int(returncode) == 124:
            self.run_ssh(
                f"""echo "benchi_marker,$(date +%s.%N),terminated,execution,{self.system_name},," """)

        return returncode

    @measure_time
    def run_remote_mkdir(self, dir, **kwargs):
        """
        creates a folder on the host
        :param dir: the path where the folder shall be created
        :param kwargs:
        :return:
        """
        return self.run_ssh(
            f"mkdir -p {dir}"
        )

    @measure_time
    def run_remote_rm_file(self, file: Path):
        """
        deletes a file on the host
        :param file: the path to the file
        :return:
        """
        return self.run_ssh(
            f"rm {file}"
        )

    def open_socks_proxy(self, port=59123) -> str:
        """
        opens a socks5 proxy to the host in order to send queries. Blocking wait until the connection has been established
        :param port: the port of the proxy, 59123 by default
        :return: the socks proxy URL
        """
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
        """
        stops the created socks5 proxy
        :return:
        """
        print("stopping SOCKS5 proxy")

        self.socks_proxy.terminate()

    def start_measure_docker(self, stage: str, prerecord=True):
        """
        starts a process that measures resource utilization on the host using docker stats. for this purpose, first the
        remote folder is created. then, the command is executed in a loop within the shell. Optionally waits for
        3 results before the program execution continues.
        :param stage: the stage for which the resource util shall be recorded
        :param prerecord: whether to wait for 3 results prior to continuing
        :return:
        """
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

            if (prerecord and init_line_counter > 1) or (not prerecord and init_measurement_flag in str(output)):
                print("")
                print(f"initialized and pre-loaded docker measurements for stage {stage}")
                break

    def stop_measure_docker(self):
        """
        stops docker stats collection on the host. Waits for 20 measurements before killing the process
        :return:
        """
        print("stopping measuring docker")

        outro_line_counter = 0

        while True:
            output = self.measure_docker.stdout.readline()

            if output.strip() != "":
                if outro_line_counter == 0:
                    print(output.strip(), "\n")

                print(f"{outro_line_counter} ", end="")
                outro_line_counter += 1

            if outro_line_counter > 1:
                print("\n", output.strip(), "\n")
                print(f"completed docker measurements")
                self.measure_docker.terminate()
                break

    def write_timings_marker(self, marker: str):
        """
        write a timings marker into the database
        :param marker: the timings string
        :return:
        """
        if ",execution," in marker:
            marker = marker.replace(",execution,", f",execution-{self.warm_start_no},")
        # print(marker)
        self.run_cursor.write_timings_marker(marker)
        # print("wrote timing to db")

        time_now = time.time()
        timings_line = f"{marker.strip()},{time_now}"

        with self.measurements_loc.timings_file.open("a") as f:
            f.write(timings_line)
            f.write("\n")

    def init_timings_sync_marker(self, system):
        """
        triggers a timings marker that records the clocks of the controller and the host in order to synchronize them.
        Result is catched by the run_command function.
        :param system: the system-under-test the marker shall be recorded for
        :return:
        """
        self.run_ssh(f"""echo "benchi_marker,$(date +%s.%N),now,time_diff_check,{system},," """)

    def add_meta_marker_start(self, warm_start_no):
        """
        adds a marker that specifies the start of an execution run.
        :param warm_start_no: the number of the back-to-back execution
        :return:
        """
        self.warm_start_no = warm_start_no
        self.run_ssh(
            f"""echo "benchi_meta,$(date +%s.%N),start,execution,{self.system_name},,{"cold" if self.warm_start_no == 0 else f"warm_{self.warm_start_no}"}" """)

    def add_meta_marker_end(self):
        """
        adds a marker that specifies the end of an execution run.
        :param warm_start_no: the number of the back-to-back execution
        :return:
        """
        self.run_ssh(
            f"""echo "benchi_meta,$(date +%s.%N),end,execution,{self.system_name},,{"cold" if self.warm_start_no == 0 else f"warm_{self.warm_start_no}"}" """)
