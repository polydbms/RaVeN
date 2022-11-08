from pathlib import Path

from configuration import PROJECT_ROOT
from hub.evaluation.measure_time import measure_time
from hub.utils.datalocation import DataLocation, FileType
from hub.utils.network import NetworkManager
from hub.utils.system import System


class FileTransporter:
    def __init__(self, network_manager: NetworkManager) -> None:
        self.network_manager = network_manager
        self.system = network_manager.system_full
        remote = self.network_manager.ssh_connection
        private_key_path = self.network_manager.private_key_path
        self.ssh_options = f"-o 'StrictHostKeyChecking=no' -o 'IdentitiesOnly=yes' -i {private_key_path}"
        self.ssh_command = (
            f"ssh {remote} {self.ssh_options}"
        )
        self.rsync_command_send = f"rsync --verbose --update -e \"ssh {self.ssh_options}\" options_plch from_File_plch {remote}:to_File_plch"
        self.scp_command_send = f"scp {self.ssh_options} options_plch from_File_plch {remote}:to_File_plch"
        self.rsync_command_receive = f"rsync --verbose --update -e \"ssh {self.ssh_options}\" options_plch {remote}:from_File_plch/ to_File_plch/"
        self.scp_command_receive = f"scp {self.ssh_options} options_plch {remote}:from_File_plch to_File_plch"
        # print(self.ssh_command)
        # print(self.scp_command_send)

    @measure_time
    def send_file(self, local: Path, remote: Path, **kwargs):
        if local.exists():
            command = (
                self.rsync_command_send.replace("options_plch", "")
                .replace("from_File_plch", str(local))
                .replace("to_File_plch", str(self.system.host_base_path.joinpath(remote)))
            )
            return self.network_manager.run_command(command)
        raise FileNotFoundError(local)

    @measure_time
    def send_folder(self, local: Path, remote: Path, **kwargs):
        if local.exists():
            command = (
                self.rsync_command_send.replace("options_plch", "-r")
                .replace("from_File_plch", str(local))
                .replace("to_File_plch", str(self.system.host_base_path.joinpath(remote)))
            )
            return self.network_manager.run_command(command)
        raise FileNotFoundError(local)

    @measure_time
    def get_file(self, remote, local, **kwargs):
        command = (
            self.scp_command_receive.replace("options_plch", "")
            .replace("from_File_plch", str(self.system.host_base_path.joinpath(remote)))
            .replace("to_File_plch", str(local))
        )
        return self.network_manager.run_command(command)

    @measure_time
    def get_folder(self, remote: Path, local: Path, **kwargs):
        command = (
            self.rsync_command_receive.replace("options_plch", "-r")
            .replace("from_File_plch", str(self.system.host_base_path.joinpath(remote)))
            .replace("to_File_plch", str(local))
        )
        return self.network_manager.run_command(command)

    @measure_time
    def send_configs(self, **kwargs):
        host_config_path = self.system.host_base_path.joinpath("config")
        print(host_config_path)
        self.network_manager.run_remote_mkdir(host_config_path)
        self.send_folder(
            Path(f"{PROJECT_ROOT}/hub/deployment/files/{self.network_manager.system}"),
            host_config_path
        )

    @measure_time
    def send_data(self, file: DataLocation, **kwargs):
        """Method for sending data to remote."""
        # print(file)
        host_dir_up = file.host_dir.joinpath("../")
        self.network_manager.run_remote_mkdir(file.host_dir)
        self.network_manager.run_remote_mkdir(file.host_dir_preprocessed)
        if file.type == FileType.FILE:
            raise NotImplementedError("Single Files are currently not supported")
            # self.send_file(file.controller_location, file.host_dir)
        elif file.type == FileType.FOLDER:
            self.send_folder(file.controller_location, host_dir_up)
        elif file.type == FileType.ZIP_ARCHIVE:
            self.send_file(file.controller_location, host_dir_up)
            self.network_manager.run_command(f"{self.ssh_command} unzip")
        else:
            print("sent nothing")
