from pathlib import Path

from configuration import PROJECT_ROOT
from hub.evaluation.measure_time import measure_time
from hub.utils.datalocation import DataLocation, FileType
from hub.utils.network import NetworkManager


class FileTransporter:
    def __init__(self, network_manager: NetworkManager) -> None:
        self.network_manager = network_manager
        remote = self.network_manager.ssh_connection.split("ssh ")[-1]
        private_key_path = self.network_manager.private_key_path
        self.ssh_command = (
            f"ssh {remote} -o 'StrictHostKeyChecking=no' -o 'IdentitiesOnly=yes' -i {private_key_path}"
        )
        self.scp_command_send = f"scp -o 'StrictHostKeyChecking=no' -o 'IdentitiesOnly=yes' -i {private_key_path} options from_File_plch {remote}:to_File_plch"
        self.scp_command_recieve = f"scp -o 'StrictHostKeyChecking=no' -o 'IdentitiesOnly=yes' -i {private_key_path} options {remote}:from_File_plch to_File_plch"
        # print(self.ssh_command)
        # print(self.scp_command_send)

    @measure_time
    def send_folder(self, local, remote, **kwargs):
        command = (
            self.scp_command_send.replace("options", "-r")
            .replace("from_File_plch", local)
            .replace("to_File_plch", remote)
        )
        return self.network_manager.run_command(command)

    @measure_time
    def send_file(self, local, remote, **kwargs):
        if Path(local).exists():
            command = (
                self.scp_command_send.replace("options", "")
                .replace("from_File_plch", local)
                .replace("to_File_plch", remote)
            )
            return self.network_manager.run_command(command)
        raise FileNotFoundError(local)

    @measure_time
    def send_folder(self, local, remote, **kwargs):
        if Path(local).exists():
            command = (
                self.scp_command_send.replace("options", "-r")
                .replace("from_File_plch", str(local))
                .replace("to_File_plch", str(remote))
            )
            return self.network_manager.run_command(command)
        raise FileNotFoundError(local)

    @measure_time
    def get_file(self, remote, local, **kwargs):
        command = (
            self.scp_command_recieve.replace("options", "")
            .replace("from_File_plch", str(remote))
            .replace("to_File_plch", str(local))
        )
        return self.network_manager.run_command(command)

    @measure_time
    def get_folder(self, remote, local, **kwargs):
        command = (
            self.scp_command_recieve.replace("options", "-r")
            .replace("from_File_plch", str(remote))
            .replace("to_File_plch", str(local))
        )
        return self.network_manager.run_command(command)

    @measure_time
    def send_configs(self, **kwargs):
        self.send_folder(
            f"{PROJECT_ROOT}/hub/deployment/files/{self.network_manager.system}", "~/config"
        )

    @measure_time
    def send_data(self, file: DataLocation, **kwargs):
        """Method for sending data to remote."""
        # print(file)
        self.network_manager.run_command(f"{self.ssh_command} mkdir -p {file.host_dir}")
        if file.type == FileType.FILE:
            raise NotImplementedError("Single Files are currently not supported")
            # self.send_file(file.controller_location, file.host_dir)
        elif file.type == FileType.FOLDER:
            self.send_folder(str(file.controller_location), file.host_dir)  # TODO make to use Path()
        elif file.type == FileType.ZIP_ARCHIVE:
            self.send_file(str(file.controller_location), file.host_dir)  # TODO make to use Path()
            self.network_manager.run_command(f"{self.ssh_command} unzip")
        else:
            print("sent nothing")
