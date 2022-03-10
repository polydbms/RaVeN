import re
import os
import json
import requests
import backoff

from hub.evaluation.main import measure_time

TRIGGER_TOKEN = os.environ["TRIGGER_TOKEN"]
PERSONAL_TOKEN = os.environ["PERSONAL_TOKEN"]


class Deployer:
    def __init__(self, resource) -> None:
        self.resource = resource
        try:
            with open(f"{self.resource['system']}.json") as f:
                self.resource = json.load(f)
        except IOError:
            print(
                f"File with name {self.resource['system']}.json does not exist. \nIt will be created by running python benchi.py start --system {self.resource['system']}."
            )
        self.ci_endpoint = "https://gitlab.com/api/v4/projects"

    def __send_request(self, **kwargs):
        """Send a general request to pipeline endpoint. Can trigger a pipeline"""
        self.url = f"{self.ci_endpoint}/{kwargs['project_id']}/trigger/pipeline"
        public_key = self.__get_public_key()
        files = {"token": TRIGGER_TOKEN, "ref": "main"}
        files["variables[TF_VAR_public_key]"] = public_key
        self.resource["project_id"] = kwargs["project_id"]
        kwargs.pop("project_id", None)
        if len(kwargs) > 1:
            files.update(kwargs)
        response = requests.post(self.url, data=files)
        if response.ok:
            self.resource["pipeline_id"] = response.json()["id"]
        self.get_pipeline_jobs()
        self.is_finished()
        self.get_ssh_connection()
        return response

    def __get_public_key(self):
        home = os.path.expanduser("~")
        path = self.resource["public_key_path"]
        if "~" in path:
            path = path.replace("~", home)
        return open(path, "r").read()

    @backoff.on_predicate(backoff.constant, interval=30)
    def is_finished(self):
        header = {"PRIVATE-TOKEN": PERSONAL_TOKEN}
        self.url = f"{self.ci_endpoint}/{self.resource['project_id']}/pipelines/{self.resource['pipeline_id']}"
        response = requests.get(self.url, headers=header)
        pipeline = response.json()
        if pipeline["status"] != "success":
            print("Backing off for (0-30s)")
            return False
        return True

    def get_pipeline_jobs(self):
        """Returns a list of jobs which are part of a pipeline"""
        header = {"PRIVATE-TOKEN": PERSONAL_TOKEN}
        self.url = f"{self.ci_endpoint}/{self.resource['project_id']}/pipelines/{self.resource['pipeline_id']}/jobs"
        response = requests.get(self.url, headers=header)
        if response.ok:
            jobs = response.json()
            self.resource["jobs"] = [
                {"id": job["id"], "name": job["name"]} for job in jobs
            ]
            with open(f"{self.resource['system']}.json", "w") as f:
                json.dump(self.resource, f)
        return response

    def __get_job_log(self, name):
        """Return a specific jobs log"""
        apply_id = self.__get_job_id(name)
        header = {"PRIVATE-TOKEN": PERSONAL_TOKEN}
        self.url = (
            f"{self.ci_endpoint}/{self.resource['project_id']}/jobs/{apply_id}/trace"
        )
        response = requests.get(self.url, headers=header)
        if not response.ok:
            print(response.json())
        log = response.text
        return log

    def get_ssh_connection(self, name="apply"):
        """Returns the ssh connection string required to connect to VM"""
        log = self.__get_job_log(name)
        search = re.search('ssh_connection = "(.*)"', log)
        ssh_connection = search.group(1)
        self.resource["ssh_connection"] = ssh_connection
        with open(f"{self.resource['system']}.json", "w") as f:
            json.dump(self.resource, f)
        return ssh_connection

    def __get_job_id(self, name="cleanup"):
        """Returns job id base on job name"""
        with open(f"{self.resource['system']}.json") as f:
            resource = json.load(f)
        return [job["id"] for job in resource["jobs"] if job["name"] == name][0]

    @measure_time
    def deploy(self, **kwargs):
        """Triggers a pipeline run and deploys a vm resource"""
        print("Creating resources")
        self.resource["variables[DESTROY]"] = "false"
        self.__send_request(**self.resource)
        print("Resources created")

    @measure_time
    def clean_up(self, **kwargs):
        """Runs job clean up to destroy resources"""
        print("Destroying resources")
        cleanup_id = self.__get_job_id()
        self.resource["variables[DESTROY]"] = "true"
        header = {"PRIVATE-TOKEN": PERSONAL_TOKEN}
        self.url = (
            f"{self.ci_endpoint}/{self.resource['project_id']}/jobs/{cleanup_id}/play"
        )
        response = requests.post(self.url, headers=header)
        self.is_finished()
        if not response.ok:
            print(response.json())
        else:
            os.remove(f"{self.resource['system']}.json")
        print("Resources destroyed")
