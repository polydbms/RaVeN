## Benchi tutorial

#### Requirements:
- Azure Account
- Docker (Tested with 4.10.1)

*Optionaly* you can set up a local GitLab runner so that it executes your pipeline (Refering to /hub/deployment/resource_allocation/.gitlab-ci.yml).

### Steps
1. Create a new GitLab Project
    - Use the contents of /hub/deployment/resource_allocation. (See the readme in the same directory)
    - In the resource_allocation readme there are variables such as **ARM_CLIENT_ID, ARM_CLIENT_SECRET, ARM_SUBSCRIPTION_ID and ARM_TENANT_ID**. You need to create a azure application (check [documentation](https://docs.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app#register-an-application)).
    - Follow the readme through
2. At this point you should have set your CI/CD Variables on your new Project and created the TRIGGER_TOKEN and PERSONAL_TOKEN. Next you need to copy /test.env and rename it as /.env
3. Add your Tokens and save the file. 
4. Run docker-compose up -d (!Change the data volume as you wish: e.g.: path_to_your_geo_data_dir:/data)
5. Run docker exec -it *smth_gdal_env* bash
6. Change the experiments.yaml with your data and query
7. Run inside the docker containers python benchi.py start
    - Use python benchi.py --help (to get other options)

8. Run python benchi.py clean to remove the resource created on the cloud
