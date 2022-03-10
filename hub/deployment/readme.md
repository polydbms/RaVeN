# Deployment
The deployment process is done through a gitlab ci/cd pipeline. For this reason it is required to create a new gitlab project and upload the content of resource_allocation into the new project. Once uploaded you need to go to settings -> CI/CD -> Variables and add the following variables:
- ARM_CLIENT_ID
- ARM_CLIENT_SECRET
- ARM_SUBSCRIPTION_ID
- ARM_TENANT_ID

These variables correspond to the service principal credentails when it is created via azure portal. 
Once the variables are set, you need to get the project id. This can be found on the main page of the project near the project name.

Furthermore you need to create a [personal token](https://docs.gitlab.com/ee/user/profile/personal_access_tokens.html) and a [trigger token](https://docs.gitlab.com/ee/ci/triggers/). Create enviorment variables on your local machine: TRIGGER_TOKEN and PERSONAL_TOKEN.