# Host Configuration

The host configuration acts as the static configuration for Benchi. it is supplied to each run and contains information on the controller as well as the hosts. Hosts are supplied as an array of `host` objects to prepare for a future extension, where one controller may be able to control multiple hosts. **Right now, only the first host supplied is used for any operation.** If a complementary [SSH config](ssh-config.md) is supplied, the name of the host shall match the name supplied in there. Otherwise, the name of the host shall match to an URL of a reachable server.

```yaml
config:
  controller:
    results_folder: /data/results         # Location of the results folder of the controller
    results_db: /data/results.db          # Location of the metrics database
  hosts:
    - host: "remote.server"               # URL of the server
      base_path: /data/benchipath         # Benchi root directory on the host
      public_key_path: ~/.ssh/id_rsa.pub  # Location of the SSH key to access the host
```
