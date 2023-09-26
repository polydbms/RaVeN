# SSH Config

The SSH config file contains information on how the remote server shall be accessed. Its existence is required, but it does not need explicit information on how a host can be accessed. An exemplary configuration can be found under `ssh/config.default`. **It is necessary to copy this configuration to `ssh/config` before running Benchi.** This is done to avoid potential leakage of network architecture information into the git repository.

```bash
HOST *
    ServerAliveInterval 60
    IdentitiesOnly=yes
    #ControlPath ssh/controlmasters/%r@%h:%p
    #ControlMaster auto
    #ControlPersist 120
    BatchMode yes
```

Uncommenting the `ControlPath`, `ControlMaster` and `ControlPersist` options can speed up the first stages of the benchmark significantly. This speed-up though comes at the cost of degraded performance if a benchmark run does not run completely, as the open SSH connection may sometimes not be reused correctly. The options are therefore disabled by default.