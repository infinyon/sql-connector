Note:
`make integration_test` can get an error:
```
Error: unable to connect to docker engine

Caused by:
    0: Error in the hyper legacy client: client error (Connect)
    1: client error (Connect)
    2: No such file or directory (os error 2)
```

Some test/dev enviroments may use alternate docker daemon implmentations.
The Docker utility crate `bollard` uses the default endpoint of `unit///var/run/docker.sock`
or the `DOCKER_HOME` environment variable, not the `docker context` default.

`docker context ls` can be use to discover the value to set `DOCKER_HOME`
environment variable so that that the `integration_test target` will use to correct
docker connection.

