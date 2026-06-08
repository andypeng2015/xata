# Contributing

The Xata project is open to contributions, but please note that this repository is "mirrored" from an internal mono-repo. Commits are automatically synced to the open source project as soon as they are merged in the mono-repo.

If you would like to contribute, please open a PR against this repository, and we will copy the commit (with attribution) to our internal repository. Note that your PR will be closed rather than merged.

If you plan a significant contribution, we recommend opening an issue first.


## Local development guide

To run locally, you need the following prerequisites:
- Docker
- [Kind](https://github.com/kubernetes-sigs/kind)
- [Tilt](https://github.com/tilt-dev/tilt)

**Step 1:** create kind cluster

```bash
kind create cluster --wait 10m
```

**Step 2:** deploy via tilt

```bash
tilt up
```

Wait until all resources are up. This might take a long time when you run it for the first time, because it needs to download all the required images. Next time you start it, it should be much quicker.

**Step 3:** install and authenticate the xata CLI:

```bash
# Install CLI
curl -fsSL https://xata.io/install.sh | bash

# Authenticate to a local profile. Sign in with email dev@xata.tech and password Xata1234!
xata auth login --profile local --issuer http://localhost:8080/realms/xata --api-url http://localhost:5001 --client-secret devsecret

# Set the profile
xata auth switch local
```

**Step 4:** Create the first project and branch

```bash
# Create project and main branch
$ xata project create --name my-project

# Create a child branch
$ xata branch create
```
