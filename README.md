# stackable-gbif-examples
A repository with a couple of examples for using stackable components in Kubernetes

## Note
Please note of potential problems when building the images on different architecture. A common problem encountered, is the developers computer being fx arch64 while the dev cluster is amd64. Docker is able to emulate different architectures but it is limited. An example is maven halting while trying to fetch dependencies. That is the reason the dockerfile .hack file.
