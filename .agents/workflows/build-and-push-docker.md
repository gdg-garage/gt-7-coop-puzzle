---
description: How to build and push the Docker image
---
# Docker Build & Push Workflow

As a coding agent, if you need to build and push the docker image for this repository, you should run the following commands:

1. Build the image with the correct tag:
   ```bash
   docker build -t tivvit/gt7-cipher .
   ```
// turbo-all
2. Push the image to Docker Hub:
   ```bash
   docker push tivvit/gt7-cipher
   ```

Always ensure you are in the project root (`/home/tivvit/git/gdg-garage/gt-coop-cipher`) when running these commands.
