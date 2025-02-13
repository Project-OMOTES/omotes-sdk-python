# OMOTES SDK Python

This repository is part of the 'Nieuwe Warmte Nu Design Toolkit' project. 

Python implementation of the OMOTES SDK through jobs which may be submitted, receive status updates for submitted jobs or delete submitted jobs.

## Protobuf
Please install `protoc` on your machine and make sure it is available in your `PATH`.  
Version 25.2 is used: https://github.com/protocolbuffers/protobuf/releases/tag/v25.2.

Installation example - Linux

1. Manually download the release zip file corresponding to your operating system and computer architecture (`protoc-<version>-<os>-<arch>.zip`) from github, or fetch the file using the command below.

    ```
    wget https://github.com/protocolbuffers/protobuf/releases/download/v25.2/protoc-25.2-linux-x86_64.zip
    ```

2. Unzip the file under `$HOME/.local` or a directory of your choice

    ```
    unzip protoc-25.2-linux-x86_64.zip -d $HOME/.local
    ```

3. Update your environment `PATH` variable to include the path to the protoc executable.

    ```
    export PATH="$PATH:$HOME/.local/bin"
    ```