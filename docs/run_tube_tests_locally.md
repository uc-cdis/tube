# Run Tube tests locally

> NOTE: Tube local tests currently can not be configured with ARM based Mac computers (M1 or M2 Macs) due to limitations with old spark and elasticsearch docker container images. The document will be updated if and when they become available.

## Initial Setup
* Running tube locally requires `python 3.7`, `postgresql`, `docker` and `jq` installed on the local machine.
*  It is recommended to create a python virtual environment to install all the required dependencies to avoid complications.
* Tube uses poetry for dependency management, we install poetry in the virtual env using pip and then install all the dependencies using the following two commands
    ```bash 
    pip install poetry
    poetry install
    ```

* Create a test database named `metadata_db` and populate it using the [metadata_db.sql](../tests/metadata_db.sql)
    ```bash 
    psql -U postgres -c "create database metadata_db;"
    psql -U postgres -f tests/metadata_db.sql metadata_db
    ```
* Sometimes, there could be issues connecting to psql database which can be identified and fixed by looking at [postgres.conf](https://www.postgresql.org/docs/8.2/runtime-config-connection.html) and [pg_hba.conf](https://www.postgresql.org/docs/8.2/auth-pg-hba-conf.html) files.

> Note: The next two steps may be used as a reference.

*  The location of these postgres config files(say `<PG_HOME>`)  can be retrieved by running the following command 
    ```bash 
    psql -U postgres -c 'SHOW config_file'
    ```
* Run the following two commands
    ```bash
    sudo sed -i "s|#listen_addresses = 'localhost'|listen_addresses = '*'|g" <PG_HOME>/postgresql.conf
    sudo sed -i "s|# DO NOT DISABLE!|host all all 0.0.0.0/0 trust\n\n# DO NOT DISABLE!|g" <PG_HOME>/pg_hba.conf
    ```
    * For Mac users, the first argument for `sed -i` is supposed to be an empty string since Mac uses BSD based sed which [is different from GNU based sed](https://unix.stackexchange.com/questions/401905/bsd-sed-vs-gnu-sed-and-i)
    
* After updating the above config files [restart postgres service](https://sqlserverguides.com/restart-postgres/) .



## Before running tests 

* Make sure you are checked out into the branch for which you'd like to run the tests. 
* Clone [compose-etl git repo](https://github.com/uc-cdis/compose-etl.git) into a different directory and checkout to master branch.
    ```bash
    git clone https://github.com/uc-cdis/compose-etl.git --branch master --single-branch
    ```
* From `tube` directory copy `etlMapping.yaml` and `user.yaml` files from [tests/gen3/tube/](../tests/gen3/tube) into `compose-etl/configs` directory.
    ```bash
    cp -f tests/gen3/tube/etlMapping.yaml compose-etl/configs/etlMapping.yaml
    cp -f tests/gen3/tube/user.yaml compose-etl/configs/user.yaml
    ```
* The next step is to setup creds for postgres in the `compose-etl/configs` directory in which one of the keys is `db_host` which needs to be configured differently for mac and linux. 

    * In case of mac, we hardcode the value to be `host.docker.internal` but in case of linux environment, we can fetch the host ip address by running the following command
    ```bash
    # For Linux
    export HOST_IP_ADDRESS="$(ip -f inet -o addr show docker0 | cut -d\  -f 7 | cut -d/ -f 1)"

    # For Mac
    export HOST_IP_ADDRESS=host.docker.internal

    cd compose-etl
    echo "{\"db_host\":\"$HOST_IP_ADDRESS\",\"db_username\":\"postgres\",\"db_password\":\"postgres\",\"db_database\":\"metadata_db\"}" > configs/creds.json 
    ```
* Open the `compose-etl/docker-compose.yaml` and replace `tube:master` with `tube:<BRANCH_NAME>` such that all the forward slashes (`/`) in the branch name are replaced with underscores(`_`). 

    Or run the following sed command where `$BRANCH` represents the branch name you want to test
    ```bash
    export BRANCH_NAME=<YOUR_BRANCH_NAME> #e.g feat/test_tube_filter

    #For Linux
    sed -i "s/tube:master/tube:${BRANCH//\//_}/g" docker-compose.yml

    #For Mac
    sed -i '' "s/tube:master/tube:${BRANCH//\//_}/g" docker-compose.yml
    ```

* Launch both `spark` and `elasticsearch` docker containers 
    ```bash
    docker-compose up -d spark elasticsearch
    ```

* Later run the following commands on the tube container to execute the etl process
    ```bash
    docker-compose run tube bash -c "python run_config.py; python run_etl.py"
    ```

* Export the following environment variables which are required to run the tests
    ```bash
    export XDG_DATA_HOME="$HOME/.local/share"
    export ES_URL="localhost"
    export ES_INDEX_NAME="etl"
    export DICTIONARY_URL="https://s3.amazonaws.com/dictionary-artifacts/ndhdictionary/3.3.8/schema.json"
    ```

* Change into tube directory and copy the config files into `$XDG_DATA_HOME/gen3/tube/`
    ```bash
    cd <tube_repo>
    mkdir -p $XDG_DATA_HOME/gen3/tube
    cp tests/gen3/tube/{creds.json,etlMapping.yaml,user.yaml} $XDG_DATA_HOME/gen3/tube/
    ```

* Run the tests using pytest
    ```bash
    python -m pytest -v tests
    ```
