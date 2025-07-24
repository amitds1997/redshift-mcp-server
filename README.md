# Redshift MCP server

This repository contains the code for the Redshift MCP server. This MCP server allows you to connect to Redshift cluster and execute SQL queries.

Has been tested up to 100+ rows of output. For larger output, your mileage may vary depending upon the total size of the output.

## Setup

1. Note down the path where this directory is located. You will need it to configure the MCP client. For now, it is assumed to be `<path_to_redshift_mcp>`.
2. In your MCP configuration file, add the following configuration. If your MCP client supports envFile, you can also add that with these variables and the server would read them from there.

    ```json
    {
        "command": "uv",
        "args": [
            "--directory",
            "<path_to_redshift_mcp>",
            "run",
            "server.py"
        ],
        "env": {
            "REDSHIFT_DB": "<db-name>",
            "REDSHIFT_USER": "<redshift_user>",
            "REDSHIFT_PASSWORD": "<redshift_password>",
            "REDSHIFT_HOST": "<redshift-host-uri>",
            "REDSHIFT_PORT": "5439"
        }
    }
    ```

3. In case you don't have `uv` installed, you can install it by following [uv installation guide](https://docs.astral.sh/uv/getting-started/installation/).

## Usage

1. Prefer using a read-only user for connecting to the Redshift cluster. Validations are performed to ensure that the user is not able to execute DDL statements but it is still a good practice to use a read-only user.
