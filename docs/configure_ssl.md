# Configuring SSL

If you're using a PostgreSQL Database server with ssl connectivity, you can update the `creds.json`.

`db_use_ssl` is currently optional.

```json
{
   "db_host": "localhost",
   "db_username": "postgres",
   "db_password": "postgres",
   "db_database": "metadata_db",
   "db_use_ssl" : false
}
```
vs. Azure PostgreSQL which uses ssl by default:

```json
{
    "db_host": "my-server.postgres.database.azure.com",
    "db_username": "my-user@my-server",
    "db_password": "mypassword",
    "db_database": "metadata_db",
    "db_use_ssl" : true
}
```

The `creds.json` could be added to the following path to get picked up by `tube`:

```bash
export XDG_DATA_HOME="$HOME/.local/share"
ls $XDG_DATA_HOME/gen3/tube/creds.json
```
