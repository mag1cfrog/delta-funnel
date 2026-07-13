# Private S3 Delta Sources

Delta Funnel copies shell `AWS_*` variables into the storage options used for
S3-compatible sources. Explicit `storage_options` values override equivalent
environment variables, which makes them useful for per-source credentials and
regions.

## Override credentials and region

Use these keys:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN` as optional
- `AWS_REGION`

Delta Funnel also accepts these common lowercase aliases:

- `aws_access_key_id`
- `aws_secret_access_key`
- `aws_session_token`
- `aws_region`
- `region`

```python
import os
from deltafunnel import Session

storage_options = {
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
    "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
}
if os.environ.get("AWS_SESSION_TOKEN"):
    storage_options["AWS_SESSION_TOKEN"] = os.environ["AWS_SESSION_TOKEN"]

source = Session().delta_lake(
    "s3://<private-bucket>/<delta-table>",
    storage_options=storage_options,
    name="source",
)
```

The region alone is not enough:

```python
Session().delta_lake(
    "s3://<private-bucket>/<delta-table>",
    storage_options={"region": "us-east-1"},
    name="source",
)
```

`region` is a supported key, but it does not provide credentials.

## Troubleshoot credential discovery

If the same table works in `deltalake` but fails in `deltafunnel`, compare the
effective `AWS_*` environment variables and explicit `storage_options` first.
The libraries can use different credential-provider paths.

Enable detailed source logging:

```python
import deltafunnel

deltafunnel.init_logging(
    "delta_funnel=debug,delta_kernel=debug,object_store=debug"
)
```

Look for `object_store` messages that show which credential-provider path was
selected.
