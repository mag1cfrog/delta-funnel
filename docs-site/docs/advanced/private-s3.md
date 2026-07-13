# Private S3 Delta Sources

Pass explicit credentials and a region in `storage_options` when reading a
private S3 Delta table from a local shell. Delta Funnel forwards these values
to its underlying object-store builder.

On the current S3 path, Delta Funnel does not auto-load shell `AWS_*` variables,
`AWS_PROFILE`, or shared AWS config and credentials files.

## Pass credentials and region

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

If the same table works in `deltalake` but fails in `deltafunnel`, the likely
cause is a credential-discovery path mismatch, not a Delta snapshot or protocol
problem.

Enable detailed source logging:

```python
import deltafunnel

deltafunnel.init_logging(
    "delta_funnel=debug,delta_kernel=debug,object_store=debug"
)
```

Look for `object_store` messages that show which credential-provider path was
selected.
