# Private S3 Delta Sources

Delta Funnel copies shell `AWS_*` variables into the storage options used for
S3-compatible sources. Explicit `storage_options` values override equivalent
environment variables, which makes them useful for per-source credentials and
regions.

## Use shell credentials

Set these variables in the environment that starts Python:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `AWS_SESSION_TOKEN` when using temporary credentials

Delta Funnel uses them automatically, so the source does not need explicit
`storage_options`:

```python
from deltafunnel import Session

source = Session().delta_lake(
    "s3://<private-bucket>/<delta-table>",
    name="source",
)
```

## Override one source

Pass `storage_options` when one source needs credentials or a region that
differs from the process environment. Use these keys:

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
    "AWS_REGION": os.environ["ORDERS_AWS_REGION"],
    "AWS_ACCESS_KEY_ID": os.environ["ORDERS_AWS_ACCESS_KEY_ID"],
    "AWS_SECRET_ACCESS_KEY": os.environ["ORDERS_AWS_SECRET_ACCESS_KEY"],
}
if os.environ.get("ORDERS_AWS_SESSION_TOKEN"):
    storage_options["AWS_SESSION_TOKEN"] = os.environ["ORDERS_AWS_SESSION_TOKEN"]

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
