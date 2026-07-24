# Delta Funnel Docs Site

Build the site locally with:

```bash
python -m pip install -r docs-site/requirements.txt && python -m zensical build --strict -f docs-site/mkdocs.yml
```

Serve it locally with:

```bash
python -m zensical serve -f docs-site/mkdocs.yml
```

## Content structure

Before adding or expanding a page, choose one primary reader and one user goal.
Place the content in the matching navigation section:

- `Start here`: the shortest path for a new user to reach a first result.
- `Common tasks`: problem-oriented guides for supported user workflows.
- `Advanced > Troubleshooting`: failure investigation and safe diagnostic
  collection.
- `Advanced > Profiling`: method selection and tool-specific performance
  workflows.
- `Reference`: precise API facts meant for lookup rather than guided reading.
- `Contributors`: repository development, testing, and implementation details.

Keep the complete explanation of a topic on one owner page. Other pages should
include only the context needed for their own task and link to that owner page.
Do not move an existing published file only to mirror the navigation hierarchy;
preserving its public URL is more important than matching the folder layout.
