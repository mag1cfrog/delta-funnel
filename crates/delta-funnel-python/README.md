# DeltaFunnel Python Package

This crate is the minimal PyO3/maturin package scaffold for the `deltafunnel`
Python module. It intentionally exposes no workflow API yet.

Build and import locally:

```bash
cd crates/delta-funnel-python
maturin build
python -m venv .venv
. .venv/bin/activate
python -m pip install ../../target/wheels/deltafunnel-*.whl
python -c "import deltafunnel"
```
