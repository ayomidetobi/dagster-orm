"""Math out: rolling-window OLS estimation, cointegration testing, signal generation, and the
PairResult output artifact -- NO I/O at all. Every function here takes pandas objects in and
returns pandas objects out, so this whole layer is testable against synthetic series with no
database, no S3, no DuckLake attach.

Must never import the DuckLake data-access layer (see tests/test_steer_package_structure.py's
no-I/O guard) -- and of the rest of steer/ may import steer.constants/steer.errors only, never
steer.source, steer.config, steer.orm, steer.model, or steer.run.
"""
