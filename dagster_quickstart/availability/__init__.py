"""Generic (role, currency) -> series_code availability resolution over a metadata catalog.

This package defines the SHAPE of an availability check -- how a driver "role" (e.g. a 2Y
interest rate swap) resolves to a real series_code for a given currency, how a currency pair's
two legs are identified, and how a per-pair report of what resolved/what's missing is built and
persisted. It has no opinion on WHICH roles matter or what a pair needs them for -- those are
STEER's answers (see STEER_AVAILABILITY_SPEC, defined alongside STEER's own configuration),
supplied to every function here as an explicit AvailabilitySpec argument.

This package imports nothing from STEER's own code (see
tests/test_availability_package_structure.py's guard test) -- STEER imports FROM here
(AvailabilitySpec, PairAvailability), never the reverse. Inverting this (moving STEER's role
definitions into this package too) would make the package define STEER's five drivers, which
defeats the point of extracting it.

    spec.py     -- AvailabilitySpec: the shape (role_filters, required_roles, variants).
    pairs.py    -- parse_fx_legs, and the base/quote/USD leg vocabulary.
    roles.py    -- RoleResolver: (role, currency) -> series_code, from one metadata snapshot.
    report.py   -- PairAvailability, assess_pair_availability, build_availability_report.
    storage.py  -- write_report/read_latest_report: persist/retrieve a report via DataAPI.
"""
