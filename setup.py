from setuptools import find_packages, setup

setup(
    name="dagster_quickstart",
    packages=find_packages(exclude=["dagster_quickstart_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "structlog",
        "pydantic",
        "dependency-injector",
        "pandera",
        "openpyxl",
        "python-decouple",
        "Markdown",
        "pyecharts",
        "PyYAML",
        "statsmodels",
    ],
    extras_require={
        "dev": ["dagster-webserver", "pytest", "ruff", "mypy", "pandas-stubs"],
        # For DataAPI.plot()/plot_values() static-image export (.png/.jpg/
        # .svg/.pdf/.gif) -- also needs Chrome/Chromium installed locally;
        # Selenium manages its own driver. HTML export needs neither extra.
        "plotting": ["snapshot-selenium", "selenium"],
    },
)
