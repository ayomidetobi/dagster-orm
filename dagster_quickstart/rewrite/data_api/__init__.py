"""DuckLake-based semantic data API rewrite."""

__all__ = [
    "DataAPI",
    "QuerySet",
    "RewriteContainer",
    "build_rewrite_container",
    "create_container",
    "create_data_api",
    "configure_logging",
]

_EXPORTS = {
    "DataAPI": "rewrite.data_api.api.data_api",
    "QuerySet": "rewrite.data_api.api.queryset",
    "RewriteContainer": "rewrite.data_api.container",
    "build_rewrite_container": "rewrite.data_api.container",
    "create_container": "rewrite.data_api.factory",
    "create_data_api": "rewrite.data_api.factory",
    "configure_logging": "rewrite.data_api.logging",
}


def __getattr__(name: str) -> object:
    """Import exports lazily (see rewrite/__init__.py for why)."""
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    module = importlib.import_module(module_name)
    return getattr(module, name)
