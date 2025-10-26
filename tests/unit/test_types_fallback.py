import importlib.util
import sys
import types as pytypes

from typing_extensions import TypedDict as ExtensionsTypedDict


def test_types_module_uses_typing_extensions_when_typed_dict_missing() -> None:
    """Reload the types module without typing.TypedDict to exercise the fallback branch."""
    import mysql_to_sqlite3.types as original_module

    module_path = original_module.__file__
    assert module_path is not None

    # Swap in a stripped-down typing module that lacks TypedDict.
    real_typing = sys.modules["typing"]
    fake_typing = pytypes.ModuleType("typing")
    fake_typing.__dict__.update({k: v for k, v in real_typing.__dict__.items() if k != "TypedDict"})
    sys.modules["typing"] = fake_typing

    try:
        spec = importlib.util.spec_from_file_location("mysql_to_sqlite3.types_fallback", module_path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        sys.modules["typing"] = real_typing
        sys.modules.pop("mysql_to_sqlite3.types_fallback", None)

    assert module.TypedDict is ExtensionsTypedDict
