"""Native library interface for datahike.

This module provides the low-level ctypes interface to libdatahike.
It handles library loading, isolate initialization, and result parsing.

The callback-based architecture avoids shared mutable memory between
Python and the GraalVM native image.
"""
from ctypes import (
    CDLL, CFUNCTYPE, POINTER,
    c_void_p, c_char_p, c_long,
    byref, ARRAY
)
import os
import io
import json
import base64
import cbor2
from typing import Any, List, Tuple, Optional, Callable, Literal

# Type aliases for better documentation
OutputFormat = Literal["json", "edn", "cbor"]
InputFormat = str  # "db", "history", "since:<timestamp>", "asof:<timestamp>"


class DatahikeException(Exception):
    """Exception raised by Datahike operations."""
    pass


# =============================================================================
# Library Loading
# =============================================================================

def _find_library() -> str:
    """Find libdatahike shared library.

    Returns:
        Path to library

    Raises:
        FileNotFoundError: If library not found in any location
    """
    # Check environment variable first
    if 'LIBDATAHIKE_PATH' in os.environ:
        path = os.environ['LIBDATAHIKE_PATH']
        if os.path.exists(path):
            return path
        raise FileNotFoundError(
            f"LIBDATAHIKE_PATH points to non-existent file: {path}"
        )

    # Check common locations relative to this file
    possible_paths = [
        # Relative to datahike repo
        os.path.join(os.path.dirname(__file__), '..', '..', '..', '..',
                     'libdatahike', 'target', 'libdatahike.so'),
        # Linux system paths
        '/usr/local/lib/libdatahike.so',
        '/usr/lib/libdatahike.so',
        # macOS
        os.path.join(os.path.dirname(__file__), '..', '..', '..', '..',
                     'libdatahike', 'target', 'libdatahike.dylib'),
        '/usr/local/lib/libdatahike.dylib',
    ]

    for path in possible_paths:
        abs_path = os.path.abspath(path)
        if os.path.exists(abs_path):
            return abs_path

    # Provide helpful error with all attempted paths
    tried_paths = '\n'.join(f'  - {os.path.abspath(p)}' for p in possible_paths)
    raise FileNotFoundError(
        f"Could not find libdatahike shared library.\n\n"
        f"Tried the following locations:\n{tried_paths}\n\n"
        f"To fix this:\n"
        f"  1. Build the native library: cd datahike && bb ni-compile\n"
        f"  2. Or set LIBDATAHIKE_PATH environment variable to the library path"
    )


# Lazy initialization - don't load library at import time
_dll: Optional[CDLL] = None
_isolatethread: Optional[c_void_p] = None


def _ensure_initialized() -> None:
    """Ensure native library is loaded and GraalVM isolate is initialized.

    Raises:
        RuntimeError: If GraalVM isolate creation fails
    """
    global _dll, _isolatethread

    if _dll is not None:
        return  # Already initialized

    # Find and load library
    lib_path = _find_library()
    _dll = CDLL(lib_path)

    # Initialize GraalVM isolate
    isolate = c_void_p()
    _isolatethread = c_void_p()

    if _dll.graal_create_isolate(None, byref(isolate), byref(_isolatethread)) != 0:
        raise RuntimeError(
            "Failed to initialize GraalVM isolate. "
            "The native library may be corrupted. Try rebuilding: bb ni-compile"
        )


# =============================================================================
# Callback Types
# =============================================================================

# Callback function type for receiving results
CALLBACK_FUNC = CFUNCTYPE(c_void_p, c_char_p)


# =============================================================================
# Result Parsing
# =============================================================================

def _cbor_tag_hook(decoder, tag, shareable_index=None):
    """Handle CBOR tags (e.g., Clojure keywords)."""
    # Tag 39 is used for Clojure keywords - just return the value
    if tag.tag == 39:
        return tag.value
    return tag.value


def parse_result(data: bytes, output_format: OutputFormat) -> Any:
    """Parse result bytes based on output format.

    Args:
        data: Raw bytes from callback
        output_format: One of 'json', 'edn', 'cbor'

    Returns:
        Parsed result

    Raises:
        DatahikeException: If result is an exception
        ValueError: If output_format is unknown
    """
    if len(data) == 0:
        return None

    # Check for exception
    if data.startswith(b"exception:"):
        raise DatahikeException(data.decode("utf8").replace("exception:", ""))

    if output_format == "json":
        return json.loads(data)
    elif output_format == "edn":
        return data.decode("utf8")  # Return as string for now
    elif output_format == "cbor":
        # Decode from base64 string, stripping any whitespace/newlines
        decoded = base64.b64decode(data.strip())
        return cbor2.loads(decoded, tag_hook=_cbor_tag_hook)
    else:
        raise ValueError(
            f"Unknown output format: {output_format!r}. "
            f"Expected one of: 'json', 'edn', 'cbor'"
        )


def make_callback(output_format: OutputFormat) -> Tuple[CALLBACK_FUNC, Callable[[], Any]]:
    """Create a callback function and result getter.

    Args:
        output_format: Format for parsing results

    Returns:
        Tuple of (callback_func, get_result_func)
    """
    result = None
    exception = None

    def callback(data: bytes) -> None:
        nonlocal result, exception
        try:
            result = parse_result(data, output_format)
        except Exception as e:
            exception = e

    def get_result() -> Any:
        if exception is not None:
            raise exception
        return result

    return CALLBACK_FUNC(callback), get_result


# =============================================================================
# Query Input Helpers
# =============================================================================

def prepare_query_inputs(inputs: List[Tuple[InputFormat, str]]) -> Tuple[int, Any, Any]:
    """Prepare query inputs for native call.

    Args:
        inputs: List of (format, value) tuples
                Format can be 'db', 'history', 'since:<timestamp>', 'asof:<timestamp>', or 'param'

    Returns:
        Tuple of (count, formats_array, values_array)
    """
    n = len(inputs)
    char_p_array = ARRAY(c_char_p, n)
    formats = char_p_array()
    values = char_p_array()

    for i, (fmt, val) in enumerate(inputs):
        formats[i] = fmt.encode("utf8")
        values[i] = val.encode("utf8")

    return n, formats, values


# =============================================================================
# Native Function Wrappers
# =============================================================================

def get_isolatethread() -> c_void_p:
    """Get the current isolate thread context.

    Ensures library is initialized before returning thread context.

    Returns:
        GraalVM isolate thread pointer
    """
    _ensure_initialized()
    assert _isolatethread is not None
    return _isolatethread


def get_dll() -> CDLL:
    """Get the loaded native library.

    Ensures library is initialized before returning.

    Returns:
        Loaded CDLL instance
    """
    _ensure_initialized()
    assert _dll is not None
    return _dll
