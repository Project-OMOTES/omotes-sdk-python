import re
from typing import cast

MEMORY_LIMIT_RE = re.compile(r"(?i)(\d+(?:\.\d+)?)([kmgtpe]i?|)")


class MemoryLimit(str):
    """Validated Kubernetes-style memory quantity.

    Examples:
        MemoryLimit("512Mi")
        MemoryLimit("2Gi")
        MemoryLimit("750M")

    """

    def __new__(cls, value: str) -> "MemoryLimit":
        """Create a validated MemoryLimit value.

        Returns:
            MemoryLimit: Normalized and validated memory quantity.

        Raises:
            ValueError: If the value is not a supported memory quantity.

        """
        normalized = value.strip()
        if not MEMORY_LIMIT_RE.fullmatch(normalized):
            raise ValueError("Unsupported memory quantity format. Examples: '512Mi', '2Gi', '750M', '1000000'.")
        return cast("MemoryLimit", str.__new__(cls, normalized))


def _memory_quantity_to_bytes(memory_limit: MemoryLimit | str) -> int:
    """Convert a Kubernetes-style memory quantity into bytes for Docker.

    Returns:
        int: Memory quantity in bytes.

    Raises:
        ValueError: If the input memory quantity has an unsupported format.

    """
    normalized = str(memory_limit).strip()
    match = MEMORY_LIMIT_RE.fullmatch(normalized)
    if not match:
        raise ValueError(f"Unsupported memory quantity: {memory_limit!r}")

    value = float(match.group(1))
    suffix = match.group(2).lower()
    binary_factors = {
        "": 1,
        "k": 10**3,
        "m": 10**6,
        "g": 10**9,
        "t": 10**12,
        "p": 10**15,
        "e": 10**18,
        "ki": 1024,
        "mi": 1024**2,
        "gi": 1024**3,
        "ti": 1024**4,
        "pi": 1024**5,
        "ei": 1024**6,
    }
    return int(value * binary_factors[suffix])


def _to_docker_mem_limit(value: MemoryLimit | str | int | float) -> str:
    """Convert memory values into Docker-compatible mem_limit string.

    Docker expects values parseable by docker.utils.parse_bytes, e.g. "1024m"
    or "8589934592b". We emit bytes with a trailing "b" for exact values.

    Returns:
        str: Docker-compatible memory limit string.

    """
    if isinstance(value, (int, float)):
        return f"{int(value)}b"

    normalized = str(value).strip()

    # Already a Docker parse_bytes value (supports b, k, m, g).
    lower = normalized.lower()
    docker_match = re.fullmatch(r"\d+(?:\.\d+)?[bkmg]", lower)
    if docker_match:
        return lower

    try:
        as_memory = MemoryLimit(normalized)
    except ValueError:
        # Preserve caller-supplied Docker-style values (e.g. "8g", "1024m").
        return normalized

    # Preserve decimal SI units directly when Docker can parse them.
    quantity_match = MEMORY_LIMIT_RE.fullmatch(str(as_memory).strip())
    if quantity_match:
        number = quantity_match.group(1)
        suffix = quantity_match.group(2).lower()
        if suffix in {"k", "m", "g"}:
            return f"{number}{suffix}"

    return f"{_memory_quantity_to_bytes(as_memory)}b"
