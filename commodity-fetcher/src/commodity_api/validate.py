def validate_api_response(raw_data: dict) -> None:
    """Validate the raw FRED response structure. Raises ValueError on problems.

    Structural checks only — does not reject '.' (non-trading-day) values, that is a
    transform-time concern.
    """
    observations = raw_data.get("observations")
    if not observations or not isinstance(observations, list):
        raise ValueError(f"FRED response missing or empty 'observations' (got {type(observations).__name__})")

    first = observations[0]
    if "date" not in first or "value" not in first:
        raise ValueError(f"FRED observation missing 'date'/'value' fields: {first!r}")
