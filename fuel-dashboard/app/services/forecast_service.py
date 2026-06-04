import pandas as pd
from api.schemas import FuelType
from api.schemas import HistoricalForecastResponse

from data.gcs_client import download_aggregate
from data.geojson_loader import normalize_data_province_name

REGIMES = ("cheap", "normal", "expensive")
REGIME_INDEX = {regime: idx for idx, regime in enumerate(REGIMES)}
REGIME_LABELS = {"cheap": "barato", "normal": "normal", "expensive": "caro"}
GEOGRAPHY_LABELS = {"zip_code": "Código postal", "province": "Provincia"}
ZIP_CODE_AGGREGATE = "zip_code_daily_stats.parquet"
PROVINCE_AGGREGATE = "province_daily_stats.parquet"
MIN_OBSERVATION_DAYS = 60
# Minimum consecutive-day transitions before a chain is served. A 3-state row estimated from
# one observation is a point estimate (matrix entry 1.0); ~10 per row is the pragmatic floor.
# `confidence` remains the soft signal above this hard floor.
MIN_TRANSITIONS = 30
DEFAULT_WINDOW_DAYS = 180


def _normalize_zip(value: object) -> str:
    """Canonical Spanish postal code: strip float suffix (``28001.0``) and zero-pad to 5 digits."""
    return str(value).split(".", 1)[0].strip().zfill(5)


def _prepare_history(df: pd.DataFrame | None, value_column: str = "avg_price") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", value_column])

    history = df.copy()
    history["date"] = pd.to_datetime(history["date"])
    history = history.dropna(subset=["date", value_column])
    history = history.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    history[value_column] = pd.to_numeric(history[value_column], errors="coerce")
    history = history[history[value_column].notna()]
    return history.reset_index(drop=True)


def _limit_recent_window(history: pd.DataFrame, window_days: int) -> pd.DataFrame:
    if history.empty:
        return history
    latest_date = history["date"].max()
    cutoff = latest_date - pd.Timedelta(days=max(1, window_days) - 1)
    return history[history["date"] >= cutoff].reset_index(drop=True)


def _load_zip_history(zip_code: str, fuel_type: FuelType, window_days: int) -> pd.DataFrame:
    aggregate = download_aggregate(ZIP_CODE_AGGREGATE)
    if aggregate is None or aggregate.empty:
        return pd.DataFrame()
    agg_zip = aggregate["zip_code"].astype(str).str.split(".", n=1).str[0].str.strip().str.zfill(5)
    filtered = aggregate[(agg_zip == _normalize_zip(zip_code)) & (aggregate["fuel_type"] == fuel_type.value)][
        ["date", "zip_code", "province", "avg_price"]
    ]
    return _limit_recent_window(_prepare_history(filtered), window_days)


def _load_province_history(province: str, fuel_type: FuelType, window_days: int) -> pd.DataFrame:
    aggregate = download_aggregate(PROVINCE_AGGREGATE)
    if aggregate is None or aggregate.empty:
        return pd.DataFrame()
    normalized = normalize_data_province_name(province)
    filtered = aggregate[(aggregate["province"] == normalized) & (aggregate["fuel_type"] == fuel_type.value)][
        ["date", "province", "avg_price"]
    ]
    return _limit_recent_window(_prepare_history(filtered), window_days)


def _assign_regimes(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return history.assign(regime=pd.Series(dtype="object"))

    low = history["avg_price"].quantile(1 / 3)
    high = history["avg_price"].quantile(2 / 3)

    def classify(price: float) -> str:
        if price <= low:
            return "cheap"
        if price >= high:
            return "expensive"
        return "normal"

    result = history.copy()
    result["regime"] = result["avg_price"].apply(classify)
    return result


def _build_transition_counts(history: pd.DataFrame) -> dict[str, dict[str, int]]:
    counts = {src: {dst: 0 for dst in REGIMES} for src in REGIMES}
    if history.empty:
        return counts

    ordered = history.sort_values("date").reset_index(drop=True)
    ordered["next_regime"] = ordered["regime"].shift(-1)
    ordered["next_date"] = ordered["date"].shift(-1)
    ordered["date_diff_days"] = (ordered["next_date"] - ordered["date"]).dt.days
    transitions = ordered[ordered["next_regime"].notna() & (ordered["date_diff_days"] == 1)][["regime", "next_regime"]]

    for _, row in transitions.iterrows():
        counts[row["regime"]][row["next_regime"]] += 1
    return counts


def _transition_matrix(counts: dict[str, dict[str, int]]) -> dict[str, dict[str, float]]:
    # Keep full precision so row sums stay exactly 1.0; per-entry rounding (e.g. 0.3333 x 3 = 0.9999)
    # leaks probability mass across the multi-step matrix multiply in the reachability calc. Round
    # only at the response boundary via _round_matrix.
    matrix = {src: {dst: 0.0 for dst in REGIMES} for src in REGIMES}
    for src in REGIMES:
        total = sum(counts[src].values())
        if total <= 0:
            matrix[src][src] = 1.0
            continue
        for dst in REGIMES:
            matrix[src][dst] = counts[src][dst] / total
    assert all(abs(sum(matrix[src].values()) - 1.0) < 1e-9 for src in REGIMES)
    return matrix


def _round_matrix(matrix: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    return {src: {dst: round(prob, 4) for dst, prob in row.items()} for src, row in matrix.items()}


def _probability_of_cheaper_regime_within_days(
    matrix: dict[str, dict[str, float]], current_regime: str, days: int
) -> float:
    current_idx = REGIME_INDEX[current_regime]
    target_states = [regime for regime in REGIMES if REGIME_INDEX[regime] < current_idx]
    if not target_states or days <= 0:
        return 0.0

    non_target_states = [regime for regime in REGIMES if regime not in target_states]
    state_probs = {regime: 0.0 for regime in non_target_states}
    state_probs[current_regime] = 1.0

    for _ in range(days):
        next_probs = {regime: 0.0 for regime in non_target_states}
        for src in non_target_states:
            for dst in non_target_states:
                next_probs[dst] += state_probs[src] * matrix[src][dst]
        state_probs = next_probs

    avoid_probability = sum(state_probs.values())
    # Full precision; rounding happens at the response boundary in _build_response.
    return max(0.0, min(1.0, 1.0 - avoid_probability))


def _expected_days_in_regime(matrix: dict[str, dict[str, float]], current_regime: str) -> float | None:
    stay_probability = matrix[current_regime][current_regime]
    if stay_probability >= 0.9999:
        return None
    return round(1.0 / max(1e-6, 1.0 - stay_probability), 1)


def _confidence_score(counts: dict[str, dict[str, int]]) -> float:
    transition_total = sum(sum(destinations.values()) for destinations in counts.values())
    if transition_total <= 0:
        return 0.0

    populated_rows = [destinations for destinations in counts.values() if sum(destinations.values()) > 0]
    concentration = sum(max(row.values()) / sum(row.values()) for row in populated_rows) / len(populated_rows)
    count_score = min(1.0, transition_total / 90.0)
    return round((0.6 * count_score) + (0.4 * concentration), 3)


def _explanation(
    geography_type: str,
    geography_value: str,
    current_regime: str,
    cheaper_3d: float,
    cheaper_7d: float,
    confidence: float,
) -> str:
    geo_label = GEOGRAPHY_LABELS.get(geography_type, geography_type)
    regime_label = REGIME_LABELS.get(current_regime, current_regime)
    location_label = f"{geo_label} {geography_value}"
    message = (
        f"El área {location_label} está en un régimen {regime_label}. "
        f"La probabilidad de ver un régimen más barato en 3 días es del {cheaper_3d * 100:.0f}% "
        f"y en 7 días del {cheaper_7d * 100:.0f}%."
    )
    if confidence < 0.45:
        return f"{message} La confianza es baja por la variabilidad o escasez de transiciones."
    return message


def _recommendation(current_regime: str, cheaper_3d: float, cheaper_7d: float) -> str:
    if current_regime == "cheap":
        return "Reposta hoy"
    if current_regime == "expensive" and (cheaper_3d >= 0.55 or cheaper_7d >= 0.75):
        return "Puedes esperar"
    if current_regime == "normal" and cheaper_3d >= 0.6:
        return "Puedes esperar"
    return "Reposta hoy"


def _insufficient_response(
    geography_type: str,
    geography_value: str,
    source: str,
    explanation: str,
) -> HistoricalForecastResponse:
    return HistoricalForecastResponse(
        geography_type=geography_type,
        geography_value=geography_value,
        source=source,
        recommendation="Sin suficiente historico",
        explanation=explanation,
        insufficient_data=True,
    )


def _build_response(
    history: pd.DataFrame,
    geography_type: str,
    geography_value: str,
    source: str,
) -> HistoricalForecastResponse:
    history = _assign_regimes(history)
    distinct_states = history["regime"].nunique()
    counts = _build_transition_counts(history)
    transition_total = sum(sum(destinations.values()) for destinations in counts.values())

    if len(history) < MIN_OBSERVATION_DAYS or distinct_states < 2 or transition_total < MIN_TRANSITIONS:
        return _insufficient_response(
            geography_type,
            geography_value,
            source,
            "No hay suficiente historico diario consistente para construir una cadena de Markov util.",
        )

    matrix = _transition_matrix(counts)
    rounded_matrix = _round_matrix(matrix)
    current_row = history.iloc[-1]
    current_regime = str(current_row["regime"])
    confidence = _confidence_score(counts)
    # Probabilities stay full precision through recommendation/explanation; rounded only for the response.
    cheaper_3d = _probability_of_cheaper_regime_within_days(matrix, current_regime, 3)
    cheaper_7d = _probability_of_cheaper_regime_within_days(matrix, current_regime, 7)

    return HistoricalForecastResponse(
        geography_type=geography_type,
        geography_value=geography_value,
        source=source,
        coverage_days=int(history["date"].nunique()),
        transition_observations=int(transition_total),
        current_date=pd.Timestamp(current_row["date"]).date().isoformat(),
        current_avg_price=round(float(current_row["avg_price"]), 4),
        current_regime=current_regime,
        next_day_probabilities=rounded_matrix[current_regime],
        cheaper_within_3d=round(cheaper_3d, 4),
        cheaper_within_7d=round(cheaper_7d, 4),
        expected_days_in_current_regime=_expected_days_in_regime(matrix, current_regime),
        confidence=confidence,
        recommendation=_recommendation(current_regime, cheaper_3d, cheaper_7d),
        explanation=_explanation(
            geography_type,
            geography_value,
            current_regime,
            cheaper_3d,
            cheaper_7d,
            confidence,
        ),
        insufficient_data=False,
        transition_matrix=rounded_matrix,
    )


def get_historical_forecast(
    fuel_type: FuelType,
    *,
    zip_code: str | None = None,
    province: str | None = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
) -> HistoricalForecastResponse:
    if zip_code:
        zip_history = _load_zip_history(zip_code, fuel_type, window_days)
        if not zip_history.empty:
            response = _build_response(
                zip_history[["date", "avg_price"]],
                "zip_code",
                str(zip_code),
                "zip_code",
            )
            if not response.insufficient_data:
                return response
            inferred_province = zip_history["province"].dropna().astype(str)
            province = province or (inferred_province.iloc[-1] if not inferred_province.empty else None)
        elif province is None:
            return _insufficient_response(
                "zip_code",
                str(zip_code),
                "zip_code",
                "No hay historico agregado para ese codigo postal y no se ha indicado una provincia alternativa.",
            )

    if province:
        province_history = _load_province_history(province, fuel_type, window_days)
        if province_history.empty:
            return _insufficient_response(
                "province",
                str(province),
                "province",
                "No hay historico agregado para la provincia seleccionada.",
            )
        return _build_response(province_history[["date", "avg_price"]], "province", str(province), "province")

    raise ValueError("zip_code or province is required")
