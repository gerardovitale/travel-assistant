import json
import logging
import subprocess
import time

from spain_fuel_api.constants import DATA_SOURCE_URL
from spain_fuel_api.constants import USER_AGENT

logger = logging.getLogger(__name__)


def fetch_raw_data(
    curl_timeout: int = 120,
    connect_timeout: int = 10,
    max_retries: int = 3,
    retry_base_delay: int = 10,
    exponential_backoff: bool = True,
) -> dict:
    """Fetch raw fuel-price JSON from the Spain government API using curl.

    Python's ssl module (OpenSSL 3.x) is blocked by the server's TLS fingerprinting,
    while curl's TLS handshake is accepted. We use subprocess + curl to bypass this.

    Retries on curl error/timeout. With exponential_backoff the delay is
    retry_base_delay * 2**(attempt-1); otherwise it is a fixed retry_base_delay.
    Raises subprocess.CalledProcessError / subprocess.TimeoutExpired after exhaustion.
    """
    logger.info("Fetching fuel price raw data from %s", DATA_SOURCE_URL)
    for attempt in range(1, max_retries + 1):
        try:
            result = subprocess.run(
                [
                    "curl",
                    "-s",
                    "-f",
                    "-L",
                    "--connect-timeout",
                    str(connect_timeout),
                    "--max-time",
                    str(curl_timeout),
                    "--tlsv1.2",
                    "--tls-max",
                    "1.2",
                    "-H",
                    "Accept: application/json",
                    "-A",
                    USER_AGENT,
                    DATA_SOURCE_URL,
                ],
                capture_output=True,
                text=True,
                check=True,
                timeout=curl_timeout + connect_timeout,
            )
            return json.loads(result.stdout)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            if attempt < max_retries:
                delay = retry_base_delay * (2 ** (attempt - 1)) if exponential_backoff else retry_base_delay
                logger.warning("Fetch attempt %d/%d failed: %s. Retrying in %ds...", attempt, max_retries, exc, delay)
                time.sleep(delay)
            else:
                logger.error("All %d fetch attempts failed: %s", max_retries, exc)
                raise
    raise RuntimeError("All fetch attempts exhausted")
