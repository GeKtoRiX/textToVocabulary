import json
import time
import urllib.error
import urllib.request


try:
    import urllib3
except Exception:  # pragma: no cover - optional dependency
    urllib3 = None


RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


class HttpClient:
    def __init__(self, *, max_retries: int = 2, backoff: float = 0.4, timeout: int = 90):
        self._max_retries = max_retries
        self._backoff = backoff
        self._timeout = timeout
        self._pool = None
        if urllib3 is not None:
            retry = urllib3.Retry(
                total=max_retries,
                read=max_retries,
                connect=max_retries,
                backoff_factor=backoff,
                status_forcelist=sorted(RETRY_STATUS_CODES),
                allowed_methods=frozenset(["POST"]),
                raise_on_status=False,
            )
            self._pool = urllib3.PoolManager(retries=retry)

    def post_json(self, url: str, payload: dict, *, timeout: int | None = None) -> str:
        body = json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        request_timeout = timeout if timeout is not None else self._timeout

        if self._pool is not None:
            response = self._pool.request(
                "POST",
                url,
                body=body,
                headers=headers,
                timeout=request_timeout,
            )
            if 200 <= response.status < 300:
                return response.data.decode("utf-8")
            detail = response.data.decode("utf-8", errors="ignore")
            raise RuntimeError(
                f"Request failed ({response.status}): {detail or response.reason}"
            )

        for attempt in range(self._max_retries + 1):
            try:
                request = urllib.request.Request(url, data=body, headers=headers)
                with urllib.request.urlopen(request, timeout=request_timeout) as response:
                    return response.read().decode("utf-8")
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="ignore")
                if attempt < self._max_retries and exc.code in RETRY_STATUS_CODES:
                    self._sleep(attempt)
                    continue
                raise RuntimeError(
                    f"Request failed ({exc.code}): {detail or exc.reason}"
                ) from exc
            except urllib.error.URLError as exc:
                if attempt < self._max_retries:
                    self._sleep(attempt)
                    continue
                raise RuntimeError(f"Request failed: {exc.reason}") from exc

        raise RuntimeError("Request failed after retries.")

    def _sleep(self, attempt: int) -> None:
        time.sleep(self._backoff * (2**attempt))
