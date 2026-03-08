#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import socket
import ssl
import time
import http.client
import urllib.parse
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


# 关键运行/测试参数（规则变化时优先修改这里）。
# 统一配置环境变量名（JSON 字符串）。
CONFIG_ENV_NAME = "CONFIG"

# 单次 HTTP 请求超时时间（秒）。
TIMEOUT_SECONDS = 30.0
# 稳定性测试中抽样次数。
RANDOM_RUNS = 10
# redirect 行为开关：True=期望 m=redirect 返回 302，False=期望回退到 proxy。
REDIRECT_ENABLED = True
# 图片文件名数字位数（例如 5 -> 00001.webp）。
IMAGE_FILENAME_DIGITS = 5
# 5xx 响应最大重试次数（不含首次请求）。
MAX_HTTP_5XX_RETRIES = 3
# 瞬时网络/读取失败时的最大重试次数（不含首次请求）。
MAX_NETWORK_RETRIES = 5
# 线性退避基数（sleep = base * attempt）。
RETRY_BACKOFF_BASE_SECONDS = 1
# 认为可重试的服务端状态码范围。
RETRYABLE_STATUS_MIN = 500
RETRYABLE_STATUS_MAX = 599

# 从统计结果筛选测试组合时允许的设备与亮度维度。
SUPPORTED_DEVICES = {"pc", "mb"}
SUPPORTED_BRIGHTNESS = {"dark", "light"}

# 一次完整测试中必须覆盖到的错误类型。
REQUIRED_ERROR_COVERAGE_KEYS = {
    "INVALID_QUERY_PARAMS",
    "INVALID_DEVICE",
    "INVALID_BRIGHTNESS",
    "INVALID_METHOD",
    "INVALID_THEME",
}
# 受数据分布影响、可能缺失的错误类型。
OPTIONAL_ERROR_COVERAGE_KEYS = {"NO_IMAGES_FOR_COMBINATION", "NO_AVAILABLE_IMAGES"}


def _normalize_asset_base_url(url: str) -> str:
    return url.rstrip("/") + "/"


def _required_config(raw_config: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw_config)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid CONFIG JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("Invalid CONFIG JSON: root must be an object")
    return parsed


def _required_config_str(config: dict[str, Any], key: str) -> str:
    value = config.get(key)
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"Missing or invalid CONFIG field: {key}")
    return value.strip()


def _mask_config_for_log(config_raw: str) -> str:
    """
    对 CONFIG_RAW 进行脱敏，只保留字段名和类型信息，不输出具体值。
    """
    try:
        parsed = json.loads(config_raw)
        if isinstance(parsed, dict):
            summary = {k: type(v).__name__ for k, v in parsed.items()}
            return f"<CONFIG fields: {summary}>"
        else:
            return "<CONFIG: not a dict>"
    except Exception:
        return "<CONFIG: invalid JSON>"


CONFIG_RAW = _required_env(CONFIG_ENV_NAME)
CONFIG = _required_config(CONFIG_RAW)

API_BASE_URL = _required_config_str(CONFIG, "API_BASE_URL")
ASSET_BASE_URL = _normalize_asset_base_url(
    _required_config_str(CONFIG, "ASSET_BASE_URL")
)
RANDOM_IMG_COUNT_PATH = "/" + _required_config_str(
    CONFIG, "RANDOM_IMG_COUNT_PATH"
).strip("/")

HIDDEN_ROUTE_QUERY_FORBIDDEN_MESSAGE_PART = "Routes do not accept query parameters"

SENSITIVE_LOG_TOKENS = sorted(
    {
        str(value).strip()
        for value in CONFIG.values()
        if isinstance(value, str) and str(value).strip()
    },
    key=len,
    reverse=True,
)

# 重定向地址格式校验正则（基于 ASSET_BASE_URL 做完整 URL 校验）。
REDIRECT_LOCATION_PATTERN = rf"^{re.escape(ASSET_BASE_URL)}(pc|mb)-(dark|light)/[a-z0-9_-]+/\d{{{IMAGE_FILENAME_DIGITS}}}\.webp$"


def _mask_url_for_log(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return "<invalid-url>"
    return "<redacted-url>"


def _redact_urls_in_text(text: str, extra_tokens: list[str] | None = None) -> str:
    value = str(text)

    redact_tokens = list(SENSITIVE_LOG_TOKENS)
    if extra_tokens:
        redact_tokens.extend(extra_tokens)

    for token in sorted(set(redact_tokens), key=len, reverse=True):
        if not token:
            continue
        value = value.replace(token, "<redacted-value>")

    def _replace(match: re.Match[str]) -> str:
        return _mask_url_for_log(match.group(0))

    return re.sub(r"https?://[^\s'\"\]\[)>,]+", _replace, value)


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


@dataclass
class HttpResult:
    status: int
    headers: dict[str, str]
    body: bytes

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")


class ApiTester:
    def __init__(
        self, api_base_url: str, asset_base_url: str, timeout: float, random_runs: int
    ) -> None:
        self.api_base_url = api_base_url.rstrip("/")
        self.asset_base_url = _normalize_asset_base_url(asset_base_url)
        self.timeout = timeout
        self.random_runs = random_runs
        self.error_coverage: dict[str, bool] = {
            "INVALID_QUERY_PARAMS": False,
            "INVALID_DEVICE": False,
            "INVALID_BRIGHTNESS": False,
            "INVALID_METHOD": False,
            "INVALID_THEME": False,
            "NO_IMAGES_FOR_COMBINATION": False,
            "NO_AVAILABLE_IMAGES": False,
        }
        self.passed = 0
        self.failed = 0
        self.failures: list[str] = []
        self.theme_tokens_for_log: list[str] = []
        self._next_assert_retry_note = ""

        self.normal_opener = urllib.request.build_opener()
        self.no_redirect_opener = urllib.request.build_opener(NoRedirectHandler())

    def register_theme_tokens(self, themes: list[str]) -> None:
        # 动态收集主题名，后续统一做日志脱敏。
        merged = {token for token in self.theme_tokens_for_log if token}
        for theme in themes:
            normalized = str(theme).strip()
            if normalized:
                merged.add(normalized)
        self.theme_tokens_for_log = sorted(merged, key=len, reverse=True)

    def redact_for_log(self, text: str) -> str:
        return _redact_urls_in_text(text, extra_tokens=self.theme_tokens_for_log)

    def _format_retry_note(self, http_5xx_retries: int, network_retries: int) -> str:
        total_retries = http_5xx_retries + network_retries
        if total_retries <= 0:
            return ""
        return f" (retries={total_retries})"

    def _url(self, path: str, query: dict[str, str] | None = None) -> str:
        if not path.startswith("/"):
            path = "/" + path
        if not query:
            return f"{self.api_base_url}{path}"
        return f"{self.api_base_url}{path}?{urllib.parse.urlencode(query)}"

    def _url_from_query_items(
        self, path: str, query_items: list[tuple[str, str]]
    ) -> str:
        if not path.startswith("/"):
            path = "/" + path
        if not query_items:
            return f"{self.api_base_url}{path}"
        return f"{self.api_base_url}{path}?{urllib.parse.urlencode(query_items)}"

    def request(
        self,
        path: str,
        query: dict[str, str] | None = None,
        follow_redirects: bool = True,
    ) -> HttpResult:
        # 统一请求入口：负责网络重试、5xx 重试和结果结构化。
        url = self._url(path, query)
        req = urllib.request.Request(url, method="GET")
        opener = self.normal_opener if follow_redirects else self.no_redirect_opener

        http_5xx_retries = 0
        network_retries = 0

        while True:
            try:
                with opener.open(req, timeout=self.timeout) as resp:
                    status = resp.getcode()
                    headers = {k.lower(): v for k, v in resp.headers.items()}
                    try:
                        body = resp.read()
                    except http.client.IncompleteRead as exc:
                        partial = bytes(exc.partial or b"")
                        if partial:
                            body = partial
                        elif network_retries < MAX_NETWORK_RETRIES:
                            network_retries += 1
                            time.sleep(RETRY_BACKOFF_BASE_SECONDS * network_retries)
                            continue
                        else:
                            raise
                    self._next_assert_retry_note = self._format_retry_note(
                        http_5xx_retries, network_retries
                    )
                    return HttpResult(
                        status=status,
                        headers=headers,
                        body=body,
                    )
            except urllib.error.HTTPError as exc:
                if (
                    RETRYABLE_STATUS_MIN <= exc.code <= RETRYABLE_STATUS_MAX
                    and http_5xx_retries < MAX_HTTP_5XX_RETRIES
                ):
                    http_5xx_retries += 1
                    time.sleep(RETRY_BACKOFF_BASE_SECONDS * http_5xx_retries)
                    continue
                try:
                    error_body = exc.read()
                except http.client.IncompleteRead as read_exc:
                    error_body = bytes(read_exc.partial or b"")
                self._next_assert_retry_note = self._format_retry_note(
                    http_5xx_retries, network_retries
                )
                return HttpResult(
                    status=exc.code,
                    headers={k.lower(): v for k, v in exc.headers.items()},
                    body=error_body,
                )
            except (
                urllib.error.URLError,
                socket.timeout,
                TimeoutError,
                ssl.SSLError,
                http.client.IncompleteRead,
                http.client.RemoteDisconnected,
                ConnectionResetError,
                OSError,
            ) as exc:
                if network_retries >= MAX_NETWORK_RETRIES:
                    self._next_assert_retry_note = self._format_retry_note(
                        http_5xx_retries, network_retries
                    )
                    return HttpResult(
                        status=599,
                        headers={},
                        body=f"request failed after retries: {exc}".encode(
                            "utf-8", errors="replace"
                        ),
                    )
                network_retries += 1
                time.sleep(RETRY_BACKOFF_BASE_SECONDS * network_retries)

        return HttpResult(
            status=599, headers={}, body=b"request failed: unexpected retry flow"
        )

    def request_query_items(
        self,
        path: str,
        query_items: list[tuple[str, str]],
        follow_redirects: bool = True,
    ) -> HttpResult:
        # 支持重复 query key（如 t=a&t=b）
        url = self._url_from_query_items(path, query_items)
        req = urllib.request.Request(url, method="GET")
        opener = self.normal_opener if follow_redirects else self.no_redirect_opener

        http_5xx_retries = 0
        network_retries = 0

        while True:
            try:
                with opener.open(req, timeout=self.timeout) as resp:
                    status = resp.getcode()
                    headers = {k.lower(): v for k, v in resp.headers.items()}
                    try:
                        body = resp.read()
                    except http.client.IncompleteRead as exc:
                        partial = bytes(exc.partial or b"")
                        if partial:
                            body = partial
                        elif network_retries < MAX_NETWORK_RETRIES:
                            network_retries += 1
                            time.sleep(RETRY_BACKOFF_BASE_SECONDS * network_retries)
                            continue
                        else:
                            raise
                    self._next_assert_retry_note = self._format_retry_note(
                        http_5xx_retries, network_retries
                    )
                    return HttpResult(
                        status=status,
                        headers=headers,
                        body=body,
                    )
            except urllib.error.HTTPError as exc:
                if (
                    RETRYABLE_STATUS_MIN <= exc.code <= RETRYABLE_STATUS_MAX
                    and http_5xx_retries < MAX_HTTP_5XX_RETRIES
                ):
                    http_5xx_retries += 1
                    time.sleep(RETRY_BACKOFF_BASE_SECONDS * http_5xx_retries)
                    continue
                try:
                    error_body = exc.read()
                except http.client.IncompleteRead as read_exc:
                    error_body = bytes(read_exc.partial or b"")
                self._next_assert_retry_note = self._format_retry_note(
                    http_5xx_retries, network_retries
                )
                return HttpResult(
                    status=exc.code,
                    headers={k.lower(): v for k, v in exc.headers.items()},
                    body=error_body,
                )
            except (
                urllib.error.URLError,
                socket.timeout,
                TimeoutError,
                ssl.SSLError,
                http.client.IncompleteRead,
                http.client.RemoteDisconnected,
                ConnectionResetError,
                OSError,
            ) as exc:
                if network_retries >= MAX_NETWORK_RETRIES:
                    self._next_assert_retry_note = self._format_retry_note(
                        http_5xx_retries, network_retries
                    )
                    return HttpResult(
                        status=599,
                        headers={},
                        body=f"request failed after retries: {exc}".encode(
                            "utf-8", errors="replace"
                        ),
                    )
                network_retries += 1
                time.sleep(RETRY_BACKOFF_BASE_SECONDS * network_retries)

        return HttpResult(
            status=599, headers={}, body=b"request failed: unexpected retry flow"
        )

    def assert_true(self, condition: bool, label: str, details: str = "") -> None:
        retry_note = self._next_assert_retry_note
        self._next_assert_retry_note = ""
        safe_label = self.redact_for_log(f"{label}{retry_note}")
        if condition:
            self.passed += 1
            print(f"[PASS] {safe_label}")
            return

        self.failed += 1
        message = f"[FAIL] {safe_label}"
        if details:
            message += f" | {self.redact_for_log(details)}"
        self.failures.append(message)
        print(message)

    def parse_json(self, result: HttpResult, label: str) -> Any:
        try:
            return json.loads(result.text)
        except json.JSONDecodeError as exc:
            self.assert_true(
                False,
                label,
                f"Invalid JSON: {exc}; body={self.redact_for_log(result.text[:200])}",
            )
            return None

    def _mark_error_coverage(self, message: str) -> None:
        if "Invalid query parameters" in message:
            self.error_coverage["INVALID_QUERY_PARAMS"] = True
        elif "Invalid device" in message:
            self.error_coverage["INVALID_DEVICE"] = True
        elif "Invalid brightness" in message:
            self.error_coverage["INVALID_BRIGHTNESS"] = True
        elif "Invalid method" in message:
            self.error_coverage["INVALID_METHOD"] = True
        elif "Invalid theme" in message:
            self.error_coverage["INVALID_THEME"] = True
        elif "No available images for the selected filters" in message:
            self.error_coverage["NO_IMAGES_FOR_COMBINATION"] = True
        elif "No available images" in message:
            self.error_coverage["NO_AVAILABLE_IMAGES"] = True

    def _assert_error_json_payload(
        self, result: HttpResult, expected_status: int, label: str
    ) -> dict[str, Any] | None:
        self.assert_true(
            "application/json" in result.headers.get("content-type", ""),
            f"{label} (content-type)",
            result.headers.get("content-type", ""),
        )
        payload = self.parse_json(result, f"{label} (json parse)")
        if not isinstance(payload, dict):
            return None
        self.assert_true(
            payload.get("status") == expected_status,
            f"{label} (payload status)",
            str(payload),
        )
        message = payload.get("message")
        self.assert_true(
            isinstance(message, str) and bool(message.strip()),
            f"{label} (payload message)",
            str(payload),
        )
        if isinstance(message, str):
            self._mark_error_coverage(message)
        return payload

    def expect_json_error(
        self,
        path: str,
        query: dict[str, str],
        expected_status: int,
        expected_message_part: str,
        label: str,
        expected_detail_keys: list[str] | None = None,
        expected_field: str | None = None,
        expected_received: str | None = None,
        expect_allowed_list: bool = False,
    ) -> None:
        # 错误场景统一断言：状态码、JSON 基本结构与可选 details 字段。
        result = self.request(path, query=query, follow_redirects=True)
        self.assert_true(
            result.status == expected_status,
            label,
            f"status={result.status}, expected={expected_status}",
        )
        payload = self._assert_error_json_payload(result, expected_status, label)
        if not isinstance(payload, dict):
            return
        message = str(payload.get("message", ""))
        self.assert_true(
            expected_message_part in message, f"{label} (message)", f"message={message}"
        )

        if (
            expected_detail_keys is None
            and expected_field is None
            and expected_received is None
            and not expect_allowed_list
        ):
            return

        details = payload.get("details")
        self.assert_true(
            isinstance(details, dict), f"{label} (details object)", str(payload)
        )
        if not isinstance(details, dict):
            return

        if expected_detail_keys:
            for key in expected_detail_keys:
                self.assert_true(
                    key in details, f"{label} (details.{key})", str(details)
                )

        if expected_field is not None:
            self.assert_true(
                details.get("field") == expected_field,
                f"{label} (details.field)",
                str(details),
            )

        if expected_received is not None:
            self.assert_true(
                str(details.get("received")) == expected_received,
                f"{label} (details.received)",
                str(details),
            )

        if expect_allowed_list:
            allowed = details.get("allowed")
            self.assert_true(
                isinstance(allowed, list) and len(allowed) > 0,
                f"{label} (details.allowed)",
                str(details),
            )

    def expect_empty_status(
        self,
        path: str,
        query: dict[str, str] | None,
        expected_status: int,
        label: str,
        follow_redirects: bool = True,
    ) -> HttpResult:
        # 用于 302 等应返回空 body 的场景，返回结果供后续断言 header。
        result = self.request(path, query=query, follow_redirects=follow_redirects)
        self.assert_true(
            result.status == expected_status,
            label,
            f"status={result.status}, expected={expected_status}",
        )
        self.assert_true(
            len(result.body) == 0, f"{label} empty body", f"len={len(result.body)}"
        )
        return result

    def assert_redirect_asset_base(self, location: str, label: str) -> None:
        if not location:
            self.assert_true(False, label, "empty location")
            return
        self.assert_true(
            location.startswith(self.asset_base_url),
            label,
            f"expected_prefix={self.asset_base_url}, location={location}",
        )

    def run(self) -> int:
        print(f"CONFIG env value: {_mask_config_for_log(CONFIG_RAW)}")
        print(f"Testing API base URL: {_mask_url_for_log(self.api_base_url)}")
        print(
            f"Expect asset base URL: {_mask_url_for_log(self.asset_base_url)} (strict=True)"
        )
        print(f"Expect actual redirect behavior: {REDIRECT_ENABLED}")
        started = time.time()

        # 1) 隐藏统计路由：仅做边界校验（状态、类型、非负值）。
        count_resp = self.request(RANDOM_IMG_COUNT_PATH)
        self.assert_true(count_resp.status == 200, "GET hidden count route status")
        self.assert_true(
            "application/json" in count_resp.headers.get("content-type", ""),
            "GET hidden count route content-type",
            count_resp.headers.get("content-type", ""),
        )
        count_data = self.parse_json(count_resp, "GET hidden count route json")
        if not isinstance(count_data, dict):
            return 1

        required_keys = {"totalImages", "groupTotals", "themeTotals", "themeDetails"}
        self.assert_true(
            required_keys.issubset(set(count_data.keys())), "count json keys"
        )

        group_totals = count_data.get("groupTotals", {})
        theme_totals = count_data.get("themeTotals", {})
        theme_details = count_data.get("themeDetails", [])

        if isinstance(theme_totals, dict):
            self.register_theme_tokens([str(theme) for theme in theme_totals.keys()])

        self.assert_true(isinstance(group_totals, dict), "groupTotals is object")
        self.assert_true(isinstance(theme_totals, dict), "themeTotals is object")
        self.assert_true(isinstance(theme_details, list), "themeDetails is array")
        if (
            not isinstance(group_totals, dict)
            or not isinstance(theme_totals, dict)
            or not isinstance(theme_details, list)
        ):
            return 1

        try:
            total_images = int(count_data.get("totalImages", -1))
        except (TypeError, ValueError):
            total_images = -1
        self.assert_true(
            total_images >= 0,
            "totalImages is non-negative integer",
            str(count_data.get("totalImages")),
        )

        normalized_theme_details: list[dict[str, Any]] = []
        seen_detail_keys: set[tuple[str, str, str]] = set()

        group_non_int_count = 0
        group_negative_count = 0
        for value in group_totals.values():
            try:
                count = int(value)
            except (TypeError, ValueError):
                group_non_int_count += 1
                continue
            if count < 0:
                group_negative_count += 1

        theme_non_int_count = 0
        theme_negative_count = 0
        for key, value in theme_totals.items():
            self.register_theme_tokens([str(key)])
            try:
                count = int(value)
            except (TypeError, ValueError):
                theme_non_int_count += 1
                continue
            if count < 0:
                theme_negative_count += 1

        self.assert_true(
            group_non_int_count == 0,
            "groupTotals values are integers",
            f"invalid={group_non_int_count}",
        )
        self.assert_true(
            group_negative_count == 0,
            "groupTotals values are non-negative",
            f"invalid={group_negative_count}",
        )
        self.assert_true(
            theme_non_int_count == 0,
            "themeTotals values are integers",
            f"invalid={theme_non_int_count}",
        )
        self.assert_true(
            theme_negative_count == 0,
            "themeTotals values are non-negative",
            f"invalid={theme_negative_count}",
        )

        for idx, row in enumerate(theme_details):
            if not isinstance(row, dict):
                continue

            device = str(row.get("device", ""))
            brightness = str(row.get("brightness", ""))
            theme = str(row.get("theme", ""))
            raw_count = row.get("count", 0)

            if theme:
                self.register_theme_tokens([theme])

            try:
                count = int(raw_count)
            except (TypeError, ValueError):
                continue

            if (
                device not in SUPPORTED_DEVICES
                or brightness not in SUPPORTED_BRIGHTNESS
                or not theme
            ):
                continue
            if count < 0:
                continue

            combo_key = (device, brightness, theme)
            if combo_key in seen_detail_keys:
                continue
            seen_detail_keys.add(combo_key)

            normalized_theme_details.append(
                {
                    "device": device,
                    "brightness": brightness,
                    "theme": theme,
                    "count": count,
                }
            )

        if total_images > 0:
            self.assert_true(
                len(normalized_theme_details) > 0,
                "themeDetails has at least one valid row",
            )

        self.expect_json_error(
            RANDOM_IMG_COUNT_PATH,
            {"x": "1"},
            403,
            HIDDEN_ROUTE_QUERY_FORBIDDEN_MESSAGE_PART,
            "hidden count route query forbidden",
        )

        # 2) 错误参数覆盖（仅 /random-img）
        self.expect_json_error(
            "/random-img",
            {"x": "1"},
            400,
            "Invalid query parameters",
            "invalid query key",
            expected_detail_keys=["invalidParams", "allowedParams"],
        )
        self.expect_json_error(
            "/random-img",
            {"d": "bad-device"},
            400,
            "Invalid device",
            "invalid device",
            expected_field="d",
            expected_received="bad-device",
            expect_allowed_list=True,
        )
        self.expect_json_error(
            "/random-img",
            {"b": "bad-brightness"},
            400,
            "Invalid brightness",
            "invalid brightness",
            expected_field="b",
            expected_received="bad-brightness",
            expect_allowed_list=True,
        )
        self.expect_json_error(
            "/random-img",
            {"m": "bad-method"},
            400,
            "Invalid method",
            "invalid method",
            expected_field="m",
            expected_received="bad-method",
            expect_allowed_list=True,
        )
        self.expect_json_error(
            "/random-img",
            {"t": "__nonexistent_theme__"},
            400,
            "Invalid theme",
            "invalid theme",
            expected_field="t",
            expected_received="__nonexistent_theme__",
            expect_allowed_list=True,
        )
        self.expect_json_error(
            "/random-img",
            {"m": "ReDiReCt", "x": "1"},
            400,
            "Invalid query parameters",
            "invalid query key has higher priority than method logic",
            expected_detail_keys=["invalidParams", "allowedParams"],
        )

        self.expect_json_error(
            "/random-img",
            {"x": "1", "d": "pc", "m": "redirect"},
            400,
            "Invalid query parameters",
            "invalid query still blocks valid known params",
            expected_detail_keys=["invalidParams", "allowedParams"],
        )

        self.expect_json_error(
            "/random-img",
            {"d": "bad-device", "m": "bad-method"},
            400,
            "Invalid method",
            "invalid method has priority over device/brightness/theme",
            expected_field="m",
            expected_received="bad-method",
            expect_allowed_list=True,
        )

        strict_mixed_case_group = next(
            (
                (str(row["device"]), str(row["brightness"]))
                for row in normalized_theme_details
                if int(row["count"]) > 0
            ),
            None,
        )
        if strict_mixed_case_group is None:
            self.assert_true(False, "mixed-case strict group available")
            return 1

        mixed_case_device_raw, mixed_case_brightness_raw = strict_mixed_case_group
        mixed_case_device = "PC" if mixed_case_device_raw == "pc" else "Mb"
        mixed_case_brightness = (
            "LiGhT" if mixed_case_brightness_raw == "light" else "DaRk"
        )

        # 3) 大小写兼容（始终覆盖 proxy 与 redirect）。
        mixed_case_proxy = self.request(
            "/random-img",
            query={"d": mixed_case_device, "b": mixed_case_brightness, "m": "PrOxY"},
            follow_redirects=True,
        )
        self.assert_true(
            mixed_case_proxy.status == 200,
            "mixed-case device/brightness proxy status",
            f"status={mixed_case_proxy.status}",
        )

        mixed_case_method_redirect = self.request(
            "/random-img",
            query={"m": "ReDiReCt"},
            follow_redirects=False,
        )
        if REDIRECT_ENABLED:
            self.assert_true(
                mixed_case_method_redirect.status == 302,
                "mixed-case method redirect status",
                f"status={mixed_case_method_redirect.status}",
            )
        else:
            self.assert_true(
                mixed_case_method_redirect.status == 200,
                "mixed-case method redirect fallback-to-proxy status",
                f"status={mixed_case_method_redirect.status}",
            )

        mixed_case_device_brightness_redirect = self.request(
            "/random-img",
            query={"d": mixed_case_device, "b": mixed_case_brightness, "m": "ReDiReCt"},
            follow_redirects=False,
        )
        if REDIRECT_ENABLED:
            self.assert_true(
                mixed_case_device_brightness_redirect.status == 302,
                "mixed-case device/brightness redirect status",
                f"status={mixed_case_device_brightness_redirect.status}",
            )
        else:
            self.assert_true(
                mixed_case_device_brightness_redirect.status == 200,
                "mixed-case device/brightness redirect fallback-to-proxy status",
                f"status={mixed_case_device_brightness_redirect.status}",
            )

        # 4) 默认请求（proxy）
        default_img = self.request("/random-img")
        self.assert_true(
            default_img.status == 200,
            "GET /random-img default status",
            f"status={default_img.status}",
        )
        self.assert_true(
            "application/json" not in default_img.headers.get("content-type", ""),
            "GET /random-img default content-type not json",
            default_img.headers.get("content-type", ""),
        )
        self.assert_true(
            len(default_img.body) > 0, "GET /random-img default body non-empty"
        )

        # 5) 基于统计数据做组合覆盖
        nonzero_details = [
            row for row in normalized_theme_details if int(row["count"]) > 0
        ]
        zero_details = [
            row for row in normalized_theme_details if int(row["count"]) == 0
        ]

        self.assert_true(
            len(nonzero_details) > 0, "there is at least one nonzero combination"
        )

        for device in sorted(SUPPORTED_DEVICES):
            for brightness in sorted(SUPPORTED_BRIGHTNESS):
                group_key = f"{device}-{brightness}"
                group_count = int(group_totals.get(group_key, 0))
                if group_count > 0:
                    group_proxy = self.request(
                        "/random-img",
                        query={"d": device, "b": brightness, "m": "proxy"},
                        follow_redirects=True,
                    )
                    self.assert_true(
                        group_proxy.status == 200,
                        f"group {group_key} proxy status",
                        f"status={group_proxy.status}",
                    )

                    group_redirect = self.request(
                        "/random-img",
                        query={"d": device, "b": brightness, "m": "redirect"},
                        follow_redirects=False,
                    )
                    if REDIRECT_ENABLED:
                        self.assert_true(
                            group_redirect.status == 302,
                            f"group {group_key} redirect status",
                            f"status={group_redirect.status}",
                        )
                    else:
                        self.assert_true(
                            group_redirect.status == 200,
                            f"group {group_key} redirect fallback-to-proxy status",
                            f"status={group_redirect.status}",
                        )
                else:
                    self.expect_json_error(
                        "/random-img",
                        {"d": device, "b": brightness},
                        404,
                        "No available images for the selected filters",
                        f"group {group_key} has no images",
                    )

        # 6) 有效参数组合（严格断言成功状态）
        pc_has_images = (
            int(group_totals.get("pc-dark", 0)) + int(group_totals.get("pc-light", 0))
            > 0
        )
        mb_has_images = (
            int(group_totals.get("mb-dark", 0)) + int(group_totals.get("mb-light", 0))
            > 0
        )
        dark_has_images = (
            int(group_totals.get("pc-dark", 0)) + int(group_totals.get("mb-dark", 0))
            > 0
        )
        light_has_images = (
            int(group_totals.get("pc-light", 0)) + int(group_totals.get("mb-light", 0))
            > 0
        )

        valid_queries = [{"m": "proxy"}, {"d": "r"}]
        if pc_has_images:
            valid_queries.append({"d": "pc"})
        if mb_has_images:
            valid_queries.append({"d": "mb"})
        if dark_has_images:
            valid_queries.append({"b": "dark"})
        if light_has_images:
            valid_queries.append({"b": "light"})
        if int(group_totals.get("pc-dark", 0)) > 0:
            valid_queries.append({"d": "pc", "b": "dark"})
        if int(group_totals.get("mb-light", 0)) > 0:
            valid_queries.append({"d": "mb", "b": "light"})
        strict_random_brightness = "dark" if dark_has_images else "light"
        valid_queries.append({"d": "r", "b": strict_random_brightness})

        for idx, query in enumerate(valid_queries, start=1):
            result = self.request("/random-img", query=query, follow_redirects=True)
            self.assert_true(
                result.status == 200,
                f"valid query #{idx} status",
                f"query={query}, status={result.status}",
            )

        if REDIRECT_ENABLED:
            valid_redirect_queries = [{"m": "redirect"}]
            if pc_has_images:
                valid_redirect_queries.append({"d": "pc", "m": "redirect"})
            if mb_has_images:
                valid_redirect_queries.append({"d": "mb", "m": "redirect"})
            valid_redirect_queries.append(
                {"d": "r", "b": strict_random_brightness, "m": "redirect"}
            )
            for idx, query in enumerate(valid_redirect_queries, start=1):
                result = self.request(
                    "/random-img", query=query, follow_redirects=False
                )
                self.assert_true(
                    result.status == 302,
                    f"valid redirect query #{idx} status",
                    f"query={query}, status={result.status}",
                )
        else:
            redirect_as_proxy_queries = [{"m": "redirect"}]
            if pc_has_images:
                redirect_as_proxy_queries.append({"d": "pc", "m": "redirect"})
            if mb_has_images:
                redirect_as_proxy_queries.append({"d": "mb", "m": "redirect"})
            redirect_as_proxy_queries.append(
                {"d": "r", "b": strict_random_brightness, "m": "redirect"}
            )
            for idx, query in enumerate(redirect_as_proxy_queries, start=1):
                result = self.request(
                    "/random-img", query=query, follow_redirects=False
                )
                self.assert_true(
                    result.status == 200,
                    f"redirect-disabled query #{idx} fallback status",
                    f"query={query}, status={result.status}",
                )

        # 7) 多主题参数覆盖（使用统计结果中同组多个可用主题）
        themes_by_group: dict[tuple[str, str], list[str]] = {}
        for row in nonzero_details:
            device = str(row["device"])
            brightness = str(row["brightness"])
            theme = str(row["theme"])
            themes_by_group.setdefault((device, brightness), []).append(theme)

        multi_theme_group = next(
            (
                (d, b, sorted(set(themes)))
                for (d, b), themes in themes_by_group.items()
                if len(set(themes)) >= 2
            ),
            None,
        )

        if multi_theme_group:
            device, brightness, themes = multi_theme_group
            first_theme, second_theme = themes[0], themes[1]

            multi_csv_proxy = self.request(
                "/random-img",
                query={
                    "d": device,
                    "b": brightness,
                    "t": f"{first_theme},{second_theme}",
                    "m": "proxy",
                },
                follow_redirects=True,
            )
            self.assert_true(
                multi_csv_proxy.status == 200,
                "multi-theme csv proxy status",
                f"status={multi_csv_proxy.status}",
            )

            multi_csv_redirect = self.request(
                "/random-img",
                query={
                    "d": device,
                    "b": brightness,
                    "t": f"{first_theme},{second_theme}",
                    "m": "redirect",
                },
                follow_redirects=False,
            )
            if REDIRECT_ENABLED:
                self.assert_true(
                    multi_csv_redirect.status == 302,
                    "multi-theme csv redirect status",
                    f"status={multi_csv_redirect.status}",
                )
            else:
                self.assert_true(
                    multi_csv_redirect.status == 200,
                    "multi-theme csv redirect fallback-to-proxy status",
                    f"status={multi_csv_redirect.status}",
                )

            self.expect_json_error(
                "/random-img",
                {
                    "d": device,
                    "b": brightness,
                    "t": f"{first_theme},__nonexistent_theme__",
                },
                400,
                "Invalid theme",
                "multi-theme csv with invalid theme",
            )

            multi_repeat_proxy = self.request_query_items(
                "/random-img",
                query_items=[
                    ("d", device),
                    ("b", brightness),
                    ("t", first_theme),
                    ("t", second_theme),
                    ("m", "proxy"),
                ],
                follow_redirects=True,
            )
            self.assert_true(
                multi_repeat_proxy.status == 200,
                "multi-theme repeated-t proxy status",
                f"status={multi_repeat_proxy.status}",
            )

            multi_repeat_redirect = self.request_query_items(
                "/random-img",
                query_items=[
                    ("d", device),
                    ("b", brightness),
                    ("t", first_theme),
                    ("t", second_theme),
                    ("m", "redirect"),
                ],
                follow_redirects=False,
            )
            if REDIRECT_ENABLED:
                self.assert_true(
                    multi_repeat_redirect.status == 302,
                    "multi-theme repeated-t redirect status",
                    f"status={multi_repeat_redirect.status}",
                )
            else:
                self.assert_true(
                    multi_repeat_redirect.status == 200,
                    "multi-theme repeated-t redirect fallback-to-proxy status",
                    f"status={multi_repeat_redirect.status}",
                )

            multi_repeat_invalid = self.request_query_items(
                "/random-img",
                query_items=[
                    ("d", device),
                    ("b", brightness),
                    ("t", first_theme),
                    ("t", "__nonexistent_theme__"),
                    ("m", "proxy"),
                ],
                follow_redirects=True,
            )
            self.assert_true(
                multi_repeat_invalid.status == 400,
                "multi-theme repeated-t with invalid theme",
                f"status={multi_repeat_invalid.status}, expected=400",
            )
            repeat_invalid_payload = self._assert_error_json_payload(
                multi_repeat_invalid,
                400,
                "multi-theme repeated-t with invalid theme",
            )
            if isinstance(repeat_invalid_payload, dict):
                repeat_invalid_message = str(repeat_invalid_payload.get("message", ""))
                self.assert_true(
                    "Invalid theme" in repeat_invalid_message,
                    "multi-theme repeated-t invalid theme message",
                    f"message={repeat_invalid_message}",
                )
        else:
            print(
                "[SKIP] 不存在同 device+brightness 下至少 2 个可用主题，跳过多主题断言"
            )

        # 8) 每个有图组合至少测一次
        for row in nonzero_details:
            device = str(row["device"])
            brightness = str(row["brightness"])
            theme = str(row["theme"])
            label_prefix = f"combo {device}-{brightness}-{theme}"

            proxy_result = self.request(
                "/random-img",
                query={"d": device, "b": brightness, "t": theme, "m": "proxy"},
                follow_redirects=True,
            )
            self.assert_true(
                proxy_result.status == 200,
                f"{label_prefix} proxy status",
                f"status={proxy_result.status}",
            )

            redirect_result = self.request(
                "/random-img",
                query={
                    "d": device,
                    "b": brightness,
                    "t": theme,
                    "m": "redirect",
                },
                follow_redirects=False,
            )
            if REDIRECT_ENABLED:
                self.assert_true(
                    redirect_result.status == 302,
                    f"{label_prefix} redirect status",
                    f"status={redirect_result.status}",
                )
            else:
                self.assert_true(
                    redirect_result.status == 200,
                    f"{label_prefix} redirect fallback-to-proxy status",
                    f"status={redirect_result.status}",
                )

        if zero_details:
            row = zero_details[0]
            device = str(row["device"])
            brightness = str(row["brightness"])
            theme = str(row["theme"])
            self.expect_json_error(
                "/random-img",
                {"d": device, "b": brightness, "t": theme},
                404,
                "No available images for the selected filters",
                f"no images for combination {device}-{brightness}-{theme}",
            )

        # 9) 方法模式行为：始终测试 proxy 与 redirect（按开关断言 redirect 语义）。
        proxy_any = self.request(
            "/random-img", query={"m": "proxy"}, follow_redirects=True
        )
        self.assert_true(
            proxy_any.status == 200,
            "GET /random-img?m=proxy status",
            f"status={proxy_any.status}",
        )

        if REDIRECT_ENABLED:
            redirect_any = self.expect_empty_status(
                "/random-img",
                query={"m": "redirect"},
                expected_status=302,
                label="GET /random-img?m=redirect status",
                follow_redirects=False,
            )
            location = redirect_any.headers.get("location", "")
            self.assert_true(
                bool(location), "GET /random-img?m=redirect location present"
            )
            self.assert_redirect_asset_base(
                location, "GET /random-img?m=redirect asset base match"
            )
            self.assert_true(
                bool(re.search(REDIRECT_LOCATION_PATTERN, location)),
                "GET /random-img?m=redirect location format",
                location,
            )
        else:
            redirect_any = self.request(
                "/random-img", query={"m": "redirect"}, follow_redirects=False
            )
            self.assert_true(
                redirect_any.status == 200,
                "GET /random-img?m=redirect fallback proxy status",
                f"status={redirect_any.status}",
            )

        redirect_with_filters = self.request(
            "/random-img",
            query={"d": "r", "b": strict_random_brightness, "m": "redirect"},
            follow_redirects=False,
        )
        if REDIRECT_ENABLED:
            self.assert_true(
                redirect_with_filters.status == 302,
                "GET /random-img?d=r&b=<available>&m=redirect status",
                f"status={redirect_with_filters.status}",
            )
        else:
            self.assert_true(
                redirect_with_filters.status == 200,
                "GET /random-img?d=r&b=<available>&m=redirect fallback status",
                f"status={redirect_with_filters.status}",
            )

        # 10) 稳定性抽样：始终测试 proxy，redirect 按开关断言。
        for i in range(self.random_runs):
            proxy_sample = self.request(
                "/random-img", query={"m": "proxy"}, follow_redirects=True
            )
            self.assert_true(
                proxy_sample.status == 200,
                f"random stability proxy #{i + 1}",
                f"status={proxy_sample.status}",
            )

        for i in range(self.random_runs):
            redirect_sample = self.request(
                "/random-img", query={"m": "redirect"}, follow_redirects=False
            )
            if REDIRECT_ENABLED:
                self.assert_true(
                    redirect_sample.status == 302,
                    f"random stability redirect #{i + 1}",
                    f"status={redirect_sample.status}",
                )
            else:
                self.assert_true(
                    redirect_sample.status == 200,
                    f"random stability redirect-fallback #{i + 1}",
                    f"status={redirect_sample.status}",
                )

        missing_hard = sorted(
            k
            for k in REQUIRED_ERROR_COVERAGE_KEYS
            if not self.error_coverage.get(k, False)
        )
        missing_optional = sorted(
            k
            for k in OPTIONAL_ERROR_COVERAGE_KEYS
            if not self.error_coverage.get(k, False)
        )

        self.assert_true(
            len(missing_hard) == 0,
            "hard error coverage complete",
            f"missing={missing_hard}",
        )
        if missing_optional:
            print(
                f"[INFO] optional error coverage missing (data-dependent): {', '.join(missing_optional)}"
            )

        elapsed = time.time() - started
        print("\n========== 测试结果 ==========")
        print(f"Passed: {self.passed}")
        print(f"Failed: {self.failed}")
        print(f"Elapsed: {elapsed:.2f}s")

        if self.failures:
            print("\n失败详情：")
            for item in self.failures:
                print(item)
            return 1

        print("全部通过 ✅")
        return 0


def main() -> None:
    tester = ApiTester(
        api_base_url=API_BASE_URL,
        asset_base_url=ASSET_BASE_URL,
        timeout=TIMEOUT_SECONDS,
        random_runs=RANDOM_RUNS,
    )
    code = tester.run()
    raise SystemExit(code)


if __name__ == "__main__":
    main()
