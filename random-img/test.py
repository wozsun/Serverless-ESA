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
TIMEOUT_SECONDS = 20.0
# 稳定性测试中随机重定向抽样次数。
RANDOM_RUNS = 30
# 瞬时网络/读取失败时的最大重试次数。
MAX_REQUEST_ATTEMPTS = 5
# 线性退避基数（sleep = base * attempt）。
RETRY_BACKOFF_BASE_SECONDS = 1

# 从统计结果筛选测试组合时允许的设备维度。
SUPPORTED_DEVICES = {"pc", "mb"}
# 从统计结果筛选测试组合时允许的亮度维度。
SUPPORTED_BRIGHTNESS = {"dark", "light"}

# 一次完整测试中必须覆盖到的错误类型。
REQUIRED_ERROR_COVERAGE_KEYS = {
    "INVALID_QUERY_PARAMS",
    "INVALID_DEVICE",
    "INVALID_BRIGHTNESS",
    "INVALID_METHOD",
    "INVALID_THEME",
    "INVALID_COUNT_REQUEST",
    "NOT_FOUND",
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


def _config_bool(config: dict[str, Any], key: str, default: bool) -> bool:
    raw_value = config.get(key)
    if raw_value is None:
        return default
    if isinstance(raw_value, bool):
        return raw_value
    if isinstance(raw_value, (int, float)):
        return raw_value != 0
    if isinstance(raw_value, str):
        return raw_value.strip().lower() in {"1", "true", "yes", "on"}
    raise RuntimeError(f"Invalid boolean CONFIG field: {key}")


def _required_config_int(config: dict[str, Any], key: str) -> int:
    raw_value = config.get(key)
    if isinstance(raw_value, bool):
        raise RuntimeError(f"Invalid integer CONFIG field: {key}")
    if isinstance(raw_value, int):
        value = raw_value
    elif isinstance(raw_value, str) and raw_value.strip():
        try:
            value = int(raw_value.strip())
        except ValueError as exc:
            raise RuntimeError(f"Invalid integer CONFIG field: {key}") from exc
    else:
        raise RuntimeError(f"Missing or invalid CONFIG field: {key}")

    if value <= 0:
        raise RuntimeError(f"Invalid CONFIG field: {key} must be > 0")
    return value

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
ASSET_BASE_URL = _normalize_asset_base_url(_required_config_str(CONFIG, "ASSET_BASE_URL"))
RANDOM_IMG_COUNT_PATH = "/" + _required_config_str(CONFIG, "RANDOM_IMG_COUNT_PATH").strip("/")

IMAGE_FILENAME_DIGITS = _required_config_int(CONFIG, "IMAGE_FILENAME_DIGITS")
RANDOM_IMG_COUNT_INVALID_QUERY_MESSAGE_PART = f"{RANDOM_IMG_COUNT_PATH} only accepts exact path without query parameters"
ENABLE_REDIRECT_TESTS = _config_bool(CONFIG, "ENABLE_REDIRECT_TESTS", True)

# 重定向地址格式校验正则（基于 ASSET_BASE_URL 做完整 URL 校验）。
REDIRECT_LOCATION_PATTERN = rf"^{re.escape(ASSET_BASE_URL)}(pc|mb)-(dark|light)/[a-z0-9_-]+/\d{{{IMAGE_FILENAME_DIGITS}}}\.webp$"


def _mask_url_for_log(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return "<invalid-url>"
    return "<redacted-url>"


def _redact_urls_in_text(text: str) -> str:
    value = str(text)

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
    def __init__(self, api_base_url: str, asset_base_url: str, timeout: float, random_runs: int) -> None:
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
            "INVALID_COUNT_REQUEST": False,
            "NO_IMAGES_FOR_COMBINATION": False,
            "NO_AVAILABLE_IMAGES": False,
            "NOT_FOUND": False,
        }
        self.passed = 0
        self.failed = 0
        self.failures: list[str] = []

        self.normal_opener = urllib.request.build_opener()
        self.no_redirect_opener = urllib.request.build_opener(NoRedirectHandler())

    def _url(self, path: str, query: dict[str, str] | None = None) -> str:
        if not path.startswith("/"):
            path = "/" + path
        if not query:
            return f"{self.api_base_url}{path}"
        return f"{self.api_base_url}{path}?{urllib.parse.urlencode(query)}"

    def request(self, path: str, query: dict[str, str] | None = None, follow_redirects: bool = True) -> HttpResult:
        url = self._url(path, query)
        req = urllib.request.Request(url, method="GET")
        opener = self.normal_opener if follow_redirects else self.no_redirect_opener

        max_attempts = MAX_REQUEST_ATTEMPTS
        for attempt in range(1, max_attempts + 1):
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
                        elif attempt < max_attempts:
                            time.sleep(RETRY_BACKOFF_BASE_SECONDS * attempt)
                            continue
                        else:
                            raise
                    return HttpResult(
                        status=status,
                        headers=headers,
                        body=body,
                    )
            except urllib.error.HTTPError as exc:
                try:
                    error_body = exc.read()
                except http.client.IncompleteRead as read_exc:
                    error_body = bytes(read_exc.partial or b"")
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
                if attempt == max_attempts:
                    return HttpResult(
                        status=599,
                        headers={},
                        body=f"request failed after retries: {exc}".encode("utf-8", errors="replace"),
                    )
                time.sleep(RETRY_BACKOFF_BASE_SECONDS * attempt)

        return HttpResult(status=599, headers={}, body=b"request failed: unexpected retry flow")

    def assert_true(self, condition: bool, label: str, details: str = "") -> None:
        if condition:
            self.passed += 1
            print(f"[PASS] {label}")
            return

        self.failed += 1
        message = f"[FAIL] {label}"
        if details:
            message += f" | {_redact_urls_in_text(details)}"
        self.failures.append(message)
        print(message)

    def parse_json(self, result: HttpResult, label: str) -> Any:
        try:
            return json.loads(result.text)
        except json.JSONDecodeError as exc:
            self.assert_true(False, label, f"Invalid JSON: {exc}; body={_redact_urls_in_text(result.text[:200])}")
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
        elif RANDOM_IMG_COUNT_INVALID_QUERY_MESSAGE_PART in message:
            self.error_coverage["INVALID_COUNT_REQUEST"] = True
        elif "No available images for the selected filters" in message:
            self.error_coverage["NO_IMAGES_FOR_COMBINATION"] = True
        elif "No available images" in message:
            self.error_coverage["NO_AVAILABLE_IMAGES"] = True
        elif "API Not Found" in message:
            self.error_coverage["NOT_FOUND"] = True

    def _assert_error_json_payload(self, result: HttpResult, expected_status: int, label: str) -> dict[str, Any] | None:
        self.assert_true(
            "application/json" in result.headers.get("content-type", ""),
            f"{label} (content-type)",
            result.headers.get("content-type", ""),
        )
        payload = self.parse_json(result, f"{label} (json parse)")
        if not isinstance(payload, dict):
            return None
        self.assert_true(payload.get("status") == expected_status, f"{label} (payload status)", str(payload))
        message = payload.get("message")
        self.assert_true(isinstance(message, str) and bool(message.strip()), f"{label} (payload message)", str(payload))
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
        result = self.request(path, query=query, follow_redirects=True)
        self.assert_true(result.status == expected_status, label, f"status={result.status}, expected={expected_status}")
        payload = self._assert_error_json_payload(result, expected_status, label)
        if not isinstance(payload, dict):
            return
        message = str(payload.get("message", ""))
        self.assert_true(expected_message_part in message, f"{label} (message)", f"message={message}")

        if expected_detail_keys is None and expected_field is None and expected_received is None and not expect_allowed_list:
            return

        details = payload.get("details")
        self.assert_true(isinstance(details, dict), f"{label} (details object)", str(payload))
        if not isinstance(details, dict):
            return

        if expected_detail_keys:
            for key in expected_detail_keys:
                self.assert_true(key in details, f"{label} (details.{key})", str(details))

        if expected_field is not None:
            self.assert_true(details.get("field") == expected_field, f"{label} (details.field)", str(details))

        if expected_received is not None:
            self.assert_true(str(details.get("received")) == expected_received, f"{label} (details.received)", str(details))

        if expect_allowed_list:
            allowed = details.get("allowed")
            self.assert_true(isinstance(allowed, list) and len(allowed) > 0, f"{label} (details.allowed)", str(details))

    def expect_empty_status(self, path: str, query: dict[str, str] | None, expected_status: int, label: str) -> None:
        result = self.request(path, query=query, follow_redirects=True)
        self.assert_true(result.status == expected_status, label, f"status={result.status}, expected={expected_status}")
        self.assert_true(len(result.body) == 0, f"{label} empty body", f"len={len(result.body)}")

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
        print(f"Expect asset base URL: {_mask_url_for_log(self.asset_base_url)} (strict=True)")
        print(f"Redirect tests enabled: {ENABLE_REDIRECT_TESTS}")
        started = time.time()

        # 1) 统计接口 schema + URL 限制
        count_resp = self.request(RANDOM_IMG_COUNT_PATH)
        self.assert_true(count_resp.status == 200, f"GET {RANDOM_IMG_COUNT_PATH} status")
        self.assert_true(
            "application/json" in count_resp.headers.get("content-type", ""),
            f"GET {RANDOM_IMG_COUNT_PATH} content-type",
            count_resp.headers.get("content-type", ""),
        )
        count_data = self.parse_json(count_resp, f"GET {RANDOM_IMG_COUNT_PATH} json")
        if not isinstance(count_data, dict):
            return 1

        required_keys = {"totalImages", "groupTotals", "themeTotals", "themeDetails"}
        self.assert_true(required_keys.issubset(set(count_data.keys())), "count json keys")

        group_totals = count_data.get("groupTotals", {})
        theme_totals = count_data.get("themeTotals", {})
        theme_details = count_data.get("themeDetails", [])

        self.assert_true(isinstance(group_totals, dict), "groupTotals is object")
        self.assert_true(isinstance(theme_totals, dict), "themeTotals is object")
        self.assert_true(isinstance(theme_details, list), "themeDetails is array")

        if not isinstance(group_totals, dict) or not isinstance(theme_totals, dict) or not isinstance(theme_details, list):
            return 1

        sum_group = sum(int(v) for v in group_totals.values())
        sum_theme = sum(int(v) for v in theme_totals.values())
        self.assert_true(sum_group == int(count_data.get("totalImages", -1)), "totalImages == sum(groupTotals)")
        self.assert_true(sum_theme == int(count_data.get("totalImages", -1)), "totalImages == sum(themeTotals)")

        normalized_theme_details: list[dict[str, Any]] = []
        detail_group_totals: dict[str, int] = {}
        detail_theme_totals: dict[str, int] = {}
        seen_detail_keys: set[tuple[str, str, str]] = set()

        for idx, row in enumerate(theme_details):
            row_label = f"themeDetails[{idx}]"
            self.assert_true(isinstance(row, dict), f"{row_label} is object", str(row))
            if not isinstance(row, dict):
                continue

            device = str(row.get("device", ""))
            brightness = str(row.get("brightness", ""))
            theme = str(row.get("theme", ""))
            raw_count = row.get("count", 0)

            self.assert_true(device in SUPPORTED_DEVICES, f"{row_label}.device", str(row))
            self.assert_true(brightness in SUPPORTED_BRIGHTNESS, f"{row_label}.brightness", str(row))
            self.assert_true(bool(theme), f"{row_label}.theme", str(row))

            try:
                count = int(raw_count)
            except (TypeError, ValueError):
                self.assert_true(False, f"{row_label}.count is int", str(row))
                continue

            self.assert_true(count >= 0, f"{row_label}.count >= 0", str(row))
            if device not in SUPPORTED_DEVICES or brightness not in SUPPORTED_BRIGHTNESS or not theme:
                continue

            combo_key = (device, brightness, theme)
            self.assert_true(combo_key not in seen_detail_keys, f"{row_label} unique combo", str(combo_key))
            if combo_key in seen_detail_keys:
                continue
            seen_detail_keys.add(combo_key)

            normalized_row = {
                "device": device,
                "brightness": brightness,
                "theme": theme,
                "count": count,
            }
            normalized_theme_details.append(normalized_row)

            group_key = f"{device}-{brightness}"
            detail_group_totals[group_key] = detail_group_totals.get(group_key, 0) + count
            detail_theme_totals[theme] = detail_theme_totals.get(theme, 0) + count

        for group_key, total in group_totals.items():
            expected = detail_group_totals.get(str(group_key), 0)
            self.assert_true(
                int(total) == expected,
                f"groupTotals consistency {group_key}",
                f"groupTotals={total}, themeDetails={expected}",
            )

        extra_group_keys = sorted(set(detail_group_totals.keys()) - set(group_totals.keys()))
        self.assert_true(len(extra_group_keys) == 0, "themeDetails has no extra groups", str(extra_group_keys))

        for theme_key, total in theme_totals.items():
            expected = detail_theme_totals.get(str(theme_key), 0)
            self.assert_true(
                int(total) == expected,
                f"themeTotals consistency {theme_key}",
                f"themeTotals={total}, themeDetails={expected}",
            )

        extra_theme_keys = sorted(set(detail_theme_totals.keys()) - set(theme_totals.keys()))
        self.assert_true(len(extra_theme_keys) == 0, "themeDetails has no extra themes", str(extra_theme_keys))

        # 统计接口仅允许精确路径且无 query
        self.expect_json_error(
            RANDOM_IMG_COUNT_PATH,
            {"x": "1"},
            403,
            RANDOM_IMG_COUNT_INVALID_QUERY_MESSAGE_PART,
            f"GET {RANDOM_IMG_COUNT_PATH} with query forbidden",
        )

        count_trailing_path = f"{RANDOM_IMG_COUNT_PATH}/"
        count_trailing_slash = self.request(count_trailing_path)
        self.assert_true(
            count_trailing_slash.status == 404,
            f"GET {count_trailing_path} status (route not found)",
            f"status={count_trailing_slash.status}",
        )

        count_trailing_json = self._assert_error_json_payload(
            count_trailing_slash,
            404,
            f"GET {count_trailing_path} error payload",
        )
        if isinstance(count_trailing_json, dict):
            self.assert_true(
                "API Not Found" in str(count_trailing_json.get("message", "")),
                f"GET {count_trailing_path} message",
                str(count_trailing_json),
            )

        not_found_resp = self.request("/__definitely_not_found__")
        self.assert_true(not_found_resp.status == 404, "GET /__definitely_not_found__ status", f"status={not_found_resp.status}")
        not_found_payload = self._assert_error_json_payload(not_found_resp, 404, "GET /__definitely_not_found__ payload")
        if isinstance(not_found_payload, dict):
            self.assert_true(
                "API Not Found" in str(not_found_payload.get("message", "")),
                "GET /__definitely_not_found__ message",
                str(not_found_payload),
            )

        # 2) 错误参数覆盖
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

        # 2.1) 大小写/空白兼容（redirect 模式）
        if ENABLE_REDIRECT_TESTS:
            mixed_case_method = self.request("/random-img", query={"m": "ReDiReCt"}, follow_redirects=False)
            self.assert_true(mixed_case_method.status == 302, "mixed-case method redirect status", f"status={mixed_case_method.status}")

            mixed_case_device_brightness = self.request(
                "/random-img",
                query={"d": "PC", "b": "LiGhT", "m": "redirect"},
                follow_redirects=False,
            )
            self.assert_true(
                mixed_case_device_brightness.status in {302, 404},
                "mixed-case device/brightness status",
                f"status={mixed_case_device_brightness.status}",
            )
        else:
            print("[SKIP] 已关闭 redirect 测试，跳过 mixed-case redirect 断言")

        # 3) 默认请求（proxy）
        default_img = self.request("/random-img")
        self.assert_true(default_img.status == 200, "GET /random-img default status")
        self.assert_true(
            "application/json" not in default_img.headers.get("content-type", ""),
            "GET /random-img default content-type not json",
            default_img.headers.get("content-type", ""),
        )
        self.assert_true(len(default_img.body) > 0, "GET /random-img default body non-empty")

        # 4) 重定向模式
        if ENABLE_REDIRECT_TESTS:
            redirect_any = self.request("/random-img", query={"m": "redirect"}, follow_redirects=False)
            self.assert_true(redirect_any.status == 302, "GET /random-img?m=redirect status")
            location = redirect_any.headers.get("location", "")
            self.assert_true(bool(location), "GET /random-img?m=redirect location present")
            self.assert_true(len(redirect_any.body) == 0, "GET /random-img?m=redirect empty body", f"len={len(redirect_any.body)}")
            self.assert_redirect_asset_base(location, "GET /random-img?m=redirect asset base match")
            self.assert_true(
                bool(re.search(REDIRECT_LOCATION_PATTERN, location)),
                "GET /random-img?m=redirect location format",
                location,
            )
        else:
            print("[SKIP] 已关闭 redirect 测试，跳过 m=redirect 行为断言")

        # 5) 基于统计数据做组合覆盖
        nonzero_details = [
            row for row in normalized_theme_details
            if int(row["count"]) > 0
        ]
        zero_details = [
            row for row in normalized_theme_details
            if int(row["count"]) == 0
        ]

        self.assert_true(len(nonzero_details) > 0, "存在可用组合（count>0）")

        # 基于 groupTotals，校验仅传 device+brightness 时的行为与统计结果一致
        for device in sorted(SUPPORTED_DEVICES):
            for brightness in sorted(SUPPORTED_BRIGHTNESS):
                group_key = f"{device}-{brightness}"
                group_count = int(group_totals.get(group_key, 0))
                if group_count > 0:
                    if ENABLE_REDIRECT_TESTS:
                        group_redirect = self.request(
                            "/random-img",
                            query={"d": device, "b": brightness, "m": "redirect"},
                            follow_redirects=False,
                        )
                        self.assert_true(
                            group_redirect.status == 302,
                            f"group {group_key} redirect status",
                            f"status={group_redirect.status}",
                        )
                        group_location = group_redirect.headers.get("location", "")
                        self.assert_true(
                            f"/{group_key}/" in group_location,
                            f"group {group_key} redirect location",
                            group_location,
                        )
                    else:
                        group_proxy = self.request(
                            "/random-img",
                            query={"d": device, "b": brightness},
                            follow_redirects=True,
                        )
                        self.assert_true(
                            group_proxy.status == 200,
                            f"group {group_key} proxy status",
                            f"status={group_proxy.status}",
                        )
                        self.assert_true(
                            len(group_proxy.body) > 0,
                            f"group {group_key} proxy body non-empty",
                            f"len={len(group_proxy.body)}",
                        )
                else:
                    self.expect_json_error(
                        "/random-img",
                        {"d": device, "b": brightness},
                        404,
                        "No available images for the selected filters",
                        f"group {group_key} has no images",
                    )

        # 6.1) 多主题参数覆盖（t=fddm,wlop 与 t=fddm&t=wlop）
        themes_by_group: dict[tuple[str, str], list[str]] = {}
        for row in nonzero_details:
            d = str(row["device"])
            b = str(row["brightness"])
            t = str(row["theme"])
            themes_by_group.setdefault((d, b), []).append(t)

        multi_theme_group = next(
            (
                (d, b, sorted(set(themes)))
                for (d, b), themes in themes_by_group.items()
                if len(set(themes)) >= 2
            ),
            None,
        )

        if multi_theme_group:
            d, b, themes = multi_theme_group
            t1, t2 = themes[0], themes[1]

            if ENABLE_REDIRECT_TESTS:
                multi_csv = self.request(
                    "/random-img",
                    query={"d": d, "b": b, "t": f"{t1},{t2}", "m": "redirect"},
                    follow_redirects=False,
                )
                self.assert_true(multi_csv.status == 302, "multi-theme csv redirect status", f"status={multi_csv.status}")
                multi_csv_loc = multi_csv.headers.get("location", "")
                self.assert_true(
                    f"/{d}-{b}/{t1}/" in multi_csv_loc or f"/{d}-{b}/{t2}/" in multi_csv_loc,
                    "multi-theme csv picks one requested theme",
                    multi_csv_loc,
                )

                repeated_query = urllib.parse.urlencode(
                    [("d", d), ("b", b), ("t", t1), ("t", t2), ("m", "redirect")]
                )
                multi_repeat = self.request(
                    f"/random-img?{repeated_query}",
                    follow_redirects=False,
                )
                self.assert_true(
                    multi_repeat.status == 302,
                    "multi-theme repeated param redirect status",
                    f"status={multi_repeat.status}",
                )
                multi_repeat_loc = multi_repeat.headers.get("location", "")
                self.assert_true(
                    f"/{d}-{b}/{t1}/" in multi_repeat_loc or f"/{d}-{b}/{t2}/" in multi_repeat_loc,
                    "multi-theme repeated param picks one requested theme",
                    multi_repeat_loc,
                )
            else:
                multi_csv = self.request(
                    "/random-img",
                    query={"d": d, "b": b, "t": f"{t1},{t2}"},
                    follow_redirects=True,
                )
                self.assert_true(multi_csv.status == 200, "multi-theme csv proxy status", f"status={multi_csv.status}")
                self.assert_true(len(multi_csv.body) > 0, "multi-theme csv proxy body non-empty")

                repeated_query = urllib.parse.urlencode(
                    [("d", d), ("b", b), ("t", t1), ("t", t2)]
                )
                multi_repeat = self.request(
                    f"/random-img?{repeated_query}",
                    follow_redirects=True,
                )
                self.assert_true(
                    multi_repeat.status == 200,
                    "multi-theme repeated param proxy status",
                    f"status={multi_repeat.status}",
                )
                self.assert_true(len(multi_repeat.body) > 0, "multi-theme repeated param proxy body non-empty")

            self.expect_json_error(
                "/random-img",
                {"d": d, "b": b, "t": f"{t1},__nonexistent_theme__"},
                400,
                "Invalid theme",
                "multi-theme csv with invalid theme",
            )

            repeated_invalid_query = urllib.parse.urlencode(
                [("d", d), ("b", b), ("t", t1), ("t", "__nonexistent_theme__")]
            )
            repeated_invalid = self.request(f"/random-img?{repeated_invalid_query}", follow_redirects=True)
            self.assert_true(
                repeated_invalid.status == 400,
                "multi-theme repeated param with invalid theme status",
                f"status={repeated_invalid.status}",
            )
            repeated_invalid_payload = self.parse_json(repeated_invalid, "multi-theme repeated invalid json")
            if isinstance(repeated_invalid_payload, dict):
                self.assert_true(
                    "Invalid theme" in str(repeated_invalid_payload.get("message", "")),
                    "multi-theme repeated param with invalid theme message",
                    str(repeated_invalid_payload),
                )

            # 去重：同一个主题重复传入不应报错
            repeated_same_theme_params: list[tuple[str, str]] = [("d", d), ("b", b), ("t", t1), ("t", t1)]
            if ENABLE_REDIRECT_TESTS:
                repeated_same_theme_params.append(("m", "redirect"))
            repeated_same_theme = urllib.parse.urlencode(repeated_same_theme_params)
            repeated_same = self.request(
                f"/random-img?{repeated_same_theme}",
                follow_redirects=not ENABLE_REDIRECT_TESTS,
            )
            self.assert_true(
                repeated_same.status == (302 if ENABLE_REDIRECT_TESTS else 200),
                "repeated same theme still works",
                f"status={repeated_same.status}",
            )

            # 去空白：包含空白与空 token 的 t 仍可正确处理
            theme_with_spaces_query: dict[str, str] = {"d": d, "b": b, "t": f" {t1} , , {t2} "}
            if ENABLE_REDIRECT_TESTS:
                theme_with_spaces_query["m"] = "redirect"
            theme_with_spaces = self.request(
                "/random-img",
                query=theme_with_spaces_query,
                follow_redirects=not ENABLE_REDIRECT_TESTS,
            )
            self.assert_true(
                theme_with_spaces.status == (302 if ENABLE_REDIRECT_TESTS else 200),
                "theme csv with spaces/empty tokens",
                f"status={theme_with_spaces.status}",
            )
        else:
            print("[SKIP] 不存在同 device+brightness 下至少 2 个可用主题，跳过多主题断言")

        # 每个有图组合都测一次 proxy（可选附加 redirect）
        for row in nonzero_details:
            d, b, t = str(row["device"]), str(row["brightness"]), str(row["theme"])
            label_prefix = f"combo {d}-{b}-{t}"

            if ENABLE_REDIRECT_TESTS:
                rr = self.request("/random-img", query={"d": d, "b": b, "t": t, "m": "redirect"}, follow_redirects=False)
                self.assert_true(rr.status == 302, f"{label_prefix} redirect status", f"status={rr.status}")
                loc = rr.headers.get("location", "")
                self.assert_redirect_asset_base(loc, f"{label_prefix} redirect asset base")
                self.assert_true(f"/{d}-{b}/{t}/" in loc, f"{label_prefix} redirect location match", loc)

            rp = self.request("/random-img", query={"d": d, "b": b, "t": t, "m": "proxy"}, follow_redirects=True)
            self.assert_true(rp.status == 200, f"{label_prefix} proxy status", f"status={rp.status}")
            self.assert_true(len(rp.body) > 0, f"{label_prefix} proxy body non-empty")

        # 选择一个无图组合，验证组合无图错误
        if zero_details:
            row = zero_details[0]
            d, b, t = str(row["device"]), str(row["brightness"]), str(row["theme"])
            self.expect_json_error(
                "/random-img",
                {"d": d, "b": b, "t": t},
                404,
                "No available images for the selected filters",
                f"no images for combination {d}-{b}-{t}",
            )

        # 选择一个“只在另一个亮度存在”的主题，验证当前亮度下参数合法但无图（404）
        theme_by_device_brightness: dict[tuple[str, str], set[str]] = {}
        for row in nonzero_details:
            d = str(row["device"])
            b = str(row["brightness"])
            t = str(row["theme"])
            theme_by_device_brightness.setdefault((d, b), set()).add(t)

        strict_theme_case: tuple[str, str, str] | None = None
        for d in SUPPORTED_DEVICES:
            dark_set = theme_by_device_brightness.get((d, "dark"), set())
            light_set = theme_by_device_brightness.get((d, "light"), set())
            dark_only = sorted(dark_set - light_set)
            light_only = sorted(light_set - dark_set)
            if dark_only:
                strict_theme_case = (d, "light", dark_only[0])
                break
            if light_only:
                strict_theme_case = (d, "dark", light_only[0])
                break

        if strict_theme_case:
            d, b, t = strict_theme_case
            strict_resp = self.request("/random-img", query={"d": d, "b": b, "t": t}, follow_redirects=True)
            self.assert_true(
                strict_resp.status == 404,
                f"theme constrained by brightness {d}-{b}-{t}",
                f"status={strict_resp.status}, expected=404",
            )
            strict_payload = self.parse_json(strict_resp, f"theme constrained by brightness {d}-{b}-{t} json")
            if isinstance(strict_payload, dict):
                strict_message = str(strict_payload.get("message", ""))
                self.assert_true(
                    "No available images for the selected filters" in strict_message,
                    f"theme constrained by brightness {d}-{b}-{t} (message)",
                    strict_message,
                )
        else:
            print("[SKIP] 没找到 brightness 维度可区分的主题，跳过 strict theme 断言")

        # 选择一个“只在另一个设备存在”的主题，验证跨设备也判为合法参数，并返回无图（404）
        device_theme_map: dict[str, set[str]] = {device: set() for device in SUPPORTED_DEVICES}
        for row in nonzero_details:
            device = str(row["device"])
            theme = str(row["theme"])
            if device in device_theme_map:
                device_theme_map[device].add(theme)

        cross_device_case: tuple[str, str] | None = None
        pc_only = sorted(device_theme_map["pc"] - device_theme_map["mb"])
        mb_only = sorted(device_theme_map["mb"] - device_theme_map["pc"])
        if pc_only:
            cross_device_case = ("mb", pc_only[0])
        elif mb_only:
            cross_device_case = ("pc", mb_only[0])

        if cross_device_case:
            d, t = cross_device_case
            cross_resp = self.request("/random-img", query={"d": d, "t": t}, follow_redirects=True)
            self.assert_true(
                cross_resp.status == 404,
                f"theme constrained by device {d}-{t}",
                f"status={cross_resp.status}, expected=404",
            )
            cross_payload = self.parse_json(cross_resp, f"theme constrained by device {d}-{t} json")
            if isinstance(cross_payload, dict):
                cross_message = str(cross_payload.get("message", ""))
                self.assert_true(
                    "No available images for the selected filters" in cross_message,
                    f"theme constrained by device {d}-{t} (message)",
                    cross_message,
                )
        else:
            print("[SKIP] 没找到 device 维度可区分的主题，跳过 cross-device theme 断言")

        # 6) 随机设备模式覆盖
        dark_themes = sorted({str(row["theme"]) for row in nonzero_details if str(row["brightness"]) == "dark"})
        if dark_themes:
            sample_theme = dark_themes[0]
            if ENABLE_REDIRECT_TESTS:
                rr = self.request(
                    "/random-img",
                    query={"d": "r", "b": "dark", "t": sample_theme, "m": "redirect"},
                    follow_redirects=False,
                )
                self.assert_true(rr.status == 302, "random device with dark+theme status", f"status={rr.status}")
                if rr.status == 302:
                    loc = rr.headers.get("location", "")
                    self.assert_redirect_asset_base(loc, "random device redirect asset base")
                    self.assert_true(
                        bool(re.search(rf"^{re.escape(self.asset_base_url)}(pc|mb)-dark/{re.escape(sample_theme)}/\d{{{IMAGE_FILENAME_DIGITS}}}\.webp$", loc)),
                        "random device redirect location matches requested dark+theme",
                        loc,
                    )
            else:
                rr = self.request(
                    "/random-img",
                    query={"d": "r", "b": "dark", "t": sample_theme},
                    follow_redirects=True,
                )
                self.assert_true(rr.status == 200, "random device with dark+theme proxy status", f"status={rr.status}")
                self.assert_true(len(rr.body) > 0, "random device with dark+theme proxy body non-empty")
        else:
            print("[SKIP] dark 亮度下没有可用主题，跳过随机设备 dark+theme 断言")

        # 7) 稳定性抽样
        if ENABLE_REDIRECT_TESTS:
            for i in range(self.random_runs):
                r = self.request("/random-img", query={"m": "redirect"}, follow_redirects=False)
                self.assert_true(r.status == 302, f"random stability redirect #{i + 1}", f"status={r.status}")
        else:
            print("[SKIP] 已关闭 redirect 测试，跳过 redirect 稳定性抽样")

        # 8) 全量无图场景（条件触发）
        if int(count_data.get("totalImages", 0)) == 0:
            self.expect_json_error(
                "/random-img",
                {},
                404,
                "No available images",
                "no available images global",
                expected_detail_keys=["hint"],
            )
        else:
            print("[SKIP] totalImages > 0，跳过全量无图断言")

        missing_hard = sorted(k for k in REQUIRED_ERROR_COVERAGE_KEYS if not self.error_coverage.get(k, False))
        missing_optional = sorted(k for k in OPTIONAL_ERROR_COVERAGE_KEYS if not self.error_coverage.get(k, False))

        self.assert_true(len(missing_hard) == 0, "hard error coverage complete", f"missing={missing_hard}")
        if missing_optional:
            print(f"[INFO] optional error coverage missing (data-dependent): {', '.join(missing_optional)}")

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
