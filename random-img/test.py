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


BASE_URL = _required_env("BASE_URL")
ASSET_BASE_URL = _required_env("ASSET_BASE_URL")
TIMEOUT_SECONDS = 20.0
RANDOM_RUNS = 20


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
    def __init__(self, base_url: str, asset_base_url: str, timeout: float, random_runs: int) -> None:
        self.base_url = base_url.rstrip("/")
        self.asset_base_url = asset_base_url.rstrip("/") + "/"
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
            return f"{self.base_url}{path}"
        return f"{self.base_url}{path}?{urllib.parse.urlencode(query)}"

    def request(self, path: str, query: dict[str, str] | None = None, follow_redirects: bool = True) -> HttpResult:
        url = self._url(path, query)
        req = urllib.request.Request(url, method="GET")
        opener = self.normal_opener if follow_redirects else self.no_redirect_opener

        max_attempts = 3
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
                            time.sleep(0.2 * attempt)
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
                time.sleep(0.2 * attempt)

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
        print(f"Testing base URL: {_mask_url_for_log(self.base_url)}")
        print(f"Expect asset base URL: {_mask_url_for_log(self.asset_base_url)} (strict=True)")
        started = time.time()

        # 1) 统计接口 schema + URL 限制
        count_resp = self.request("/random-img-count")
        self.assert_true(count_resp.status == 200, "GET /random-img-count status")
        self.assert_true(
            "application/json" in count_resp.headers.get("content-type", ""),
            "GET /random-img-count content-type",
            count_resp.headers.get("content-type", ""),
        )
        count_data = self.parse_json(count_resp, "GET /random-img-count json")
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

        # /random-img-count 仅允许精确路径且无 query
        self.expect_empty_status(
            "/random-img-count",
            {"x": "1"},
            403,
            "GET /random-img-count with query forbidden",
        )

        count_trailing_slash = self.request("/random-img-count/")
        self.assert_true(
            count_trailing_slash.status == 404,
            "GET /random-img-count/ status (route not found)",
            f"status={count_trailing_slash.status}",
        )

        count_trailing_json = self._assert_error_json_payload(
            count_trailing_slash,
            404,
            "GET /random-img-count/ error payload",
        )
        if isinstance(count_trailing_json, dict):
            self.assert_true(
                "API Not Found" in str(count_trailing_json.get("message", "")),
                "GET /random-img-count/ message",
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

        # 2.1) 大小写/空白兼容
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
        redirect_any = self.request("/random-img", query={"m": "redirect"}, follow_redirects=False)
        self.assert_true(redirect_any.status == 302, "GET /random-img?m=redirect status")
        location = redirect_any.headers.get("location", "")
        self.assert_true(bool(location), "GET /random-img?m=redirect location present")
        self.assert_true(len(redirect_any.body) == 0, "GET /random-img?m=redirect empty body", f"len={len(redirect_any.body)}")
        self.assert_redirect_asset_base(location, "GET /random-img?m=redirect asset base match")
        self.assert_true(
            bool(re.search(r"/random-img/(pc|mb)-(dark|light)/[a-z0-9_-]+/\d{6}\.webp$", location)),
            "GET /random-img?m=redirect location format",
            location,
        )

        # 5) 基于统计数据做组合覆盖
        nonzero_details = [
            row for row in theme_details
            if isinstance(row, dict)
            and int(row.get("count", 0)) > 0
            and row.get("device") in {"pc", "mb"}
            and row.get("brightness") in {"dark", "light"}
            and isinstance(row.get("theme"), str)
        ]
        zero_details = [
            row for row in theme_details
            if isinstance(row, dict)
            and int(row.get("count", 0)) == 0
            and row.get("device") in {"pc", "mb"}
            and row.get("brightness") in {"dark", "light"}
            and isinstance(row.get("theme"), str)
        ]

        self.assert_true(len(nonzero_details) > 0, "存在可用组合（count>0）")

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
            repeated_same_theme = urllib.parse.urlencode(
                [("d", d), ("b", b), ("t", t1), ("t", t1), ("m", "redirect")]
            )
            repeated_same = self.request(f"/random-img?{repeated_same_theme}", follow_redirects=False)
            self.assert_true(
                repeated_same.status == 302,
                "repeated same theme still works",
                f"status={repeated_same.status}",
            )

            # 去空白：包含空白与空 token 的 t 仍可正确处理
            theme_with_spaces = self.request(
                "/random-img",
                query={"d": d, "b": b, "t": f" {t1} , , {t2} ", "m": "redirect"},
                follow_redirects=False,
            )
            self.assert_true(
                theme_with_spaces.status == 302,
                "theme csv with spaces/empty tokens",
                f"status={theme_with_spaces.status}",
            )
        else:
            print("[SKIP] 不存在同 device+brightness 下至少 2 个可用主题，跳过多主题断言")

        # 每个有图组合都测一次 redirect + proxy
        for row in nonzero_details:
            d, b, t = str(row["device"]), str(row["brightness"]), str(row["theme"])
            label_prefix = f"combo {d}-{b}-{t}"

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
        for d in {"pc", "mb"}:
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
        device_theme_map: dict[str, set[str]] = {"pc": set(), "mb": set()}
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
        valid_themes = sorted(str(k) for k, v in theme_totals.items() if int(v) > 0)
        if valid_themes:
            sample_theme = valid_themes[0]
            rr = self.request(
                "/random-img",
                query={"d": "r", "b": "dark", "t": sample_theme, "m": "redirect"},
                follow_redirects=False,
            )
            if rr.status == 302:
                loc = rr.headers.get("location", "")
                self.assert_redirect_asset_base(loc, "random device redirect asset base")
                self.assert_true("/random-img/" in loc, "random device redirect location", loc)
            else:
                payload = self.parse_json(rr, "random device fallback json")
                self.assert_true(
                    rr.status in {404, 400},
                    "random device status acceptable",
                    f"status={rr.status}, body={payload}",
                )

        # 7) 稳定性抽样
        for i in range(self.random_runs):
            r = self.request("/random-img", query={"m": "redirect"}, follow_redirects=False)
            self.assert_true(r.status == 302, f"random stability redirect #{i + 1}", f"status={r.status}")

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

        hard_required_error_keys = {
            "INVALID_QUERY_PARAMS",
            "INVALID_DEVICE",
            "INVALID_BRIGHTNESS",
            "INVALID_METHOD",
            "INVALID_THEME",
            "NOT_FOUND",
        }
        optional_error_keys = {"NO_IMAGES_FOR_COMBINATION", "NO_AVAILABLE_IMAGES"}

        missing_hard = sorted(k for k in hard_required_error_keys if not self.error_coverage.get(k, False))
        missing_optional = sorted(k for k in optional_error_keys if not self.error_coverage.get(k, False))

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
        base_url=BASE_URL,
        asset_base_url=ASSET_BASE_URL,
        timeout=TIMEOUT_SECONDS,
        random_runs=RANDOM_RUNS,
    )
    code = tester.run()
    raise SystemExit(code)


if __name__ == "__main__":
    main()
