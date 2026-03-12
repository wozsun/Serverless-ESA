import * as randomImgHandlers from "../random-img/function.js";
import { getKvTextCached } from "./kv.js";
import { detailedErrorResponse, jsonErrorResponse, jsonSuccessResponse } from "./response.js";

// ===========================
// 可配置参数（优先编辑此区域）
// ===========================
const PATH_CONFIG_NAMESPACE = "path-config";
// 隐藏路由入口注册：新增隐藏路由时仅需在此追加 KV key 字符串。
const HIDDEN_PATH_KEYS = ["RANDOM_IMG_COUNT_PATH"];

// 普通路由入口注册：
// - 固定 handler: 直接传函数
// - 业务模块: 传模块导出对象，按 handleXxx 自动匹配
const ROUTES = {
	"/": async () => jsonErrorResponse({ status: 404, message: "No API route specified" }),
	"/hello": async () => jsonSuccessResponse({ message: "Hello, World!" }),
	"/healthcheck": async () => jsonSuccessResponse({ message: "API on EdgeFunction is healthy" }),
	"/random-img": randomImgHandlers,
};

const routeHandlerCache = new Map();
let hiddenHandlerMapPromise = null;
let hiddenHandlerValidationLogged = false;

const toPascalCase = (value) =>
	value
		.split(/[-_]/)
		.filter(Boolean)
		.map((part) => part.charAt(0).toUpperCase() + part.slice(1))
		.join("");

const toHandlerNameFromRoutePath = (routePath) => {
	const normalizedPath = routePath.replace(/^\/+/, "");
	return `handle${toPascalCase(normalizedPath)}`;
};

// 约定：KV key `XXX_PATH` 对应 handler `handleXxx`。
const toHiddenHandlerName = (kvPathKey) =>
	`handle${toPascalCase(kvPathKey.replace(/_PATH$/, "").toLowerCase())}`;

const getRegisteredRouteModules = () =>
	Array.from(new Set(Object.values(ROUTES).filter((value) => value && typeof value === "object")));

const resolveRouteHandler = async (pathname) => {
	if (routeHandlerCache.has(pathname)) {
		return routeHandlerCache.get(pathname);
	}

	const routeEntry = ROUTES[pathname];
	if (!routeEntry) {
		return null;
	}

	if (typeof routeEntry === "function") {
		routeHandlerCache.set(pathname, routeEntry);
		return routeEntry;
	}

	if (!routeEntry || typeof routeEntry !== "object") {
		return null;
	}

	const handlerName = toHandlerNameFromRoutePath(pathname);
	const handler = routeEntry[handlerName];
	if (typeof handler === "function") {
		routeHandlerCache.set(pathname, handler);
		return handler;
	}

	return null;
};

const resolveHiddenHandler = async (kvPathKey) => {
	if (!hiddenHandlerMapPromise) {
		hiddenHandlerMapPromise = (async () => {
			const map = new Map();
			const unresolvedPathKeys = [];
			for (const pathKey of HIDDEN_PATH_KEYS) {
				const handlerName = toHiddenHandlerName(pathKey);
				let resolved = false;
				for (const moduleExports of getRegisteredRouteModules()) {
					const handler = moduleExports[handlerName];
					if (typeof handler === "function") {
						map.set(pathKey, handler);
						resolved = true;
						break;
					}
				}

				if (!resolved) {
					unresolvedPathKeys.push(pathKey);
				}
			}

			if (unresolvedPathKeys.length > 0 && !hiddenHandlerValidationLogged) {
				hiddenHandlerValidationLogged = true;
				console.warn(
					"Hidden route handler mapping missing for keys:",
					unresolvedPathKeys.join(", ")
				);
			}

			return map;
		})();
	}

	const hiddenHandlerMap = await hiddenHandlerMapPromise;
	if (hiddenHandlerMap.has(kvPathKey)) {
		return hiddenHandlerMap.get(kvPathKey);
	}

	return null;
};

// 命中隐藏路径时返回对应响应，未命中返回 null。
const resolveHiddenPathRoute = async (url, request) => {
	const { pathname, search } = url;

	for (const pathKey of HIDDEN_PATH_KEYS) {
		const dynamicPath = await getKvTextCached({
			namespace: PATH_CONFIG_NAMESPACE,
			key: pathKey,
			cacheKey: `path-config::${pathKey}`,
		});

		if (dynamicPath && pathname === dynamicPath) {
			if (search) {
				return detailedErrorResponse({ status: 403, message: "Forbidden: Routes do not accept query parameters" }, {
					hint: "Call hidden routes with exact path and no query string",
				});
			}

			const handler = await resolveHiddenHandler(pathKey);
			if (handler) {
				return await handler(request);
			}

			return detailedErrorResponse({ status: 500, message: "Internal Server Error: Route handler is not configured" }, {
				hint: "Check hidden route key to handler naming convention",
			});
		}
	}

	return null;
};

// ===========================
// 边缘函数入口
// ===========================
export default {
	// 边缘函数主入口：按 pathname 分发路由并兜底处理未捕获异常。
	async fetch(request) {
		try {
			const url = new URL(request.url);
			const { pathname } = url;
			const handler = await resolveRouteHandler(pathname);

			if (handler) {
				return await handler(request);
			}

			const hiddenPathResponse = await resolveHiddenPathRoute(url, request);
			if (hiddenPathResponse) {
				return hiddenPathResponse;
			}

			return jsonErrorResponse({ status: 404, message: "API Not Found" });
		} catch (error) {
			// 捕获未预期的错误，避免函数崩溃
			console.error("Unhandled error in edge function:", error instanceof Error ? error.message : "unknown");
			return jsonErrorResponse({ status: 500, message: "Internal Server Error" });
		}
	},
};
