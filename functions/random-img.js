import { getKvJsonObjectCached, getKvUrlCached } from "../commons/kv.js";
import { jsonErrorResponse, jsonSuccessResponse } from "../commons/response.js";
import { validateRefererAccess } from "../commons/referer.js";

// ===========================
// 随机图片 API 配置
// ===========================

const RANDOM_IMG_CONFIG_NAMESPACE = "random-img-config";
const FOLDER_MAP_KEY = "FOLDER_MAP";
const BASE_IMAGE_URL_KEY = "BASE_IMAGE_URL";

const ALLOWED_PARAMS = ["d", "b", "t", "m"];
const MAP_DEVICES = ["pc", "mb"];
const REQUEST_DEVICES = [...MAP_DEVICES, "r"];
const BRIGHTNESS_VALUES = ["dark", "light"];
const METHOD_VALUES = ["proxy", "redirect"];

const IMAGE_FILENAME_DIGITS = 6;
const FETCH_MAX_ATTEMPTS = 3;
const FETCH_RETRY_DELAY_MS = 100;
const REFERER_CHECK_ENABLED = false;
const ALLOW_EMPTY_REFERER = true;
const REDIRECT_ENABLED = true;

const ALLOWED_PARAMS_SET = new Set(ALLOWED_PARAMS);
const REQUEST_DEVICE_SET = new Set(REQUEST_DEVICES);
const BRIGHTNESS_SET = new Set(BRIGHTNESS_VALUES);
const METHOD_SET = new Set(METHOD_VALUES);


// ===========================
// 随机图片 API 错误定义
// ===========================
const RANDOM_IMG_ERRORS = {
	INVALID_QUERY_PARAMS: { status: 400, message: "Bad Request: Invalid query parameters" },
	INVALID_DEVICE: { status: 400, message: "Bad Request: Invalid device" },
	INVALID_BRIGHTNESS: { status: 400, message: "Bad Request: Invalid brightness" },
	INVALID_THEME: { status: 400, message: "Bad Request: Invalid theme" },
	THEME_CONFLICT: { status: 400, message: "Bad Request: Cannot mix include and exclude theme values" },
	INVALID_METHOD: { status: 400, message: "Bad Request: Invalid method" },
	BASE_IMAGE_URL_CONFIG_ERROR: { status: 500, message: "Internal Server Error: BASE_IMAGE_URL is invalid or missing in KV" },
	FOLDER_MAP_CONFIG_ERROR: { status: 500, message: "Internal Server Error: FOLDER_MAP is invalid or missing in KV" },
	NO_IMAGES_FOR_COMBINATION: { status: 404, message: "Not Found: No available images for the selected filters" },
	NO_AVAILABLE_IMAGES: { status: 404, message: "Not Found: No available images" },
	REDIRECT_RESPONSE_EXCEPTION: { status: 502, message: "Bad Gateway: Failed to construct redirect response" },
	UPSTREAM_BAD_STATUS: { status: 502, message: "Bad Gateway: Upstream image service responded with a non-success status" },
	UPSTREAM_FETCH_EXCEPTION: { status: 502, message: "Bad Gateway: Failed to reach upstream image service due to network/runtime exception" },
};

const FOLDER_MAP_CONFIG_ERROR_DETAILS = {
	configKey: FOLDER_MAP_KEY,
	namespace: RANDOM_IMG_CONFIG_NAMESPACE,
	hint: "Ensure FOLDER_MAP exists in KV and contains a valid JSON object",
};

const BASE_IMAGE_URL_CONFIG_ERROR_DETAILS = {
	configKey: BASE_IMAGE_URL_KEY,
	namespace: RANDOM_IMG_CONFIG_NAMESPACE,
	hint: "Ensure BASE_IMAGE_URL exists in KV and is a non-empty URL string",
};

// 基于字段名、收到的值与允许值构造统一的参数校验错误响应。
const buildInvalidFieldResponse = (error, field, received, allowed) =>
	jsonErrorResponse(error, { field, received, allowed });

// 检查查询参数是否都在白名单内，若存在非法参数则直接返回 400 错误响应。
const validateAllowedQueryParams = (params, allowedParams) => {
	// 遍历请求中出现的每个查询参数键。
	for (const key of params.keys()) {
		// 若当前参数不在允许集合中，则立即返回错误。
		if (!allowedParams.has(key)) {
			return jsonErrorResponse(RANDOM_IMG_ERRORS.INVALID_QUERY_PARAMS, {
				invalidParams: [key],
				allowedParams: ALLOWED_PARAMS,
			});
		}
	}
	// 若不存在非法参数，则返回 null 表示通过校验。
	return null;
};

let validThemeCache = {
	themes: null,
	themeSet: null,
	sourceRef: null,
};

// 从 FOLDER_MAP 汇总“全局有效主题”列表：
// 1) 仅遍历固定设备范围（pc/mb）；2) 拉平亮度层后的主题键；3) 通过 Set 去重。
const buildValidThemes = (folderMap) =>
	Array.from(
		new Set(
			// 按设备展开，再按 brightness 展开，最终收集每个主题名。
			MAP_DEVICES.flatMap((device) =>
				Object.values(folderMap[device] ?? {}).flatMap((brightnessMap) =>
					Object.keys(brightnessMap ?? {})
				)
			)
		)
	);

// 惰性更新有效主题缓存：仅当 folderMap 引用变化时重新计算。
const ensureValidThemeCache = (folderMap) => {
	if (validThemeCache.themes && validThemeCache.sourceRef === folderMap) {
		return validThemeCache;
	}

	const themes = buildValidThemes(folderMap);
	validThemeCache = {
		themes,
		themeSet: new Set(themes),
		sourceRef: folderMap,
	};

	return validThemeCache;
};

// 读取并校验 FOLDER_MAP 配置。
const getFolderMapFromKV = async () => {
	return getKvJsonObjectCached({
		namespace: RANDOM_IMG_CONFIG_NAMESPACE,
		key: FOLDER_MAP_KEY,
		cacheKey: "random-img::folder-map",
	});
};

// 按全局开关决定是否执行 Referer 校验，关闭时直接放行。
const validateRefererByConfig = async (request) => {
	if (!REFERER_CHECK_ENABLED) {
		return { allowed: true, response: null };
	}

	return validateRefererAccess({
		namespace: RANDOM_IMG_CONFIG_NAMESPACE,
		referer: request.headers.get("referer") || "",
		allowEmptyReferer: ALLOW_EMPTY_REFERER,
	});
};

// 根据所选文件夹组合随机生成一个图片 URL。
const buildImageUrl = (baseImageUrl, selectedFolder) => {
	const imageNumber = Math.floor(Math.random() * selectedFolder.count) + 1;
	const imageFilename = `${String(imageNumber).padStart(IMAGE_FILENAME_DIGITS, "0")}.webp`;
	return `${baseImageUrl}${selectedFolder.device}-${selectedFolder.brightness}/${selectedFolder.theme}/${imageFilename}`;
};

// 按指定 method 响应图片：redirect 直接跳转，proxy 拉取上游后转发（失败时按次数重试）。
const respondImageByMethod = async (method, imageUrl) => {
	if (method === "redirect") {
		try {
			return new Response(null, {
				status: 302,
				headers: { Location: imageUrl },
			});
		} catch (error) {
			return jsonErrorResponse(RANDOM_IMG_ERRORS.REDIRECT_RESPONSE_EXCEPTION, {
				hint: "Redirect target is invalid for Location header",
				errorName: error instanceof Error ? error.name : "unknown",
			});
		}
	}

	for (let attempt = 1; attempt <= FETCH_MAX_ATTEMPTS; attempt++) {
		try {
			const upstreamResponse = await fetch(imageUrl);

			if (!upstreamResponse.ok) {
				return jsonErrorResponse(RANDOM_IMG_ERRORS.UPSTREAM_BAD_STATUS, {
					upstreamStatus: upstreamResponse.status,
					upstreamStatusText: upstreamResponse.statusText || undefined,
					hint: "Upstream responded but did not return a success status",
				});
			}

			return new Response(upstreamResponse.body, {
				status: upstreamResponse.status,
				headers: upstreamResponse.headers,
			});
		} catch {
			if (attempt >= FETCH_MAX_ATTEMPTS) {
				return jsonErrorResponse(RANDOM_IMG_ERRORS.UPSTREAM_FETCH_EXCEPTION, {
					hint: "Upstream request failed before receiving a valid response",
					retryAttempts: attempt,
				});
			}
			await new Promise((resolve) => setTimeout(resolve, FETCH_RETRY_DELAY_MS * attempt));
		}
	}

	return jsonErrorResponse(RANDOM_IMG_ERRORS.UPSTREAM_FETCH_EXCEPTION, {
		hint: "Upstream request failed before receiving a valid response",
		retryAttempts: FETCH_MAX_ATTEMPTS,
	});
};

// ===========================
// 随机图片主处理逻辑
// ===========================
export const handleRandomImg = async (request) => {
	// 处理随机图片请求：参数校验 -> 候选组合筛选 -> 加权抽样 -> redirect/proxy 返回。
	if (request.method !== "GET" && request.method !== "HEAD") {
		return jsonErrorResponse({ status: 405, message: "Method Not Allowed" });
	}

	const refererCheckResult = await validateRefererByConfig(request);
	if (!refererCheckResult.allowed) {
		return refererCheckResult.response;
	}

	// 解析请求 URL 以获取路径与查询参数。
	let params;
	try {
		params = new URL(request.url).searchParams;
	} catch {
		return jsonErrorResponse({
			status: 400,
			message: "Bad Request: Request URL is malformed or cannot be parsed",
		}, {
			hint: "Ensure the request URL is valid and properly encoded",
		});
	}

	// 第一步：优先校验链接参数合法性，再做后续处理
	// 执行参数白名单校验，返回值为 null 或错误响应对象。
	const invalidParamsResponse = validateAllowedQueryParams(params, ALLOWED_PARAMS_SET);
	// 若存在非法参数，直接返回错误响应并中止流程。
	if (invalidParamsResponse) {
		return invalidParamsResponse;
	}

	// 读取 method 参数，缺省时默认使用 proxy。
	const method = params.get("m")?.toLowerCase() || "proxy";

	// 校验 method 参数：仅允许 proxy 或 redirect
	// 判断 method 是否在允许集合内。
	if (!METHOD_SET.has(method)) {
		return buildInvalidFieldResponse(RANDOM_IMG_ERRORS.INVALID_METHOD, "m", method, METHOD_VALUES);
	}

	// 强制开关：若关闭重定向，则无论参数如何都用 proxy
	const effectiveMethod = REDIRECT_ENABLED ? method : "proxy";

	// 读取请求指定的设备参数（若未传则为 null）。
	const requestedDevice = params.get("d")?.toLowerCase() || null;
	// 若传入了设备参数，则校验其是否属于请求允许集合。
	if (requestedDevice && !REQUEST_DEVICE_SET.has(requestedDevice)) {
		return buildInvalidFieldResponse(RANDOM_IMG_ERRORS.INVALID_DEVICE, "d", requestedDevice, REQUEST_DEVICES);
	}

	// 设备选择逻辑：优先使用请求参数，其次根据 User-Agent 判断移动/桌面，最后退回默认设备 "r"（random）。
	let autoDevice = "r";
	if (!requestedDevice) {
		const userAgent = request.headers.get("User-Agent") || "";
		const isMobile = /Mobi|Android|iPhone/i.test(userAgent);
		const isDesktop = /Windows|Macintosh|Linux x86_64|X11/i.test(userAgent);
		autoDevice = isMobile ? "mb" : (isDesktop ? "pc" : "r");
	}
	const device = requestedDevice || autoDevice;
	// 读取亮度参数（若未传则为 null）。
	const requestedBrightness = params.get("b")?.toLowerCase() || null;
	// 若传入亮度参数，则校验其合法性。
	if (requestedBrightness && !BRIGHTNESS_SET.has(requestedBrightness)) {
		return buildInvalidFieldResponse(RANDOM_IMG_ERRORS.INVALID_BRIGHTNESS, "b", requestedBrightness, BRIGHTNESS_VALUES);
	}

	// 读取并归一化 theme 参数：支持多次传参与逗号分隔，最终去重。
	// 以 ! 为前缀的值表示排除该主题，不带前缀为包含，两者不可混用。
	const rawThemeValues = Array.from(new Set(params
		.getAll("t")
		.flatMap((value) => value.split(","))
		.map((value) => value.trim().toLowerCase())
		.filter(Boolean)));

	const themeIncludes = rawThemeValues.filter((v) => !v.startsWith("!"));
	const themeExcludes = rawThemeValues.filter((v) => v.startsWith("!")).map((v) => v.slice(1)).filter(Boolean);

	// 包含与排除不可混用。
	if (themeIncludes.length > 0 && themeExcludes.length > 0) {
		return jsonErrorResponse(RANDOM_IMG_ERRORS.THEME_CONFLICT, {
			includeThemes: themeIncludes,
			excludeThemes: themeExcludes,
			hint: "Use either include themes (e.g. t=nature) or exclude themes (e.g. t=!nature), not both",
		});
	}

	// 处理 device 参数
	const deviceCandidates =
		device === "r"
			? MAP_DEVICES
			: [device];

	// 处理 brightness 参数
	// 若指定亮度则只用该值，否则使用全部亮度候选。
	const brightnessCandidates = requestedBrightness ? [requestedBrightness] : BRIGHTNESS_VALUES;

	// 并行读取 FOLDER_MAP 与 BASE_IMAGE_URL 配置（两者互不依赖）。
	const [folderMap, baseImageUrl] = await Promise.all([
		getFolderMapFromKV(),
		getKvUrlCached({
			namespace: RANDOM_IMG_CONFIG_NAMESPACE,
			key: BASE_IMAGE_URL_KEY,
			cacheKey: "random-img::base-image-url",
		}),
	]);
	// 若配置异常则返回统一配置错误响应。
	if (!folderMap) {
		return jsonErrorResponse(RANDOM_IMG_ERRORS.FOLDER_MAP_CONFIG_ERROR, FOLDER_MAP_CONFIG_ERROR_DETAILS);
	}
	if (!baseImageUrl) {
		return jsonErrorResponse(RANDOM_IMG_ERRORS.BASE_IMAGE_URL_CONFIG_ERROR, BASE_IMAGE_URL_CONFIG_ERROR_DETAILS);
	}

	// 处理 theme 参数：统一校验所有提及的主题名是否在配置中存在。
	const themeCache = ensureValidThemeCache(folderMap);
	const allMentionedThemes = [...themeIncludes, ...themeExcludes];
	if (allMentionedThemes.length > 0) {
		const invalidTheme = allMentionedThemes.find((t) => !themeCache.themeSet.has(t));
		if (invalidTheme) {
			const displayValue = themeExcludes.includes(invalidTheme) ? `!${invalidTheme}` : invalidTheme;
			return buildInvalidFieldResponse(RANDOM_IMG_ERRORS.INVALID_THEME, "t", displayValue);
		}
	}

	let themeCandidates;
	if (themeIncludes.length > 0) {
		themeCandidates = themeIncludes;
	} else if (themeExcludes.length > 0) {
		const excludeSet = new Set(themeExcludes);
		themeCandidates = themeCache.themes.filter((t) => !excludeSet.has(t));
	} else {
		// 未传 t 时，才构建并使用全量主题候选。
		themeCandidates = themeCache.themes;
	}

	// 初始化候选组合列表，用于后续加权随机抽样。
	const candidates = [];
	// 遍历设备候选集合。
	for (const candidateDevice of deviceCandidates) {
		// 读取当前设备下的配置映射。
		const deviceMap = folderMap[candidateDevice] ?? {};
		// 遍历亮度候选集合。
		for (const b of brightnessCandidates) {
			// 遍历主题候选集合。
			for (const t of themeCandidates) {
				// 读取当前组合的图片数量并归一化为数值，缺省按 0 处理。
				const count = Number(deviceMap?.[b]?.[t] ?? 0);
				// 仅将有限且大于 0 的组合纳入候选池。
				if (Number.isFinite(count) && count > 0) {
					candidates.push({ device: candidateDevice, brightness: b, theme: t, count });
				}
			}
		}
	}

	// 若候选池为空，则根据是否传过滤条件返回不同的 404 错误。
	if (candidates.length === 0) {
		// 指定了亮度或主题（含排除）但无结果时，返回组合无图错误并回显过滤条件。
		if (requestedBrightness || themeIncludes.length > 0 || themeExcludes.length > 0) {
			return jsonErrorResponse(RANDOM_IMG_ERRORS.NO_IMAGES_FOR_COMBINATION, {
				filters: {
					device,
					brightness: requestedBrightness,
					themes: themeCandidates,
					excludedThemes: themeExcludes.length > 0 ? themeExcludes : undefined,
				},
			});
		}
		// 未指定过滤条件且仍无可用图时，返回通用无图错误。
		return jsonErrorResponse(RANDOM_IMG_ERRORS.NO_AVAILABLE_IMAGES, {
			hint: "Check FOLDER_MAP counts in KV to ensure at least one image count is greater than 0",
		});
	}

	let selectedFolder;
	if (candidates.length === 1) {
		selectedFolder = candidates[0];
	} else {
		// 加权抽样：按 count 作为权重选择候选组合，保证“每张图”更接近等概率
		// 计算候选池总权重（各组合 count 之和）。
		const totalWeight = candidates.reduce((sum, candidate) => sum + candidate.count, 0);
		// 权重异常兜底，避免 totalWeight 非法导致随机逻辑出错。
		if (!Number.isFinite(totalWeight) || totalWeight <= 0) {
			return jsonErrorResponse(RANDOM_IMG_ERRORS.NO_AVAILABLE_IMAGES, {
				hint: "No valid weighted candidates available",
			});
		}
		// 在 [0, totalWeight) 区间生成随机权重点，减少边界判断出错风险。
		let remainingWeight = Math.random() * totalWeight;
		// 命中结果初始化为 null，循环后再统一兜底。
		selectedFolder = null;
		// 线性递减权重，首次小于 0 时即命中当前候选项。
		for (const candidate of candidates) {
			remainingWeight -= candidate.count;
			if (remainingWeight < 0) {
				selectedFolder = candidate;
				break;
			}
		}
		// 浮点边界兜底：理论上不会触发，触发时选最后一个候选项。
		if (!selectedFolder) {
			selectedFolder = candidates[candidates.length - 1];
		}
	}

	return await respondImageByMethod(effectiveMethod, buildImageUrl(baseImageUrl, selectedFolder));
};

// 汇总 FOLDER_MAP 中的图片数量：按设备-亮度组合分组、按主题聚合、并计算总数。
const buildRandomImgCountData = (folderMap) => {
	const groupTotals = {};
	const themeDetails = {};
	let totalImages = 0;
	for (const device of Object.keys(folderMap).sort()) {
		const deviceEntry = folderMap[device];
		if (!deviceEntry || typeof deviceEntry !== "object") {
			continue;
		}
		for (const brightness of Object.keys(deviceEntry).sort()) {
			const brightnessEntry = deviceEntry[brightness];
			if (!brightnessEntry || typeof brightnessEntry !== "object") {
				continue;
			}
			const groupKey = `${device}-${brightness}`;
			let groupTotal = 0;
			for (const theme of Object.keys(brightnessEntry).sort()) {
				const count = Number(brightnessEntry[theme] ?? 0);
				groupTotal += count;
				totalImages += count;
				if (!themeDetails[theme]) {
					themeDetails[theme] = { total: 0 };
				}
				themeDetails[theme].total += count;
				themeDetails[theme][groupKey] = count;
			}
			groupTotals[groupKey] = groupTotal;
		}
	}
	return {
		totalImages,
		groupTotals,
		themeDetails,
	};
};

// 处理图片数量统计请求：读取 FOLDER_MAP 并返回汇总统计数据。
export const handleRandomImgCount = async () => {
	const folderMap = await getFolderMapFromKV();
	if (!folderMap) {
		return jsonErrorResponse(RANDOM_IMG_ERRORS.FOLDER_MAP_CONFIG_ERROR, FOLDER_MAP_CONFIG_ERROR_DETAILS);
	}
	return jsonSuccessResponse(buildRandomImgCountData(folderMap));
};
