import { detailedErrorResponse, jsonSuccessResponse } from "../main/response.js";

// ===========================
// 随机图片 API 配置
// ===========================
const ALLOWED_PARAMS = ["d", "b", "t", "m"];
const MAP_DEVICES = ["pc", "mb"];
const REQUEST_DEVICES = [...MAP_DEVICES, "r"];
const BRIGHTNESS_VALUES = ["dark", "light"];
const METHOD_VALUES = ["proxy", "redirect"];

const FORCE_REDIRECT_ENABLED = true;

const ALLOWED_PARAMS_SET = new Set(ALLOWED_PARAMS);
const REQUEST_DEVICE_SET = new Set(REQUEST_DEVICES);
const BRIGHTNESS_SET = new Set(BRIGHTNESS_VALUES);
const METHOD_SET = new Set(METHOD_VALUES);

const BASE_IMAGE_URL_KEY = "BASE_IMAGE_URL";
const FOLDER_MAP_KEY = "FOLDER_MAP";
const KV_NAMESPACE = "random-img-config";
const KV_CACHE_TTL_MS = 60 * 1000;

const RANDOM_IMG_COUNT_PATH_KEY = "RANDOM_IMG_COUNT_PATH";

// 懒初始化并复用同一个 EdgeKV 客户端：仅在真正读取 KV 时创建。
let edgeKVClient = null;
const getEdgeKVClient = () => {
	if (!edgeKVClient) {
		edgeKVClient = new EdgeKV({ namespace: KV_NAMESPACE });
	}
	return edgeKVClient;
};

// ===========================
// 随机图片 API 错误定义
// ===========================
const RANDOM_IMG_ERRORS = {
	INVALID_QUERY_PARAMS: { status: 400, message: "Bad Request: Invalid query parameters" },
	INVALID_DEVICE: { status: 400, message: "Bad Request: Invalid device" },
	INVALID_BRIGHTNESS: { status: 400, message: "Bad Request: Invalid brightness" },
	INVALID_THEME: { status: 400, message: "Bad Request: Invalid theme" },
	INVALID_METHOD: { status: 400, message: "Bad Request: Invalid method" },
	INVALID_COUNT_REQUEST: { status: 403, message: "Forbidden: Image count path only accepts exact path without query parameters" },
	BASE_IMAGE_URL_CONFIG_ERROR: { status: 500, message: "Internal Server Error: BASE_IMAGE_URL is invalid or missing in KV" },
	FOLDER_MAP_CONFIG_ERROR: { status: 500, message: "Internal Server Error: FOLDER_MAP is invalid or missing in KV" },
	NO_IMAGES_FOR_COMBINATION: { status: 404, message: "Not Found: No available images for the selected filters" },
	NO_AVAILABLE_IMAGES: { status: 404, message: "Not Found: No available images" },
	UPSTREAM_BAD_STATUS: { status: 502, message: "Bad Gateway: Upstream image service responded with a non-success status" },
	UPSTREAM_FETCH_EXCEPTION: { status: 502, message: "Bad Gateway: Failed to reach upstream image service due to network/runtime exception" },
};

const FOLDER_MAP_CONFIG_ERROR_DETAILS = {
	configKey: FOLDER_MAP_KEY,
	namespace: KV_NAMESPACE,
	hint: "Ensure FOLDER_MAP exists in KV and contains a valid JSON object",
};

const BASE_IMAGE_URL_CONFIG_ERROR_DETAILS = {
	configKey: BASE_IMAGE_URL_KEY,
	namespace: KV_NAMESPACE,
	hint: "Ensure BASE_IMAGE_URL exists in KV and is a non-empty URL string",
};

// 基于字段名、收到的值与允许值构造统一的参数校验错误响应。
const invalidFieldError = (error, field, received, allowed) =>
	detailedErrorResponse(error, { field, received, allowed });

// 检查查询参数是否都在白名单内，若存在非法参数则直接返回 400 错误响应。
const validateAllowedQueryParams = (params, allowedParams) => {
	// 遍历请求中出现的每个查询参数键。
	for (const key of params.keys()) {
		// 若当前参数不在允许集合中，则立即返回错误。
		if (!allowedParams.has(key)) {
			return detailedErrorResponse(RANDOM_IMG_ERRORS.INVALID_QUERY_PARAMS, {
				invalidParams: [key],
				allowedParams: ALLOWED_PARAMS,
			});
		}
	}
	// 若不存在非法参数，则返回 null 表示通过校验。
	return null;
};

// 进程内缓存：仅在当前边缘实例复用，跨实例/冷启动不共享
let folderMapCache = {
	value: null,
	expiresAt: 0,
};
let baseImageUrlCache = {
	value: null,
	expiresAt: 0,
};
let validThemeCache = {
	themes: null,
	themeSet: null,
	expiresAt: 0,
};

// 从 KV 读取文本值并做 trim 归一化，读取失败或非字符串时返回 null。
const getKvText = async (key) => {
	try {
		// 以文本模式读取指定 KV 键的值。
		const value = await getEdgeKVClient().get(key, { type: "text" });
		// 若读取结果不是字符串，则按无效值处理。
		if (typeof value !== "string") {
			return null;
		}
		// 去除字符串首尾空白以统一后续解析行为。
		const trimmed = value.trim();
		// 若去空白后为空字符串，则返回 null。
		return trimmed || null;
	} catch {
		// 读取异常统一降级为 null，避免抛出运行时错误。
		return null;
	}
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

const ensureValidThemeCache = (folderMap) => {
	const now = Date.now();
	if (validThemeCache.themes && now < validThemeCache.expiresAt) {
		return validThemeCache;
	}

	const themes = buildValidThemes(folderMap);
	validThemeCache = {
		themes,
		themeSet: new Set(themes),
		expiresAt: now + KV_CACHE_TTL_MS,
	};

	return validThemeCache;
};

const getValidThemeSet = (folderMap) => {
	return ensureValidThemeCache(folderMap).themeSet;
};

const getValidThemes = (folderMap) => {
	return ensureValidThemeCache(folderMap).themes;
};

// 读取并校验 FOLDER_MAP，优先命中内存缓存并返回统一的 { folderMap, errorKey } 结构。
const getFolderMapFromKV = async () => {
	// 命中 TTL 时直接复用，减少 KV 读取与 JSON 解析成本
	// 判断当前实例缓存是否仍在有效期。
	if (folderMapCache.value && Date.now() < folderMapCache.expiresAt) {
		return { folderMap: folderMapCache.value, errorKey: null };
	}

	// 从 KV 拉取 FOLDER_MAP 原始文本。
	const rawValue = await getKvText(FOLDER_MAP_KEY);
	// 若 KV 未配置或读取为空，返回统一配置错误键。
	if (!rawValue) {
		return { folderMap: null, errorKey: "FOLDER_MAP_CONFIG_ERROR" };
	}

	// 声明解析结果变量，便于 try/catch 后统一使用。
	let parsed;
	try {
		// 解析 JSON 文本为对象结构。
		parsed = JSON.parse(rawValue);
	} catch {
		// JSON 解析失败时返回配置错误键。
		return { folderMap: null, errorKey: "FOLDER_MAP_CONFIG_ERROR" };
	}
	// 轻量类型守卫：仅接受非数组对象，避免后续访问时报运行时错误。
	if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
		return { folderMap: null, errorKey: "FOLDER_MAP_CONFIG_ERROR" };
	}

	// 将合法配置写入本地缓存并刷新过期时间。
	const expiresAt = Date.now() + KV_CACHE_TTL_MS;
	folderMapCache = {
		value: parsed,
		expiresAt,
	};
	validThemeCache = { themes: null, themeSet: null, expiresAt: 0 };
	// 缓存写回：后续同实例请求在 TTL 内可直接命中

	return { folderMap: parsed, errorKey: null };
};

// 读取 BASE_IMAGE_URL 并规范成以 `/` 结尾的可拼接基础地址（含本地缓存）。
const getBaseImageUrlFromKV = async () => {
	// 若基础 URL 本地缓存仍有效，则直接复用。
	if (baseImageUrlCache.value && Date.now() < baseImageUrlCache.expiresAt) {
		return baseImageUrlCache.value;
	}

	// 从 KV 读取基础 URL 文本。
	const normalizedUrl = await getKvText(BASE_IMAGE_URL_KEY);
	// 若为空则表示配置缺失或无效。
	if (!normalizedUrl) {
		return null;
	}

	// 确保 URL 末尾包含 `/` 便于后续路径拼接。
	const normalizedWithSlash = normalizedUrl.endsWith("/") ? normalizedUrl : `${normalizedUrl}/`;
	// 写入本地缓存并设置过期时间。
	baseImageUrlCache = {
		value: normalizedWithSlash,
		expiresAt: Date.now() + KV_CACHE_TTL_MS,
	};

	return normalizedWithSlash;
};

const buildImageUrl = (baseImageUrl, selectedFolder) => {
	const imageNumber = Math.floor(Math.random() * selectedFolder.count) + 1;
	const imageFilename = `${String(imageNumber).padStart(5, "0")}.webp`;
	return `${baseImageUrl}${selectedFolder.device}-${selectedFolder.brightness}/${selectedFolder.theme}/${imageFilename}`;
};

const respondImageByMethod = async (method, imageUrl) => {
	if (method === "redirect") {
		return new Response(null, {
			status: 302,
			headers: { Location: imageUrl },
		});
	}

	try {
		const upstreamResponse = await fetch(imageUrl);

		if (!upstreamResponse.ok) {
			return detailedErrorResponse(RANDOM_IMG_ERRORS.UPSTREAM_BAD_STATUS, {
				upstreamStatus: upstreamResponse.status,
				upstreamStatusText: upstreamResponse.statusText || undefined,
				hint: "Upstream responded but did not return a success status",
			});
		}

		const headers = new Headers(upstreamResponse.headers);
		if (!headers.has("Cache-Control")) {
			headers.set("Cache-Control", "public, max-age=3600");
		}

		return new Response(upstreamResponse.body, {
			status: upstreamResponse.status,
			headers,
		});
	} catch {
		return detailedErrorResponse(RANDOM_IMG_ERRORS.UPSTREAM_FETCH_EXCEPTION, {
			hint: "Upstream request failed before receiving a valid response",
		});
	}
};

// ===========================
// 随机图片主处理逻辑
// ===========================
export const handleRandomImg = async (request) => {
	// 处理随机图片请求：参数校验 -> 候选组合筛选 -> 加权抽样 -> redirect/proxy 返回。
	// 解析请求 URL 以获取路径与查询参数。
	const url = new URL(request.url);
	// 提取 URLSearchParams 便于读取与校验参数。
	const params = url.searchParams;

	// 第一步：优先校验链接参数合法性，再做后续处理
	// 执行参数白名单校验，返回值为 null 或错误响应对象。
	const invalidParamsResponse = validateAllowedQueryParams(params, ALLOWED_PARAMS_SET);
	// 若存在非法参数，直接返回错误响应并中止流程。
	if (invalidParamsResponse) {
		return invalidParamsResponse;
	}

	// 读取 method 参数，缺省时默认使用 proxy。
	const method = params.get("m")?.toLowerCase() || "proxy";
	// 强制开关：若关闭重定向，则无论参数如何都用 proxy
	const effectiveMethod = FORCE_REDIRECT_ENABLED ? method : "proxy";

	// 校验 method 参数：仅允许 proxy 或 redirect
	// 判断 method 是否在允许集合内。
	if (!METHOD_SET.has(method)) {
		return invalidFieldError(RANDOM_IMG_ERRORS.INVALID_METHOD, "m", method, METHOD_VALUES);
	}

	// 读取请求指定的设备参数（若未传则为 null）。
	const requestedDevice = params.get("d")?.toLowerCase() || null;
	// 若传入了设备参数，则校验其是否属于请求允许集合。
	if (requestedDevice && !REQUEST_DEVICE_SET.has(requestedDevice)) {
		return invalidFieldError(RANDOM_IMG_ERRORS.INVALID_DEVICE, "d", requestedDevice, REQUEST_DEVICES);
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
		return invalidFieldError(RANDOM_IMG_ERRORS.INVALID_BRIGHTNESS, "b", requestedBrightness, BRIGHTNESS_VALUES);
	}

	// 读取并归一化 theme 参数：支持多次传参与逗号分隔，最终去重。
	const themeParams = Array.from(new Set(params
		.getAll("t")
		.flatMap((value) => value.split(","))
		.map((value) => value.trim().toLowerCase())
		.filter(Boolean)));

	// 处理 device 参数
	const deviceCandidates =
		device === "r"
			? MAP_DEVICES
			: [device];

	// 处理 brightness 参数
	// 若指定亮度则只用该值，否则使用全部亮度候选。
	const brightnessCandidates = requestedBrightness ? [requestedBrightness] : BRIGHTNESS_VALUES;

	// 读取并校验 FOLDER_MAP 配置。
	const { folderMap, errorKey: folderMapErrorKey } = await getFolderMapFromKV();
	// 若配置异常则返回统一配置错误响应。
	if (folderMapErrorKey) {
		return detailedErrorResponse(RANDOM_IMG_ERRORS[folderMapErrorKey], FOLDER_MAP_CONFIG_ERROR_DETAILS);
	}

	// 处理 theme 参数
	let themeCandidates;
	if (themeParams.length > 0) {
		// 按需验证：仅检查请求中的主题是否在配置中存在（Set 查找）。
		const validThemeSet = getValidThemeSet(folderMap);
		const invalidTheme = themeParams.find((candidateTheme) => !validThemeSet.has(candidateTheme));
		if (invalidTheme) {
			return invalidFieldError(RANDOM_IMG_ERRORS.INVALID_THEME, "t", invalidTheme, getValidThemes(folderMap));
		}
		themeCandidates = themeParams;
	} else {
		// 未传 t 时，才构建并使用全量主题候选。
		themeCandidates = getValidThemes(folderMap);
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
		// 指定了亮度或主题但无结果时，返回组合无图错误并回显过滤条件。
		if (requestedBrightness || themeParams.length > 0) {
			return detailedErrorResponse(RANDOM_IMG_ERRORS.NO_IMAGES_FOR_COMBINATION, {
				filters: {
					device,
					brightness: requestedBrightness,
					themes: themeCandidates,
				},
			});
		}
		// 未指定过滤条件且仍无可用图时，返回通用无图错误。
		return detailedErrorResponse(RANDOM_IMG_ERRORS.NO_AVAILABLE_IMAGES, {
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
			return detailedErrorResponse(RANDOM_IMG_ERRORS.NO_AVAILABLE_IMAGES, {
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

	// 读取基础图片 URL 配置。
	const baseImageUrl = await getBaseImageUrlFromKV();
	// 若基础 URL 缺失或无效，则返回配置错误。
	if (!baseImageUrl) {
		return detailedErrorResponse(RANDOM_IMG_ERRORS.BASE_IMAGE_URL_CONFIG_ERROR, BASE_IMAGE_URL_CONFIG_ERROR_DETAILS);
	}

	return await respondImageByMethod(effectiveMethod, buildImageUrl(baseImageUrl, selectedFolder));
};

const buildRandomImgCountData = (folderMap) => {
	const groupTotals = {};
	const themeTotals = {};
	const themeDetails = [];
	let totalImages = 0;
	for (const device of Object.keys(folderMap).sort()) {
		for (const brightness of Object.keys(folderMap[device]).sort()) {
			const groupKey = `${device}-${brightness}`;
			let groupTotal = 0;
			for (const theme of Object.keys(folderMap[device][brightness]).sort()) {
				const count = Number(folderMap[device][brightness][theme] ?? 0);
				groupTotal += count;
				totalImages += count;
				themeTotals[theme] = (themeTotals[theme] ?? 0) + count;
				themeDetails.push({ theme, device, brightness, count });
			}
			groupTotals[groupKey] = groupTotal;
		}
	}
	themeDetails.sort((a, b) =>
		a.theme.localeCompare(b.theme) ||
		a.device.localeCompare(b.device) ||
		a.brightness.localeCompare(b.brightness)
	);
	return {
		totalImages,
		groupTotals,
		themeTotals,
		themeDetails,
	};
};

export const handleRandomImgCount = async (request) => {
	const url = new URL(request.url);
	const randomImgCountPath = await getKvText(RANDOM_IMG_COUNT_PATH_KEY);
	if (!randomImgCountPath) {
		return detailedErrorResponse(RANDOM_IMG_ERRORS.FOLDER_MAP_CONFIG_ERROR, {
			configKey: RANDOM_IMG_COUNT_PATH_KEY,
			namespace: KV_NAMESPACE,
			hint: "计数接口路径未配置，请在 KV 中设置 RANDOM_IMG_COUNT_PATH"
		});
	}
	if (url.pathname !== randomImgCountPath || url.search) {
		return detailedErrorResponse(RANDOM_IMG_ERRORS.INVALID_COUNT_REQUEST);
	}
	const { folderMap, errorKey: folderMapErrorKey } = await getFolderMapFromKV();
	if (folderMapErrorKey) {
		return detailedErrorResponse(RANDOM_IMG_ERRORS[folderMapErrorKey], FOLDER_MAP_CONFIG_ERROR_DETAILS);
	}
	return jsonSuccessResponse(buildRandomImgCountData(folderMap));
};
