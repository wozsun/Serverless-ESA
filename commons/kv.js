// ===========================
// KV 常量
// ===========================

const KV_CACHE_TTL_MS = 60 * 1000;
const KV_NEGATIVE_CACHE_TTL_MS = 3 * 1000;
const KV_GET_MAX_ATTEMPTS = 5;
const KV_RETRY_BASE_DELAY_MS = 60;

// ===========================
// KV 客户端
// ===========================

const edgeKVClients = new Map();

// 按 namespace 获取 EdgeKV 客户端（懒初始化并复用）
const getEdgeKVClient = (namespace) => {
	if (!edgeKVClients.has(namespace)) {
		edgeKVClients.set(namespace, new EdgeKV({ namespace }));
	}
	return edgeKVClients.get(namespace);
};

// ===========================
// 缓存核心
// ===========================

const cacheStores = {
	raw: new Map(),
	boolean: new Map(),
	jsonObject: new Map(),
	number: new Map(),
	text: new Map(),
	textLines: new Map(),
	url: new Map(),
	urlLines: new Map(),
};

// 生成缓存键：优先使用自定义 cacheKey，否则使用 namespace + key
const buildCacheKey = (namespace, key, cacheKey) => cacheKey || `${namespace}::${key}`;

// 重试退避使用的异步睡眠函数
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// 读取缓存值：存在且未过期则命中
const readCache = (cacheStore, id) => {
	const cached = cacheStore.get(id);
	if (!cached) {
		return { hit: false, value: null };
	}
	if (Date.now() >= cached.expiresAt) {
		cacheStore.delete(id);
		return { hit: false, value: null };
	}
	return { hit: true, value: cached.value };
};

// 写入缓存：正常值使用正向 TTL，空值使用负向 TTL
const writeCache = (cacheStore, id, value, ttlMs) => {
	const normalizedValue = value ?? null;
	const ttl = normalizedValue === null ? KV_NEGATIVE_CACHE_TTL_MS : ttlMs;
	cacheStore.set(id, {
		value: normalizedValue,
		expiresAt: Date.now() + ttl,
	});
	return normalizedValue;
};

// 统一流程：读缓存 -> 回源加载 -> 写缓存
const readCachedValue = async ({ cacheStore, id, ttlMs, loader }) => {
	const cached = readCache(cacheStore, id);
	if (cached.hit) {
		return cached.value;
	}
	const loaded = await loader();
	return writeCache(cacheStore, id, loaded, ttlMs);
};

// ===========================
// 源数据归一化
// ===========================

// 删除空行，并对每个非空行做 trim
const toTrimmedNonEmptyLines = (raw) => {
	if (typeof raw !== "string") {
		return null;
	}
	const lines = raw
		.split(/\r?\n/)
		.map((line) => line.trim())
		.filter(Boolean);
	return lines.length > 0 ? lines : null;
};

// 将原始文本归一化为标准载荷（text + lines）
const normalizeRawText = (raw) => {
	const lines = toTrimmedNonEmptyLines(raw);
	if (!lines) {
		return null;
	}
	return {
		text: lines.join("\n"),
		lines,
	};
};

// 严格提取单行：仅当行数等于 1 时返回该行，否则返回 null
const toSingleLine = (payload) => {
	if (!payload || !Array.isArray(payload.lines) || payload.lines.length !== 1) {
		return null;
	}
	return payload.lines[0];
};

// 从 KV 按指定 type 拉取数据，失败时按上限重试；key 不存在或异常均返回 null
const fetchFromKv = async ({ namespace, key, type }) => {
	for (let attempt = 1; attempt <= KV_GET_MAX_ATTEMPTS; attempt++) {
		try {
			const value = await getEdgeKVClient(namespace).get(key, { type });
			return value ?? null;
		} catch {
			if (attempt >= KV_GET_MAX_ATTEMPTS) {
				return null;
			}
			await sleep(KV_RETRY_BASE_DELAY_MS * attempt);
		}
	}

	return null;
};

// 带缓存获取归一化后的源数据载荷
const getNormalizedSourceCached = async ({
	namespace,
	key,
	cacheKey = "",
	ttlMs = KV_CACHE_TTL_MS,
}) => {
	const id = buildCacheKey(namespace, key, cacheKey);
	return readCachedValue({
		cacheStore: cacheStores.raw,
		id,
		ttlMs,
		loader: async () => normalizeRawText(await fetchFromKv({ namespace, key, type: "text" })),
	});
};

// ===========================
// 格式解析器
// ===========================

// 解析严格布尔值：仅接受 true/false（大小写不敏感）
const parseBoolean = (line) => {
	if (!line) {
		return null;
	}
	const lower = line.toLowerCase();
	if (lower === "true") {
		return true;
	}
	if (lower === "false") {
		return false;
	}
	return null;
};

// 解析单行有限数字
const parseNumber = (line) => {
	if (!line) {
		return null;
	}
	const value = Number(line);
	return Number.isFinite(value) ? value : null;
};

// 解析单行文本
const parseText = (line) => line || null;

// 解析多行文本
const parseTextLines = (lines) => (lines && lines.length > 0 ? lines : null);

// 解析并规范化单行 URL
const parseUrl = (line) => {
	if (!line) {
		return null;
	}
	try {
		const parsed = new URL(line);
		const normalized = parsed.toString();
		return normalized.endsWith("/") ? normalized : `${normalized}/`;
	} catch {
		return null;
	}
};

// 解析并规范化多行 URL
const parseUrlLines = (lines) => {
	if (!lines) {
		return null;
	}
	const urls = [];
	for (const line of lines) {
		const url = parseUrl(line);
		if (!url) {
			return null;
		}
		urls.push(url);
	}
	return urls.length > 0 ? urls : null;
};

// ===========================
// Getter 工厂
// ===========================

// 根据 sourceType 从归一化载荷中提取对应片段
const pickSource = (payload, sourceType) => {
	if (!payload) {
		return null;
	}
	if (sourceType === "line") {
		return toSingleLine(payload);
	}
	if (sourceType === "lines") {
		return payload.lines;
	}
	return null;
};

// 构建类型化 KV Getter：组合 source 提取、严格解析和独立缓存
const createTypedKvGetter = ({ cacheStore, sourceType, parser }) => {
	return async ({ namespace, key, cacheKey = "", ttlMs = KV_CACHE_TTL_MS }) => {
		const id = buildCacheKey(namespace, key, cacheKey);
		return readCachedValue({
			cacheStore,
			id,
			ttlMs,
			loader: async () => {
				const payload = await getNormalizedSourceCached({ namespace, key, cacheKey, ttlMs });
				const source = pickSource(payload, sourceType);
				return parser(source);
			},
		});
	};
};

// ===========================
// 对外类型接口
// ===========================

// 1) 严格布尔值（单行 true/false，大小写不敏感）
export const getKvBooleanCached = createTypedKvGetter({
	cacheStore: cacheStores.boolean,
	sourceType: "line",
	parser: parseBoolean,
});

// 2) JSON 对象（通过 type:"json" 直接获取，不做本地校验与类型转换）
export const getKvJsonObjectCached = async ({ namespace, key, cacheKey = "", ttlMs = KV_CACHE_TTL_MS }) => {
	const id = buildCacheKey(namespace, key, cacheKey);
	return readCachedValue({
		cacheStore: cacheStores.jsonObject,
		id,
		ttlMs,
		loader: () => fetchFromKv({ namespace, key, type: "json" }),
	});
};

// 3) 单行数字
export const getKvNumberCached = createTypedKvGetter({
	cacheStore: cacheStores.number,
	sourceType: "line",
	parser: parseNumber,
});

// 4) 单行文本
export const getKvTextCached = createTypedKvGetter({
	cacheStore: cacheStores.text,
	sourceType: "line",
	parser: parseText,
});

// 5) 多行文本
export const getKvTextLinesCached = createTypedKvGetter({
	cacheStore: cacheStores.textLines,
	sourceType: "lines",
	parser: parseTextLines,
});

// 6) 单行 URL
export const getKvUrlCached = createTypedKvGetter({
	cacheStore: cacheStores.url,
	sourceType: "line",
	parser: parseUrl,
});

// 7) 多行 URL
export const getKvUrlLinesCached = createTypedKvGetter({
	cacheStore: cacheStores.urlLines,
	sourceType: "lines",
	parser: parseUrlLines,
});

