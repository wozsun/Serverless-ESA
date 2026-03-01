// ===========================
// 全局错误消息 Global Errors
// ===========================
const GLOBAL_ERRORS = {
    NOT_FOUND: { status: 404, message: "API Not Found" },
    INTERNAL_ERROR: { status: 500, message: "Internal Server Error" },
};

// ===========================
// 通用错误响应：返回 JSON 格式的错误信息
// ===========================
const jsonErrorResponse = (error) =>
    new Response(JSON.stringify({ status: error.status, message: error.message }), {
        status: error.status,
        headers: { "Content-Type": "application/json" },
    });

// ===========================
// 随机图片 API 配置
// ===========================
const RANDOM_IMG_CONFIG = {
    BASE_IMAGE_URL: "https://example.com/", // 图片基础路径
    ALLOWED_PARAMS: new Set(["d", "b", "t", "m"]), // 允许的查询参数 d->device, b->brightness, t->theme, m->method
    VALID_DEVICES: new Set(["pc", "mb", "r"]),                 // 合法设备类型（r 表示忽略设备）
    VALID_BRIGHTNESS: new Set(["dark", "light"]),              // 合法明暗类型
    VALID_METHODS: new Set(["proxy", "redirect"]),   // 合法 method（proxy, redirect）
    FOLDER_MAP: {
        mb: {
            dark: {
                acg: 0,
                fddm: 2,
                ghost: 35,
                koh: 0,
                wlop: 6,
            },
            light: {
                acg: 0,
                fddm: 4,
                ghost: 44,
                koh: 0,
                wlop: 4,
            },
        },
        pc: {
            dark: {
                acg: 15,
                fddm: 2,
                ghost: 13,
                koh: 0,
                wlop: 13,
            },
            light: {
                acg: 12,
                fddm: 4,
                ghost: 15,
                koh: 4,
                wlop: 11,
            },
        },
    },
};

// 随机图片 API 错误信息
const RANDOM_IMG_ERRORS = {
    INVALID_QUERY_PARAMS: { status: 400, message: "Bad Request: Invalid query parameters" },
    INVALID_DEVICE: { status: 400, message: "Bad Request: Invalid device" },
    INVALID_BRIGHTNESS: { status: 400, message: "Bad Request: Invalid brightness" },
    INVALID_THEME: { status: 400, message: "Bad Request: Invalid theme" },
    INVALID_METHOD: { status: 400, message: "Bad Request: Invalid method" },
    NO_IMAGES_FOR_COMBINATION: { status: 404, message: "Not Found: No available images for the selected filters" },
    NO_AVAILABLE_IMAGES: { status: 404, message: "Not Found: No available images" },
    FETCH_FAILED: { status: 502, message: "Bad Gateway: Failed to fetch image" },
    FETCH_ERROR: { status: 502, message: "Bad Gateway: Error fetching image" },
};

// ===========================
// 随机图片 API 逻辑
// ===========================
const handleRandomImg = async (request) => {
    const url = new URL(request.url);
    const params = url.searchParams;

    // 参数校验：不允许出现未定义参数
    for (const key of params.keys()) {
        if (!RANDOM_IMG_CONFIG.ALLOWED_PARAMS.has(key)) {
            return jsonErrorResponse(RANDOM_IMG_ERRORS.INVALID_QUERY_PARAMS);
        }
    }

    const userAgent = request.headers.get("User-Agent") || "";
    const isMobile = /Mobi|Android|iPhone/i.test(userAgent);
    const device = params.get("d")?.toLowerCase() || (isMobile ? "mb" : "pc");
    const brightness = params.get("b")?.toLowerCase() || null;
    const theme = params.get("t")?.toLowerCase() || null;
    const folderMap = RANDOM_IMG_CONFIG.FOLDER_MAP;

    //device处理
    if (!RANDOM_IMG_CONFIG.VALID_DEVICES.has(device)) {
        return jsonErrorResponse(RANDOM_IMG_ERRORS.INVALID_DEVICE);
    }
    const deviceCandidates =
        device === "r"
            ? Array.from(RANDOM_IMG_CONFIG.VALID_DEVICES)
                .filter((candidate) => candidate !== "r")
            : [device];

    //brightness处理
    if (brightness && !RANDOM_IMG_CONFIG.VALID_BRIGHTNESS.has(brightness)) {
        return jsonErrorResponse(RANDOM_IMG_ERRORS.INVALID_BRIGHTNESS);
    }
    const brightnessCandidates = brightness ? [brightness] : Array.from(RANDOM_IMG_CONFIG.VALID_BRIGHTNESS);

    //theme处理
    const validThemes = Array.from(
        new Set(
            deviceCandidates.flatMap((candidateDevice) =>
                Object.values(folderMap[candidateDevice]).flatMap((brightnessMap) =>
                    Object.keys(brightnessMap)
                )
            )
        )
    );
    if (theme && !validThemes.includes(theme)) {
        return jsonErrorResponse(RANDOM_IMG_ERRORS.INVALID_THEME);
    }
    const themeCandidates = theme ? [theme] : validThemes;

    const candidates = [];
    for (const candidateDevice of deviceCandidates) {
        const deviceMap = folderMap[candidateDevice];
        for (const b of brightnessCandidates) {
            for (const t of themeCandidates) {
                const count = deviceMap[b]?.[t] ?? 0;
                if (count > 0) {
                    candidates.push({ device: candidateDevice, brightness: b, theme: t, count });
                }
            }
        }
    }

    if (candidates.length === 0) {
        if (brightness || theme) {
            return jsonErrorResponse(RANDOM_IMG_ERRORS.NO_IMAGES_FOR_COMBINATION);
        }
        return jsonErrorResponse(RANDOM_IMG_ERRORS.NO_AVAILABLE_IMAGES);
    }

    const selectedFolder = candidates[Math.floor(Math.random() * candidates.length)];

    // 随机选取图片编号
    const imageNumber = Math.floor(Math.random() * selectedFolder.count) + 1;
    const imageFilename = `${String(imageNumber).padStart(6, "0")}.webp`;
    const imageUrl = `${RANDOM_IMG_CONFIG.BASE_IMAGE_URL}${selectedFolder.device}-${selectedFolder.brightness}/${selectedFolder.theme}/${imageFilename}`;

    const method = params.get("m")?.toLowerCase() || "proxy";

    // method 参数校验：仅允许 proxy 或 redirect
    if (!RANDOM_IMG_CONFIG.VALID_METHODS.has(method)) {
        return jsonErrorResponse(RANDOM_IMG_ERRORS.INVALID_METHOD);
    }

    // 仅当 method=redirect 时使用重定向，method=proxy 时代理返回图片内容
    if (method === "redirect") {
        return new Response(null, {
            status: 302,
            headers: { Location: imageUrl },
        });
    }

    try {
        const upstreamResponse = await fetch(imageUrl);

        if (!upstreamResponse.ok) {
            return jsonErrorResponse(RANDOM_IMG_ERRORS.FETCH_FAILED);
        }

        const headers = new Headers(upstreamResponse.headers);
        if (!headers.has("Cache-Control")) {
            headers.set("Cache-Control", "public, max-age=3600");
        }

        return new Response(upstreamResponse.body, {
            status: upstreamResponse.status,
            headers,
        });
    } catch (error) {
        console.error("Failed to fetch upstream image:", error);
        return jsonErrorResponse(RANDOM_IMG_ERRORS.FETCH_ERROR);
    }

};

const buildRandomImgCountData = () => {
    const folderMap = RANDOM_IMG_CONFIG.FOLDER_MAP;
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

const handleRandomImgCount = async () =>
    new Response(`${JSON.stringify(buildRandomImgCountData(), null, 2)}\n`, {
        status: 200,
        headers: { "Content-Type": "application/json; charset=utf-8" },
    });

// ===========================
// 路由配置
// ===========================
const routes = {
    "/": async () => jsonErrorResponse(GLOBAL_ERRORS.NOT_FOUND),
    "/hello": async () =>
        new Response(JSON.stringify({ message: "Hello, World!" }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        }),
    "/healthcheck": async () =>
        new Response(JSON.stringify({ message: "API on EdgeFunction is healthy" }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        }),
    "/random-img": handleRandomImg,
    "/random-img-count": handleRandomImgCount,
};

// ===========================
// 边缘函数入口函数
// ===========================
export default {
    async fetch(request) {
        try {
            const { pathname } = new URL(request.url);
            const handler = routes[pathname];

            if (handler) {
                return await handler(request);
            }

            return jsonErrorResponse(GLOBAL_ERRORS.NOT_FOUND);
        } catch (error) {
            // 捕获未预期的错误，避免函数崩溃
            console.error('Unhandled error in edge function:', error);
            return jsonErrorResponse(GLOBAL_ERRORS.INTERNAL_ERROR);
        }
    },
};
