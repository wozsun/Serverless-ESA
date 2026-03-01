const GLOBAL_ERRORS = {
    NOT_FOUND: { status: 404, message: "API Not Found" },
    INTERNAL_ERROR: { status: 500, message: "Internal Server Error" },
};

const jsonErrorResponse = (error) =>
    new Response(JSON.stringify({ status: error.status, message: error.message }), {
        status: error.status,
        headers: { "Content-Type": "application/json" },
    });

const RANDOM_IMG_CONFIG = {
    BASE_IMAGE_URL: "https://example.com/",
    ALLOWED_PARAMS: new Set(["type", "theme"]),
    VALID_TYPES: new Set(["pc", "mb", "sq"]),
    FOLDER_MAP: {
        dark: { pc: 27, mb: 6, sq: 0 },
        light: { pc: 28, mb: 4, sq: 0 },
        fddm: { pc: 8, mb: 15, sq: 1 },
    },
};

const RANDOM_IMG_ERRORS = {
    INVALID_QUERY_PARAMS: { status: 400, message: "Bad Request: Invalid query parameters" },
    INVALID_TYPE: { status: 400, message: "Bad Request: Invalid type" },
    INVALID_THEME: { status: 400, message: "Bad Request: Invalid theme" },
    NO_IMAGES_FOR_THEME: (theme) => ({ status: 404, message: `Not Found: No available images for theme ${theme}` }),
    NO_AVAILABLE_IMAGES: { status: 404, message: "Not Found: No available images" },
    FETCH_FAILED: { status: 502, message: "Bad Gateway: Failed to fetch image" },
    FETCH_ERROR: { status: 502, message: "Bad Gateway: Error fetching image" },
};

const handleRandomImg = async (request) => {
    const url = new URL(request.url);
    const params = url.searchParams;

    for (const key of params.keys()) {
        if (!RANDOM_IMG_CONFIG.ALLOWED_PARAMS.has(key)) {
            return jsonErrorResponse(RANDOM_IMG_ERRORS.INVALID_QUERY_PARAMS);
        }
    }

    const userAgent = request.headers.get("User-Agent") || "";
    const isMobile = /Mobi|Android|iPhone/i.test(userAgent);
    const type = params.get("type") || (isMobile ? "mb" : "pc");

    if (!RANDOM_IMG_CONFIG.VALID_TYPES.has(type)) {
        return jsonErrorResponse(RANDOM_IMG_ERRORS.INVALID_TYPE);
    }

    const theme = params.get("theme");
    const folderMap = RANDOM_IMG_CONFIG.FOLDER_MAP;

    let finalTheme;

    if (theme) {
        const themeData = folderMap[theme];
        if (!themeData) {
            return jsonErrorResponse(RANDOM_IMG_ERRORS.INVALID_THEME);
        }
        if (themeData[type] === 0) {
            return jsonErrorResponse(RANDOM_IMG_ERRORS.NO_IMAGES_FOR_THEME(theme));
        }
        finalTheme = theme;
    } else {

        const availableThemes = Object.keys(folderMap).filter((t) => folderMap[t][type] > 0);
        if (availableThemes.length === 0) {
            return jsonErrorResponse(RANDOM_IMG_ERRORS.NO_AVAILABLE_IMAGES);
        }
        finalTheme = availableThemes[Math.floor(Math.random() * availableThemes.length)];
    }

    const imageNumber = Math.floor(Math.random() * folderMap[finalTheme][type]) + 1;
    const imageUrl = `${RANDOM_IMG_CONFIG.BASE_IMAGE_URL}${type}-${finalTheme}/${imageNumber}.webp`;

    return new Response(null, {
        status: 302,
        headers: { Location: imageUrl },
    });


};

const routes = {
    "/": async () => jsonErrorResponse(GLOBAL_ERRORS.NOT_FOUND),
    "/hello": async () =>
        new Response(JSON.stringify({ message: "Hello, World!" }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
        }),
    "/random-img": handleRandomImg,
};

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
