const handleRandomImg = async (request) => {
    
    const url = new URL(request.url);
    const params = url.searchParams;

    const allowedParams = new Set(["type", "theme"]);
    for (const key of params.keys()) {
        if (!allowedParams.has(key)) {
            return new Response("Bad Request: Invalid query parameters", { status: 400 });
        }
    }

    const userAgent = request.headers.get("User-Agent");
    const isMobile = /Mobi|Android|iPhone/i.test(userAgent);

    const validTypes = new Set(["pc", "mb", "sq"]);
    const type = params.get("type") || (isMobile ? "mb" : "pc");
    if (!validTypes.has(type)) {
        return new Response("Bad Request: Invalid type", { status: 400 });
    }

    const folderMap = {
        dark: { pc: 27, mb: 6, sq: 0 },
        light: { pc: 28, mb: 4, sq: 0 },
        fddm: { pc: 1, mb: 1, sq: 0 }
    };

    const theme = params.get("theme");
    if (theme && !folderMap[theme]) {
        return new Response("Bad Request: Invalid theme", { status: 400 });
    }

    if (theme && folderMap[theme][type] === 0) {
        return new Response("No available images", { status: 404 });
    }

    const availableThemes = Object.keys(folderMap).filter(theme => folderMap[theme][type] > 0);
    if (availableThemes.length === 0) {
        return new Response("No available images", { status: 404 });
    }

    const finalTheme = theme || availableThemes[Math.floor(Math.random() * availableThemes.length)];
    const imageNumber = Math.floor(Math.random() * folderMap[finalTheme][type]) + 1;
    const imageUrl = `https://example.com/${type}-${finalTheme}/${imageNumber}.webp`;

    return new Response(null, {
        status: 302,
        headers: {
            "Location": imageUrl,
            "Cache-Control": "public, max-age=7200"
        }
    });
};

const routes = {
    '/hello': async () => new Response("Hello, World!", { status: 200 }),
    '/random-img': handleRandomImg,
};

export default {
    async fetch(request) {
        const { pathname } = new URL(request.url);
        const handler = routes[pathname];
        return handler ? handler(request) : new Response('API Not Found', { status: 404 });
    }
};