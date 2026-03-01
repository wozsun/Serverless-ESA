import { handleRandomImg, handleRandomImgCount } from "../random-img/function.js";
import { jsonErrorResponse } from "./response.js";

// ===========================
// 全局错误消息 Global Errors
// ===========================
const GLOBAL_ERRORS = {
	NOT_FOUND: { status: 404, message: "API Not Found" },
	INTERNAL_ERROR: { status: 500, message: "Internal Server Error" },
};

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
			console.error("Unhandled error in edge function:", error);
			return jsonErrorResponse(GLOBAL_ERRORS.INTERNAL_ERROR);
		}
	},
};
