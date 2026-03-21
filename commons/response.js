// ===========================
// 响应工具函数
// ===========================

// 组装标准错误 JSON（可附带 details），并以指定缩进输出响应体。
export const jsonErrorResponse = (error, details = undefined, spaces = 2) => {
	const payload = {
		status: error.status,
		message: error.message,
	};

	if (details && Object.keys(details).length > 0) {
		payload.details = details;
	}

	return new Response(JSON.stringify(payload, null, spaces), {
		status: error.status,
		headers: { "Content-Type": "application/json; charset=utf-8" },
	});
};

// 将任意成功数据封装为 JSON 响应并统一设置 Content-Type 与缩进。
export const jsonSuccessResponse = (data, status = 200, spaces = 2) =>
	new Response(JSON.stringify(data, null, spaces), {
		status,
		headers: { "Content-Type": "application/json; charset=utf-8" },
	});
