export const jsonErrorResponse = (error) =>
	new Response(JSON.stringify({ status: error.status, message: error.message }), {
		status: error.status,
		headers: { "Content-Type": "application/json" },
	});