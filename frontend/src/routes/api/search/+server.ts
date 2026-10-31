
import type { RequestHandler } from './$types';

const BACKEND_URL = 'http://127.0.0.1:8000';

export const GET: RequestHandler = async ({ url, fetch }) => {
    const query = url.searchParams.get('q')?.trim() ?? '';

    if (!query) {
        return Response.json([]);
    }

    try {
        const backendUrl = new URL(
            '/api/tracks/search',
            BACKEND_URL
        );

        backendUrl.searchParams.set('q', query);
        backendUrl.searchParams.set('limit', '20');

        const response = await fetch(backendUrl.toString(), {
            headers: {
                Accept: 'application/json'
            },
            signal: AbortSignal.timeout(15000)
        });

        const data = await response.json();

        return Response.json(data, {
            status: response.status
        });
    } catch (error) {
        console.error('Track search backend error:', error);

        return Response.json(
            { detail: 'Unable to search tracks' },
            { status: 502 }
        );
    }
};
