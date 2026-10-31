
import type { RequestHandler } from './$types';

const BACKEND_URL = 'http://127.0.0.1:8000';

export const GET: RequestHandler = async ({ url, fetch }) => {
    const uris = url.searchParams.get('uris')?.trim() ?? '';

    // No tracks selected
    if (!uris) {
        return Response.json([]);
    }

    try {
        const backendUrl = new URL(
            '/api/recommendations/tracks',
            BACKEND_URL
        );

        backendUrl.searchParams.set('uris', uris);
        backendUrl.searchParams.set('k', '20');

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
        console.error('Recommendation backend error:', error);

        return Response.json(
            { detail: 'Unable to fetch recommendations' },
            { status: 502 }
        );
    }
};
