import { json } from '@sveltejs/kit';
import type { RecommendedTrack, TrackRecommendations } from '#lib/types.ts';
import type { RequestHandler } from './$types';

// Simulerad MongoDB-data med förberäknade toppgrannar
const MOCK_DB: TrackRecommendations[] = [
	{
		track_uri: 'spotify:track:1',
		recommendations: [
			{
				track_uri: 'spotify:track:10',
				track_name: 'Killer Queen',
				artist_name: 'Queen',
				similarity: 0.95
			},
			{
				track_uri: 'spotify:track:11',
				track_name: 'Somebody to Love',
				artist_name: 'Queen',
				similarity: 0.88
			},
			{
				track_uri: 'spotify:track:12',
				track_name: 'Space Oddity',
				artist_name: 'David Bowie',
				similarity: 0.81
			}
		]
	},
	{
		track_uri: 'spotify:track:3',
		recommendations: [
			{
				track_uri: 'spotify:track:12',
				track_name: 'Space Oddity',
				artist_name: 'David Bowie',
				similarity: 0.92
			},
			{
				track_uri: 'spotify:track:13',
				track_name: 'Life on Mars?',
				artist_name: 'David Bowie',
				similarity: 0.89
			},
			{
				track_uri: 'spotify:track:10',
				track_name: 'Killer Queen',
				artist_name: 'Queen',
				similarity: 0.75
			}
		]
	}
];

export const GET: RequestHandler = async ({ url }) => {
	const urisParam = url.searchParams.get('uris') ?? '';
	const selectedUris = urisParam ? urisParam.split(',') : [];

	if (selectedUris.length === 0) {
		return json([]);
	}

	// 1. Slå ihop och summera likhetspoäng
	const scoreMap = new Map<string, RecommendedTrack>();

	for (const uri of selectedUris) {
		const neighbors = MOCK_DB.find((doc) => doc.track_uri === uri)?.recommendations ?? [];
		for (const track of neighbors) {
			// Hoppa över om låten redan valts av användaren
			if (selectedUris.includes(track.track_uri)) continue;

			const existing = scoreMap.get(track.track_uri);
			if (!existing) {
				scoreMap.set(track.track_uri, { ...track, totalScore: track.similarity });
			} else {
				existing.totalScore = (existing.totalScore ?? existing.similarity) + track.similarity;
			}
		}
	}

	// 2. Sortera fallande efter totalScore
	const sorted = Array.from(scoreMap.values()).sort(
		(a, b) => (b.totalScore ?? b.similarity) - (a.totalScore ?? a.similarity)
	);

	return json(sorted);
};
