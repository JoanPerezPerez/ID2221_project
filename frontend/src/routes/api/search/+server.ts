import { json } from '@sveltejs/kit';
import type { Track } from '#lib/types.ts';
import type { RequestHandler } from './$types';

// Mockade sökbara låtar (där spotify:track:1 och spotify:track:3 har rekommendationer)
const MOCK_TRACKS: Track[] = [
	{
		track_uri: 'spotify:track:1',
		track_name: 'Bohemian Rhapsody',
		artist_name: 'Queen',
		album_name: 'A Night at the Opera'
	},
	{
		track_uri: 'spotify:track:2',
		track_name: "Don't Stop Me Now",
		artist_name: 'Queen',
		album_name: 'Jazz'
	},
	{
		track_uri: 'spotify:track:3',
		track_name: 'Heroes',
		artist_name: 'David Bowie',
		album_name: '"Heroes"'
	},
	{
		track_uri: 'spotify:track:10',
		track_name: 'Killer Queen',
		artist_name: 'Queen',
		album_name: 'Sheer Heart Attack'
	},
	{
		track_uri: 'spotify:track:11',
		track_name: 'Somebody to Love',
		artist_name: 'Queen',
		album_name: 'A Day at the Races'
	},
	{
		track_uri: 'spotify:track:12',
		track_name: 'Space Oddity',
		artist_name: 'David Bowie',
		album_name: 'David Bowie'
	},
	{
		track_uri: 'spotify:track:13',
		track_name: 'Life on Mars?',
		artist_name: 'David Bowie',
		album_name: 'Hunky Dory'
	}
];

export const GET: RequestHandler = async ({ url }) => {
	const query = (url.searchParams.get('q') ?? '').toLowerCase().trim();

	if (!query) {
		return json([]);
	}

	const results = query
		? MOCK_TRACKS.filter((t) => t.track_name.toLowerCase().includes(query))
		: [];

	return json(results);
};
