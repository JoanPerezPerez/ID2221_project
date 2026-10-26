export type Track = {
	track_name: string;
	track_uri: string;
	artist_name: string;
	artist_uri?: string;
	album_name?: string;
	album_uri?: string;
	duration_ms?: number;
	pos?: number;
};

// En låt som rekommenderas, med beräknad likhetspoäng
export type RecommendedTrack = Track & {
	similarity: number;
	totalScore?: number;
};

export type TrackRecommendations = {
	track_uri: string;
	recommendations: RecommendedTrack[];
};
