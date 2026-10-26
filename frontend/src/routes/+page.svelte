<script lang="ts">
	import Recommendations from '#lib/components/recommendations.svelte';
	import Search from '#lib/components/search.svelte';
	import type { RecommendedTrack, Track } from '#lib/types.ts';

	// Search and results
	let searchQuery = $state('');
	let searchResults = $state<Track[]>([]);

	// Selected reference tracks
	let selectedTracks = $state<Track[]>([]);

	// Recommendations
	let recommendations = $state<RecommendedTrack[]>([]);
	let isLoadingRecommendations = $state(false);

	// Debounced search
	$effect(() => {
		if (!searchQuery.trim()) {
			searchResults = [];
			return;
		}

		const timer = setTimeout(async () => {
			try {
				const res = await fetch(`/api/search?q=${encodeURIComponent(searchQuery)}`);
				if (res.ok) {
					searchResults = await res.json();
				}
			} catch (err) {
				console.error('Search error:', err);
			}
		}, 300);

		return () => clearTimeout(timer);
	});

	// Fetch recommendations when selected tracks change
	$effect(() => {
		if (selectedTracks.length === 0) {
			recommendations = [];
			return;
		}

		const uris = selectedTracks.map((t) => t.track_uri);

		async function fetchRecommendations() {
			isLoadingRecommendations = true;
			try {
				const res = await fetch(`/api/recommendations?uris=${encodeURIComponent(uris.join(','))}`);
				if (res.ok) {
					recommendations = await res.json();
				}
			} catch (err) {
				console.error('Failed to fetch recommendations:', err);
			} finally {
				isLoadingRecommendations = false;
			}
		}

		fetchRecommendations();
	});
</script>

<main class="min-h-screen px-4 py-12">
	<div class="mx-auto flex max-w-xl flex-col items-center gap-6">
		<!-- Header -->
		<header class="space-y-1 text-center">
			<h1 class="text-2xl font-bold tracking-tight text-surface-950-50 sm:text-3xl">
				Spotify Recommendation Engine
			</h1>
			<p class="text-sm text-surface-600-400">
				Explore and generate music recommendations based on your favorite tracks
			</p>
		</header>

		<Search bind:query={searchQuery} {searchResults} bind:selectedTracks />

		<Recommendations
			{recommendations}
			isLoading={isLoadingRecommendations}
			hasSelectedTracks={selectedTracks.length > 0}
		/>
	</div>
</main>
