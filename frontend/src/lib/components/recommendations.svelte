<script lang="ts">
	import type { RecommendedTrack } from '#lib/types.ts';

	let {
		recommendations = [],
		isLoading = false,
		hasSelectedTracks = false
	}: {
		recommendations?: RecommendedTrack[];
		isLoading?: boolean;
		hasSelectedTracks?: boolean;
	} = $props();
</script>

<div class="card preset-filled-surface-100-900 w-full space-y-4 p-4">
	<div class="flex items-center justify-between">
		<div>
			<h2 class="h6">Suggested Songs</h2>
			<p class="opacity-60 text-xs">Based on selected tracks</p>
		</div>
		{#if recommendations.length > 0}
			<span class="badge preset-tonal-primary text-xs">{recommendations.length}</span>
		{/if}
	</div>

	{#if isLoading}
		<p class="p-4 text-center text-sm opacity-60">Calculating similar songs...</p>
	{:else if recommendations.length > 0}
		<ul class="space-y-2">
			{#each recommendations as rec (rec.track_uri)}
				<li class="card preset-outlined-surface-200-800 flex items-center justify-between p-3">
					<div>
						<p class="font-medium text-sm">{rec.track_name}</p>
						<p class="opacity-60 text-xs">{rec.artist_name}</p>
					</div>
					<span class="badge preset-tonal text-xs">
						{(rec.totalScore ?? rec.similarity).toFixed(2)}
					</span>
				</li>
			{/each}
		</ul>
	{:else if hasSelectedTracks}
		<p class="p-4 text-center text-sm opacity-60">No recommendations found</p>
	{:else}
		<p class="p-4 text-center text-sm opacity-60">Select songs above to see suggestions</p>
	{/if}
</div>
