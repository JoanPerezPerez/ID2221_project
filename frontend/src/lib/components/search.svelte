<script lang="ts">
	import type { Track } from '#lib/types.ts';
	import {
		Combobox,
		Portal,
		type ComboboxRootProps,
		useListCollection
	} from '@skeletonlabs/skeleton-svelte';

	let {
		query = $bindable(''),
		searchResults = [],
		selectedTracks = $bindable([])
	}: {
		query?: string;
		searchResults?: Track[];
		selectedTracks?: Track[];
	} = $props();

	const collection = $derived(
		useListCollection({
			items: searchResults.map((track: Track) => ({
				label: `${track.track_name} - ${track.artist_name}`,
				value: track.track_uri
			})),
			itemToString: (item) => item.label,
			itemToValue: (item) => item.value
		})
	);

	const onInputValueChange: ComboboxRootProps['onInputValueChange'] = (event) => {
		query = event.inputValue;
	};

	const onValueChange: ComboboxRootProps['onValueChange'] = (event) => {
		const allKnown = [...selectedTracks, ...searchResults];
		const updated: Track[] = [];

		for (const uri of event.value) {
			const found = allKnown.find((t) => t.track_uri === uri);
			if (found && !updated.some((t) => t.track_uri === uri)) {
				updated.push(found);
			}
		}

		selectedTracks = updated;
	};

	function removeTrack(trackUri: string) {
		selectedTracks = selectedTracks.filter((t) => t.track_uri !== trackUri);
	}

	let selectedUris = $derived(selectedTracks.map((track) => track.track_uri));
</script>

<div class="card preset-filled-surface-100-900 w-full space-y-4 p-4">
	<div>
		<h2 class="h6">Search for songs</h2>
		<p class="opacity-60 text-xs">Song or artist</p>
	</div>

	<Combobox
		placeholder="Search..."
		{collection}
		{onInputValueChange}
		value={selectedUris}
		{onValueChange}
		multiple
	>
		<Combobox.Control>
			<Combobox.Input class="input" />
		</Combobox.Control>

		<Portal>
			<Combobox.Positioner>
				<Combobox.Content class="card preset-filled-surface-100-900 z-50 max-h-60 overflow-y-auto p-2 shadow-xl">
					{#each collection.items as item (item.value)}
						<Combobox.Item {item} class="btn hover:preset-tonal w-full justify-between">
							<Combobox.ItemText>{item.label}</Combobox.ItemText>
							<Combobox.ItemIndicator>✓</Combobox.ItemIndicator>
						</Combobox.Item>
					{:else}
						{#if query.trim().length > 0}
							<div class="p-3 text-center text-xs opacity-60">
								No tracks found
							</div>
						{/if}
					{/each}
				</Combobox.Content>
			</Combobox.Positioner>
		</Portal>
	</Combobox>

	{#if selectedTracks.length > 0}
		<div class="flex flex-wrap gap-2 pt-2">
			{#each selectedTracks as track (track.track_uri)}
				<span class="badge preset-filled-primary-500">
					{track.track_name}
					<button
						type="button"
						class="btn-icon btn-icon-xs opacity-75 hover:opacity-100"
						onclick={() => removeTrack(track.track_uri)}
					>
						✕
					</button>
				</span>
			{/each}
		</div>
	{/if}
</div>
