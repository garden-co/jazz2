<template>
  <div class="space-bg">
    <div class="control-bar">
      <div class="control-bar__brand">
        <div v-if="bandNameDisplay" class="band-name-row">
          <input
            v-if="editingBandName"
            ref="bandNameInput"
            class="band-name-input"
            :value="bandNameDisplay"
            @keydown.enter="saveBandName(($event.target as HTMLInputElement).value)"
            @keydown.escape="editingBandName = false"
            @blur="saveBandName(($event.target as HTMLInputElement).value)"
          />
          <span
            v-else
            class="band-name"
            :class="{ editable: canEdit }"
            @click="canEdit && startEditBandName()"
            >{{ bandNameDisplay }}<span v-if="canEdit" class="band-name-pencil">✎</span></span
          >
        </div>
        <div class="wordmark">
          <span class="wordmark__world">WORLD</span>
          <span class="wordmark__tour">TOUR</span>
        </div>
      </div>
      <div class="control-bar__row">
        <BandLogo v-if="firstBandId" :bandId="firstBandId" />
        <button v-if="canEdit" class="btn-auth" @click="switchView()">View as public</button>
        <span v-if="!canEdit" class="public-label">Public view</span>
        <button v-if="!canEdit" class="btn-auth" @click="switchView()">Log in</button>
      </div>
    </div>
    <div id="map" style="width: 100vw; height: 100vh"></div>

    <AddStopPopover
      :x="popoverX"
      :y="popoverY"
      :visible="popoverVisible"
      @confirm="onPopoverConfirm"
      @dismiss="dismissPopover"
    />

    <Sheet :open="sheetOpen" @close="closeSheet" @closed="onSheetClosed">
      <TourCalendar
        v-if="sheetMode === 'detail'"
        :stops="calendarStops"
        :selectedStopId="selectedStop?.id ?? null"
        @selectStop="onCalendarSelectStop"
      />
      <StopDetail
        v-if="sheetMode === 'detail' && selectedStop"
        :stop="selectedStop"
        @close="closeSheet"
      />
      <StopCreateForm
        v-if="sheetMode === 'create' && firstBandId"
        :lat="createLat"
        :lng="createLng"
        :bandId="firstBandId"
        @created="closeSheet"
        @cancel="closeSheet"
      />
    </Sheet>

    <TourPoster
      v-if="showLandingPoster && landingPosterStops.length > 0"
      :bandName="bandsData?.[0]?.name ?? 'Unknown Band'"
      :stops="landingPosterStops"
      @dismiss="showLandingPoster = false"
    />

    <div
      v-if="showLandingPoster && landingPosterStops.length === 0"
      class="splash-overlay"
      @click="showLandingPoster = false"
    >
      <div class="splash-modal" @click.stop>
        <p class="splash-text" style="margin-bottom: 8px">No stops planned yet</p>
        <p class="splash-subtext">Log in as a band member to add tour dates</p>
        <button class="splash-btn" @click="showLandingPoster = false">Explore the globe</button>
      </div>
    </div>

    <StopPoster
      v-if="posterStop && !showLandingPoster"
      :stop="posterStop"
      :bandName="bandsData?.[0]?.name ?? 'Unknown Band'"
      @close="posterStop = null"
    />

    <GeolocateFab :sheetOpen="sheetOpen" @locate="onGeolocate" />

    <!-- Splash hint for logged-in users -->
    <Transition name="splash-fade">
      <div v-if="showSplash" class="splash-overlay" @click="showSplash = false">
        <div class="splash-modal" @click.stop>
          <p class="splash-text">Click on an event to edit details</p>
          <button class="splash-btn" @click="showSplash = false">Got it</button>
        </div>
      </div>
    </Transition>

    <button
      class="tour-btn"
      :class="{ 'sheet-open': sheetOpen, touring: isTouring }"
      @click="isTouring ? stopTour() : startTour()"
    >
      {{ isTouring ? "■ Stop" : "▶ Tour" }}
    </button>
  </div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref, computed, watch, nextTick } from "vue";
import { useAll, useDb, useSession } from "jazz-tools/vue";
import { app } from "../schema.js";
import type { StopWithVenue } from "../schema.js";
import { MapController, type StopMapData } from "./lib/map-controller";
import { findNearestStop } from "./lib/nearest-stop";
import type { StopWithLocation } from "./lib/nearest-stop";
import { ensureData } from "./seed-loader";
import { isPublicMode } from "./constants";
import Sheet from "./components/Sheet.vue";
import StopDetail from "./components/StopDetail.vue";
import StopPoster from "./components/StopPoster.vue";
import TourPoster from "./components/TourPoster.vue";
import StopCreateForm from "./components/StopCreateForm.vue";
import AddStopPopover from "./components/AddStopPopover.vue";
import TourCalendar from "./components/TourCalendar.vue";
import BandLogo from "./components/BandLogo.vue";
import GeolocateFab from "./components/GeolocateFab.vue";

const db = useDb();
const session = useSession();
const canEdit = !!session && !isPublicMode;

function switchView() {
  if (canEdit) {
    window.location.href = window.location.pathname + "?public";
  } else {
    window.location.href = window.location.pathname;
  }
}

ensureData(db, session?.user_id, canEdit).catch((err) =>
  console.error("Failed to ensure data:", err),
);

const selectedStop = ref<StopWithVenue | null>(null);
const posterStop = ref<StopWithVenue | null>(null);
const showLandingPoster = ref(!canEdit);
const showSplash = ref(canEdit);
const sheetOpen = ref(false);
const sheetMode = ref<"detail" | "create">("detail");

const createLat = ref(0);
const createLng = ref(0);

const popoverVisible = ref(false);
const popoverX = ref(0);
const popoverY = ref(0);
const popoverLat = ref(0);
const popoverLng = ref(0);

const today = ref(new Date());
today.value.setHours(0, 0, 0, 0);

// Refresh the date window periodically so the 3-week query doesn't go stale
const dateTimer = setInterval(() => {
  const now = new Date();
  now.setHours(0, 0, 0, 0);
  if (now.getTime() !== today.value.getTime()) {
    today.value = now;
  }
}, 60_000);

const threeWeeks = computed(() => new Date(today.value.getTime() + 21 * 24 * 60 * 60 * 1000));

const stopsQuery = computed(() => {
  const t = today.value;
  const tw = threeWeeks.value;
  const base = app.stops
    .where({ date: { gte: t, lte: tw } })
    .include({ venue: true })
    .orderBy("date", "asc")
    .limit(12);
  const confirmed = app.stops
    .where({ status: "confirmed", date: { gte: t, lte: tw } })
    .include({ venue: true })
    .orderBy("date", "asc")
    .limit(12);
  return canEdit ? base : confirmed;
});
const stopsData = useAll(stopsQuery);

const bandsData = useAll(app.bands.limit(1));

const firstBandId = computed(() => {
  const bands = bandsData.value;
  return bands && bands.length > 0 ? bands[0].id : null;
});

const bandNameDisplay = computed(() => bandsData.value?.[0]?.name ?? null);
const editingBandName = ref(false);
const bandNameInput = ref<HTMLInputElement | null>(null);

function startEditBandName() {
  editingBandName.value = true;
  nextTick(() => bandNameInput.value?.focus());
}

function saveBandName(value: string) {
  editingBandName.value = false;
  const trimmed = value.trim();
  if (!trimmed || trimmed === bandNameDisplay.value) return;
  const bandId = firstBandId.value;
  if (bandId) db.update(app.bands, bandId, { name: trimmed });
}

const landingPosterStops = computed(() => {
  const stops = stopsData.value;
  if (!stops) return [];
  return stops
    .filter((s) => s.venue != null)
    .map((s) => ({
      id: s.id,
      date: s.date instanceof Date ? s.date : new Date(s.date),
      venueName: s.venue!.name,
      city: s.venue!.city,
      country: s.venue!.country,
    }));
});

const calendarStops = computed(() => {
  const stops = stopsData.value;
  if (!stops) return [];
  return stops
    .filter((s) => s.venue != null)
    .map((s) => ({
      id: s.id,
      date: s.date instanceof Date ? s.date : new Date(s.date),
      venue: { name: s.venue!.name },
    }));
});

function selectStop(stop: StopWithVenue) {
  if (!canEdit) {
    posterStop.value = stop;
  } else {
    selectedStop.value = stop;
    sheetMode.value = "detail";
    sheetOpen.value = true;
  }

  if (stop.venue && mapCtrl) {
    mapCtrl.flyTo(
      { lng: stop.venue.lng, lat: stop.venue.lat },
      { zoom: 5, pitch: 40, duration: 1500 },
    );
  }
}

function closeSheet() {
  sheetOpen.value = false;
}

function onSheetClosed() {
  selectedStop.value = null;
  sheetMode.value = "detail";
}

function dismissPopover() {
  popoverVisible.value = false;
}

function onPopoverConfirm() {
  popoverVisible.value = false;
  createLat.value = popoverLat.value;
  createLng.value = popoverLng.value;
  sheetMode.value = "create";
  sheetOpen.value = true;
}

function onCalendarSelectStop(stopId: string) {
  const stops = stopsData.value;
  if (!stops) return;
  const stop = stops.find((s) => s.id === stopId);
  if (stop) selectStop(stop);
}

function onGeolocate(coords: { lat: number; lng: number }) {
  const stops = stopsData.value;
  if (!stops || stops.length === 0) return;

  const stopsWithLocation: StopWithLocation[] = stops
    .filter((s) => s.venue != null)
    .map((s) => ({ id: s.id, lat: s.venue!.lat, lng: s.venue!.lng }));

  const nearest = findNearestStop(coords, stopsWithLocation);
  if (!nearest) return;

  const nearestStop = stops.find((s) => s.id === nearest.id);
  if (!nearestStop?.venue) return;

  mapCtrl?.flyTo(
    { lng: nearestStop.venue.lng, lat: nearestStop.venue.lat },
    { zoom: 6, pitch: 50, bearing: -20, duration: 3000 },
  );

  selectStop(nearestStop);
}

const isTouring = ref(false);

function stopTour() {
  mapCtrl?.stopTour();
  isTouring.value = false;
}

async function startTour() {
  const stops = stopsData.value;
  if (!stops || !mapCtrl) return;

  const sorted = stops
    .filter((s) => s.venue != null)
    .sort((a, b) => {
      const dateA = a.date instanceof Date ? a.date : new Date(a.date);
      const dateB = b.date instanceof Date ? b.date : new Date(b.date);
      return dateA.getTime() - dateB.getTime();
    });

  if (sorted.length === 0) return;

  isTouring.value = true;
  closeSheet();

  const tourStops: StopMapData[] = sorted.map((s) => ({
    id: s.id,
    name: s.venue!.name,
    lng: s.venue!.lng,
    lat: s.venue!.lat,
  }));

  await mapCtrl.tour(tourStops);
  isTouring.value = false;
}

let mapCtrl: MapController | null = null;

onMounted(async () => {
  mapCtrl = new MapController({ container: "map" });

  mapCtrl.on("stopClick", (e) => {
    dismissPopover();
    const stops = stopsData.value;
    const stop = stops?.find((s) => s.id === e.stopId);
    if (stop) selectStop(stop);
  });

  mapCtrl.on("mapClick", (e) => {
    dismissPopover();

    // Reject clicks outside valid Earth coordinates
    if (e.lat < -90 || e.lat > 90 || e.lng < -180 || e.lng > 180) return;

    if (canEdit) {
      popoverX.value = e.x;
      popoverY.value = e.y;
      popoverLat.value = e.lat;
      popoverLng.value = e.lng;
      popoverVisible.value = true;
    } else {
      const stops = stopsData.value;
      if (!stops || stops.length === 0) return;

      const confirmedWithLocation: StopWithLocation[] = stops
        .filter((s) => s.status === "confirmed" && s.venue != null)
        .map((s) => ({ id: s.id, lat: s.venue!.lat, lng: s.venue!.lng }));

      const nearestLoc = findNearestStop({ lat: e.lat, lng: e.lng }, confirmedWithLocation);
      if (!nearestLoc) return;

      const nearestStop = stops.find((s) => s.id === nearestLoc.id);
      if (!nearestStop?.venue) return;

      mapCtrl!.flyTo(
        { lng: nearestStop.venue.lng, lat: nearestStop.venue.lat },
        { zoom: 5, pitch: 45 },
      );

      selectStop(nearestStop);
    }
  });

  await mapCtrl.whenReady();
  renderStops();
  mapCtrl.startRotation();

  const stops = stopsData.value;
  const firstStop = stops?.find((s) => s.venue != null);
  if (firstStop?.venue) {
    mapCtrl.flyTo(
      { lng: firstStop.venue.lng, lat: firstStop.venue.lat },
      { zoom: 4, pitch: 40, duration: 2000 },
    );
  }
});

onUnmounted(() => {
  clearInterval(dateTimer);
  mapCtrl?.destroy();
  mapCtrl = null;
});

watch(
  stopsData,
  () => {
    renderStops();
    if (selectedStop.value) {
      const stops = stopsData.value;
      const updated = stops?.find((s) => s.id === selectedStop.value!.id);
      selectedStop.value = updated ?? null;
    }
  },
  { deep: true },
);

function renderStops() {
  if (!mapCtrl) return;

  const stops = stopsData.value;
  if (!stops) return;

  const withVenue = stops.filter((stop) => stop.venue != null);

  const stopMapData: StopMapData[] = withVenue.map((stop) => ({
    id: stop.id,
    name: stop.venue!.name,
    lng: stop.venue!.lng,
    lat: stop.venue!.lat,
  }));

  const dates = withVenue.map((stop) =>
    stop.date instanceof Date ? stop.date : new Date(stop.date),
  );

  mapCtrl.setStops(stopMapData, dates);
}
</script>

<style>
@import "./styles/app.css";
</style>
