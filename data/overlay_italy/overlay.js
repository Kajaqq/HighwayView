const DATA_URL = "./overlay_data.json";
const SCRIPT_DATA_URL = "./overlay_data.js";
const FETCH_INTERVAL_MS = 10_000;
const SCROLL_INTERVAL_MS = 15_000;
const DEFAULT_VISIBLE_ALERTS = 3;
const ITEM_HEIGHT_PX = 64;
const ITEM_GAP_PX = 8;
const STEP_PX = ITEM_HEIGHT_PX + ITEM_GAP_PX;
const SCROLL_ANIMATION_MS = 650;

const MANAGEMENT_LABELS = {
  roadClosed: "Road Closed",
  singleAlternateLineTraffic: "Alternating Traffic",
  narrowLanes: "Narrow Lanes",
  roadDeviation: "Deviation",
  congestion: "Queues",
  slowTraffic: "Slow Traffic",
  stationaryTraffic: "Traffic Stopped",
};

const CAUSE_LABELS = {
  roadMaintenance: "Roadworks",
  accident: "Accident",
  infrastructureDamageObstruction: "Damaged Infrastructure",
  poorWeatherConditions: "Poor Weather",
  animalPresence: "Animal on Road",
  vehicleObstruction: "Disabled Vehicle",
  abnormalTraffic: "Heavy Traffic",
};

const ALERT_TRANSLATIONS = {
  // Road Closures
  "roadClosed_roadMaintenance_tratto chiuso": "Road Closed (Roadworks)",
  "roadClosed_roadMaintenance_chiuso": "Road Closed (Roadworks)",
  "roadClosed_roadMaintenance_chiusura rampa": "Ramp Closed (Roadworks)",
  "roadClosed_accident_tratto chiuso": "Road Closed (Accident)",
  "roadClosed_accident_chiuso": "Road Closed (Accident)",
  "roadClosed_infrastructureDamageObstruction_tratto chiuso": "Road Closed (Damaged Road)",
  "roadClosed_infrastructureDamageObstruction_chiuso": "Road Closed (Damaged Road)",
  "roadClosed_poorWeatherConditions_tratto chiuso": "Road Closed (Poor Weather)",
  "roadClosed_poorWeatherConditions_chiuso": "Road Closed (Poor Weather)",
  "roadClosed_animalPresence_tratto chiuso": "Road Closed (Animal)",
  "roadClosed_vehicleObstruction_tratto chiuso": "Road Closed (Disabled Vehicle)",
  roadClosed_None_None: "Road Closed",
  "roadClosed_roadMaintenance_None": "Road Closed (Roadworks)",
  "roadClosed_accident_None": "Road Closed (Accident)",

  // Alternating Traffic
  "singleAlternateLineTraffic_roadMaintenance_senso unico alternato": "Alternating Traffic (Roadworks)",
  "singleAlternateLineTraffic_accident_senso unico alternato": "Alternating Traffic (Accident)",
  "singleAlternateLineTraffic_infrastructureDamageObstruction_senso unico alternato": "Alternating Traffic (Damaged Road)",
  singleAlternateLineTraffic_None_None: "Alternating Traffic",
  "singleAlternateLineTraffic_roadMaintenance_None": "Alternating Traffic (Roadworks)",

  // Narrow Lanes
  "narrowLanes_roadMaintenance_restringimento carreggiata": "Narrow Lanes (Roadworks)",
  "narrowLanes_roadMaintenance_carreggiata ridotta": "Narrow Lanes (Roadworks)",
  "narrowLanes_accident_restringimento carreggiata": "Narrow Lanes (Accident)",
  "narrowLanes_accident_carreggiata ridotta": "Narrow Lanes (Accident)",
  "narrowLanes_infrastructureDamageObstruction_restringimento carreggiata": "Narrow Lanes (Damaged Road)",
  "narrowLanes_vehicleObstruction_restringimento carreggiata": "Narrow Lanes (Disabled Vehicle)",
  narrowLanes_None_None: "Narrow Lanes",
  "narrowLanes_roadMaintenance_None": "Narrow Lanes (Roadworks)",

  // Deviation
  "roadDeviation_roadMaintenance_deviazione": "Deviation (Roadworks)",
  "roadDeviation_accident_deviazione": "Deviation (Accident)",
  "roadDeviation_infrastructureDamageObstruction_deviazione": "Deviation (Damaged Road)",
  roadDeviation_None_None: "Deviation",
  "roadDeviation_roadMaintenance_None": "Deviation (Roadworks)",

  // Queues / Congestion
  "congestion_abnormalTraffic_code": "Queues",
  "congestion_abnormalTraffic_code a tratti": "Intermittent Queues",
  "congestion_accident_code": "Queues (Accident)",
  "congestion_accident_code a tratti": "Intermittent Queues (Accident)",
  "congestion_roadMaintenance_code": "Queues (Roadworks)",
  "congestion_roadMaintenance_code a tratti": "Intermittent Queues (Roadworks)",
  "congestion_vehicleObstruction_code": "Queues (Disabled Vehicle)",
  congestion_None_None: "Queues",

  // Slow Traffic
  "slowTraffic_abnormalTraffic_traffico rallentato": "Slow Traffic",
  "slowTraffic_accident_traffico rallentato": "Slow Traffic (Accident)",
  "slowTraffic_roadMaintenance_traffico rallentato": "Slow Traffic (Roadworks)",
  "slowTraffic_vehicleObstruction_traffico rallentato": "Slow Traffic (Disabled Vehicle)",
  slowTraffic_None_None: "Slow Traffic",

  // Stationary Traffic
  "stationaryTraffic_abnormalTraffic_traffico bloccato": "Traffic Stopped",
  "stationaryTraffic_accident_traffico bloccato": "Traffic Stopped (Accident)",
  "stationaryTraffic_vehicleObstruction_traffico bloccato": "Traffic Stopped (Disabled Vehicle)",
  stationaryTraffic_None_None: "Traffic Stopped",

  // No management type — cause only
  None_roadMaintenance_None: "Roadworks",
  None_accident_None: "Accident",
  None_infrastructureDamageObstruction_None: "Damaged Infrastructure",
  None_poorWeatherConditions_None: "Poor Weather",
  None_animalPresence_None: "Animal on Road",
  None_vehicleObstruction_None: "Disabled Vehicle",
  None_abnormalTraffic_None: "Heavy Traffic",
};

const overlayContainerEl = document.getElementById("overlay-container");
const headerEl = document.getElementById("header");
const alertsListEl = document.getElementById("alerts-list");
const statusLineEl = document.getElementById("status-line");

let allAlerts = [];
let scrollIndex = 0;
let scrollTimer = null;
let lastAlertSignature = "";
let isAnimating = false;
let visibleAlertCount = DEFAULT_VISIBLE_ALERTS;

function parseHeightParam() {
  const rawHeight = new URLSearchParams(window.location.search).get("height");
  if (!rawHeight) return null;

  const height = Number.parseInt(rawHeight, 10);
  return Number.isFinite(height) && height > 0 ? height : null;
}

function readPx(style, property) {
  const value = Number.parseFloat(style[property]);
  return Number.isFinite(value) ? value : 0;
}

function alertSlotsForHeight(height) {
  const containerStyle = window.getComputedStyle(overlayContainerEl);
  const paddingY = readPx(containerStyle, "paddingTop") + readPx(containerStyle, "paddingBottom");
  const borderY = readPx(containerStyle, "borderTopWidth") + readPx(containerStyle, "borderBottomWidth");
  const rowGap = readPx(containerStyle, "rowGap") || readPx(containerStyle, "gap");
  const chromeHeight = headerEl.offsetHeight + statusLineEl.offsetHeight + paddingY + borderY + rowGap * 2;
  const availableListHeight = height - chromeHeight;

  return Math.max(1, Math.floor((availableListHeight + ITEM_GAP_PX) / STEP_PX));
}

function setVisibleAlertCount(count) {
  visibleAlertCount = count;
  const listHeight = ITEM_HEIGHT_PX * count + ITEM_GAP_PX * (count - 1);
  alertsListEl.style.setProperty("--alerts-list-height", `${listHeight}px`);
}

function applyHeightParam() {
  const height = parseHeightParam();
  if (!height) {
    setVisibleAlertCount(DEFAULT_VISIBLE_ALERTS);
    return;
  }

  overlayContainerEl.style.setProperty("--overlay-height", `${height}px`);
  overlayContainerEl.style.setProperty("--overlay-min-height", `${height}px`);
  setVisibleAlertCount(alertSlotsForHeight(height));
}

function severityClass(severity) {
  const key = (severity || "").toLowerCase();
  if (key === "high" || key === "highest") return "severity-high";
  if (key === "medium") return "severity-medium";
  return "severity-low";
}

function toLocalShort(isoString) {
  if (!isoString) return "Active now";
  const date = new Date(isoString);
  if (Number.isNaN(date.valueOf())) return "Active now";
  const day = String(date.getDate()).padStart(2, "0");
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  return `${day}/${month} ${hours}:${minutes}`;
}

function formatKm(km) {
  if (km === null || km === undefined) return null;
  return Number(km).toFixed(1).replace(/\.0$/, "");
}

function hasLocationInfo(alert) {
  const hasKm = alert.location_from?.km_point != null || alert.location_to?.km_point != null;
  const hasMuni = !!alert.location_from?.municipality || !!alert.location_to?.municipality;
  const hasCoords =
    (alert.location_from?.latitude != null && alert.location_from?.longitude != null) ||
    (alert.location_to?.latitude != null && alert.location_to?.longitude != null);
  return hasKm || hasMuni || hasCoords;
}

function formatLocation(alert) {
  const fromKm = formatKm(alert.location_from?.km_point);
  const toKm = formatKm(alert.location_to?.km_point);
  const fromMuni = alert.location_from?.municipality || null;
  const toMuni = alert.location_to?.municipality || null;

  if (fromKm && toKm) {
    const names = [...new Set([fromMuni, toMuni].filter(Boolean))];
    const near = names.length ? ` (${names.join(" – ")})` : "";
    return `between km ${fromKm} - ${toKm}${near}`;
  }

  const km = fromKm || toKm;
  const muni = fromMuni || toMuni;

  if (km && muni) return `at km ${km} (${muni})`;
  if (km) return `at km ${km}`;
  if (muni) return `near ${muni}`;

  const lat = alert.location_from?.latitude ?? alert.location_to?.latitude;
  const lon = alert.location_from?.longitude ?? alert.location_to?.longitude;
  if (lat != null && lon != null) return `${lat.toFixed(4)}°N ${lon.toFixed(4)}°E`;
  return "";
}

function normalizeLookupPart(value) {
  if (value === null || value === undefined) return "None";
  const text = String(value).trim();
  return text ? text : "None";
}

function formatEventType(alert) {
  const managementType = normalizeLookupPart(alert.management_type);
  const causeType = normalizeLookupPart(alert.cause_type);
  const detailedCauseType = normalizeLookupPart(alert.detailed_cause_type);
  const lookupKey = `${managementType}_${causeType}_${detailedCauseType}`;
  const translated = ALERT_TRANSLATIONS[lookupKey];
  if (translated) return translated;

  // Fallback: action first, cause second. If no action, show cause only.
  const action = managementType === "None" ? "" : MANAGEMENT_LABELS[managementType] || "Restriction";
  const cause = CAUSE_LABELS[causeType] || (causeType === "None" ? "" : causeType);

  if (action && cause) return `${action} (${cause})`;
  if (action) return action;
  if (cause) return cause;
  return "Traffic Alert";
}

function formatAlertText(alert) {
  const road = alert.road_name || "Unknown road";
  const eventType = formatEventType(alert);
  const location = formatLocation(alert);
  const until = alert.end_time ? `until ${toLocalShort(alert.end_time)}` : "";
  const main = `${road} ${eventType}`;
  const sub = [location, until].filter(Boolean).join(" ");
  return { main, sub };
}

function render(alerts) {
  alertsListEl.innerHTML = "";
  const track = document.createElement("div");
  track.className = "alerts-track";

  for (const alert of alerts) {
    const { main, sub } = formatAlertText(alert);
    const item = document.createElement("div");
    item.className = `alert-item ${severityClass(alert.severity)}`;

    const mainEl = document.createElement("div");
    mainEl.className = "alert-main";
    mainEl.textContent = main;
    item.appendChild(mainEl);

    const subEl = document.createElement("div");
    subEl.className = "alert-sub";
    subEl.textContent = sub;
    item.appendChild(subEl);

    track.appendChild(item);
  }
  alertsListEl.appendChild(track);
}

function visibleSlice(extra = 0) {
  if (allAlerts.length <= visibleAlertCount) return allAlerts;
  const start = scrollIndex % allAlerts.length;
  const end = start + visibleAlertCount + extra;
  if (end <= allAlerts.length) return allAlerts.slice(start, end);
  return allAlerts.slice(start).concat(allAlerts.slice(0, end - allAlerts.length));
}

function renderWindow() {
  const extra = allAlerts.length > visibleAlertCount ? 1 : 0;
  render(visibleSlice(extra));
}

function scrollDown() {
  if (!allAlerts.length || allAlerts.length <= visibleAlertCount || isAnimating) return;

  const track = alertsListEl.querySelector(".alerts-track");
  if (!track) {
    renderWindow();
    return;
  }

  isAnimating = true;
  track.style.transition = `transform ${SCROLL_ANIMATION_MS}ms ease`;
  track.style.transform = `translateY(-${STEP_PX}px)`;

  setTimeout(() => {
    scrollIndex = (scrollIndex + 1) % allAlerts.length;
    renderWindow();
    isAnimating = false;
  }, SCROLL_ANIMATION_MS + 20);
}

function setScrollTimer() {
  if (scrollTimer) clearInterval(scrollTimer);
  scrollTimer = setInterval(scrollDown, SCROLL_INTERVAL_MS);
}

function loadScriptPayload() {
  return new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.async = true;
    script.src = `${SCRIPT_DATA_URL}?t=${Date.now()}`;
    script.onload = () => {
      script.remove();
      if (window.OVERLAY_DATA && typeof window.OVERLAY_DATA === "object") {
        resolve(window.OVERLAY_DATA);
        return;
      }
      reject(new Error("Overlay data script did not define data"));
    };
    script.onerror = () => {
      script.remove();
      reject(new Error("Overlay data script unavailable"));
    };
    window.OVERLAY_DATA = null;
    document.head.appendChild(script);
  });
}

async function loadJsonPayload() {
  const response = await fetch(`${DATA_URL}?t=${Date.now()}`, { cache: "no-store" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

async function loadPayload() {
  try {
    return await loadScriptPayload();
  } catch (scriptError) {
    if (window.location.protocol === "file:") throw scriptError;
    return loadJsonPayload();
  }
}

async function loadAlerts() {
  try {
    const payload = await loadPayload();
    const incomingAlerts = (Array.isArray(payload.alerts) ? payload.alerts : []).filter(hasLocationInfo);
    const signature = incomingAlerts
      .map((a) => `${a.record_id || ""}:${a.version_time || a.creation_time || a.start_time || ""}`)
      .join("|");
    const hasChanged = signature !== lastAlertSignature;

    allAlerts = incomingAlerts;
    if (hasChanged) {
      lastAlertSignature = signature;
      scrollIndex = 0;
      renderWindow();
      setScrollTimer();
    }
    statusLineEl.textContent = `Live: ${allAlerts.length} alerts`;
  } catch (error) {
    statusLineEl.textContent = `Overlay data unavailable (${error.message})`;
  }
}

applyHeightParam();
loadAlerts().catch(console.error);
setInterval(loadAlerts, FETCH_INTERVAL_MS);
