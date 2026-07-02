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
  contraflow: "Contraflow",
  narrowLanes: "Narrow Lanes",
  newRoadworksLayout: "Modified Layout",
  roadClosed: "Road Closed",
  singleAlternateLineTraffic: "Alternating Traffic",
};

const CAUSE_LABELS = {
  abnormalTraffic: "Heavy Traffic",
  Accident: "Accident",
  ReroutingManagement: "Rerouting",
  roadMaintenance: "Roadworks",
  roadOrCarriagewayOrLaneManagement: "Traffic Management Active",
  vehicleObstruction: "Disabled Vehicle",
};

const DETAIL_LABELS = {
  accident: "Accident",
  accidentInvolvingHeavyLorries: "Lorry Accident",
  brokenDownVehicle: "Disabled Vehicle",
  contraflow: "Contraflow",
  narrowLanes: "Narrow Lanes",
  newRoadworksLayout: "Modified Layout",
  queuingTraffic: "Queuing Traffic",
  roadClosed: "Road Closed",
  roadworks: "Roadworks",
  roadsideWork: "Roadside Work",
  singleAlternateLineTraffic: "Alternating Traffic",
  slowTraffic: "Slow Traffic",
  stationaryTraffic: "Stationary Traffic",
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
  if (key === "medium" || key === "") return "severity-medium";
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

function normalizeLookupPart(value) {
  if (value === null || value === undefined) return "None";
  const text = String(value).trim();
  return text ? text : "None";
}

function locationRoad(location) {
  return location?.alertc_road_number || location?.alertc_road_name || "";
}

function locationName(location) {
  return location?.alertc_location_name || "";
}

function formatRoad(alert) {
  return alert.road_name || locationRoad(alert.location_from) || locationRoad(alert.location_to) || "BE";
}

function formatLocation(alert) {
  if (alert.road_destination) return alert.road_destination;

  const fromName = locationName(alert.location_from);
  const toName = locationName(alert.location_to);

  if (fromName && toName && fromName !== toName) return `${fromName} -> ${toName}`;
  if (fromName || toName) return `near ${fromName || toName}`;
  return "";
}

function formatEventType(alert) {
  const managementType = normalizeLookupPart(alert.management_type);
  const causeType = normalizeLookupPart(alert.cause_type);
  const detailedCauseType = normalizeLookupPart(alert.detailed_cause_type);
  const action = managementType === "None" ? "" : MANAGEMENT_LABELS[managementType] || "Restriction";
  const cause = CAUSE_LABELS[causeType] || (causeType === "None" ? "" : causeType);
  const detail = DETAIL_LABELS[detailedCauseType] || "";

  if (action && detail && action !== detail) return `${action} (${detail})`;
  if (action && cause && cause !== "Traffic Management Active") return `${action} (${cause})`;
  if (action) return action;
  if (detail) return detail;
  if (cause) return cause;
  return "Traffic Alert";
}

function formatAlertText(alert) {
  const road = formatRoad(alert);
  const eventType = formatEventType(alert);
  const location = formatLocation(alert);
  const until = alert.end_time ? `until ${toLocalShort(alert.end_time)}` : "";
  const main = `${road} ${eventType}`;
  const sub = [location, until].filter(Boolean).join(" ");
  return { main, sub, safetyRelated: alert.safety_related_message === true };
}

function render(alerts) {
  alertsListEl.innerHTML = "";
  const track = document.createElement("div");
  track.className = "alerts-track";

  for (const alert of alerts) {
    const { main, sub, safetyRelated } = formatAlertText(alert);
    const item = document.createElement("div");
    item.className = `alert-item ${severityClass(alert.severity)}`;

    const mainEl = document.createElement("div");
    mainEl.className = "alert-main";
    mainEl.textContent = main;

    if (safetyRelated) {
      const badge = document.createElement("span");
      badge.className = "safety-badge";
      badge.textContent = "Safety";
      mainEl.appendChild(badge);
    }

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
    const incomingAlerts = Array.isArray(payload.alerts) ? payload.alerts : [];
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
