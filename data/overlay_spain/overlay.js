const DATA_URL = "./overlay_data.json";
const FETCH_INTERVAL_MS = 10_000;
const SCROLL_INTERVAL_MS = 15_000;
const MAX_VISIBLE_ALERTS = 3;
const ITEM_HEIGHT_PX = 64;
const ITEM_GAP_PX = 8;
const STEP_PX = ITEM_HEIGHT_PX + ITEM_GAP_PX;
const SCROLL_ANIMATION_MS = 650;

const MANAGEMENT_LABELS = {
  roadClosed: "Road Closed",
  laneClosures: "Lane Closed",
  narrowLanes: "Narrow Lanes",
  singleAlternateLineTraffic: "Alternating Traffic",
  newRoadworksLayout: "Modified Layout",
};

const CAUSE_LABELS = {
  accident: "Accident",
  vehicleObstruction: "Disabled Vehicle",
  obstruction: "Obstruction on Road",
  environmentalObstruction: "Environmental Hazard",
  infrastructureDamageObstruction: "Damaged Infrastructure",
  roadMaintenance: "Roadworks",
  abnormalTraffic: "Heavy Traffic",
  roadOrCarriagewayOrLaneManagement: "Traffic Management Active",
  poorEnvironment: "Poor Weather",
};

const ALERT_TRANSLATIONS = {
  // Pure Causes (No specific management active)
  None_accident_None: "Accident",
  None_vehicleObstruction_None: "Disabled Vehicle",
  None_obstruction_None: "Obstruction on Road",
  None_environmentalObstruction_None: "Environmental Hazard",
  None_infrastructureDamageObstruction_None: "Damaged Infrastructure",
  None_roadMaintenance_roadworks: "Roadworks",
  None_abnormalTraffic_None: "Heavy Traffic",
  None_roadOrCarriagewayOrLaneManagement_None: "Traffic Management Active",

  // Road / Carriageway Closures
  roadClosed_roadMaintenance_roadworks: "Road Closed (Roadworks)",
  roadClosed_environmentalObstruction_None: "Road Closed (Hazard)",
  roadClosed_poorEnvironment_None: "Road Closed (Poor Weather)",
  roadClosed_infrastructureDamageObstruction_None: "Road Closed (Damaged Road)",
  roadClosed_roadOrCarriagewayOrLaneManagement_None: "Road Closed",
  carriagewayClosures_roadMaintenance_roadworks: "Direction Closed (Roadworks)",
  carriagewayClosures_environmentalObstruction_None: "Direction Closed (Hazard)",
  carriagewayClosures_infrastructureDamageObstruction_None: "Direction Closed (Damaged Road)",

  // Lane Closures
  laneClosures_accident_None: "Lane Closed (Accident)",
  laneClosures_vehicleObstruction_None: "Lane Closed (Disabled Vehicle)",
  laneClosures_roadMaintenance_roadworks: "Lane Closed (Roadworks)",
  laneClosures_obstruction_None: "Lane Closed (Obstruction)",
  laneClosures_environmentalObstruction_None: "Lane Closed (Hazard)",
  laneClosures_infrastructureDamageObstruction_None: "Lane Closed (Damaged Road)",
  laneClosures_roadOrCarriagewayOrLaneManagement_None: "Lane Closed",

  // Intermittent / Alternating Traffic
  intermittentShortTermClosures_roadMaintenance_roadworks: "Intermittent Closures (Roadworks)",
  intermittentShortTermClosures_environmentalObstruction_None: "Intermittent Closures (Hazard)",
  singleAlternateLineTraffic_roadMaintenance_roadworks: "Alternating Traffic (Roadworks)",
  singleAlternateLineTraffic_environmentalObstruction_None: "Alternating Traffic (Hazard)",

  // Narrowing & Deviations
  narrowLanes_accident_None: "Narrow Lanes (Accident)",
  narrowLanes_vehicleObstruction_None: "Narrow Lanes (Disabled Vehicle)",
  narrowLanes_roadMaintenance_roadworks: "Narrow Lanes (Roadworks)",
  narrowLanes_environmentalObstruction_None: "Narrow Lanes (Hazard)",
  lanesDeviated_roadMaintenance_roadworks: "Lanes Deviated (Roadworks)",
  lanesDeviated_infrastructureDamageObstruction_None: "Lanes Deviated (Damaged Road)",
  lanesDeviated_environmentalObstruction_None: "Lanes Deviated (Hazard)",
  newRoadworksLayout_roadMaintenance_roadworks: "Modified Layout (Roadworks)",

  // Specific Usage Restrictions
  useOfSpecifiedLanesOrCarriagewaysAllowed_roadMaintenance_roadworks:
    "Restricted Lane Usage (Roadworks)",
  useOfSpecifiedLanesOrCarriagewaysAllowed_environmentalObstruction_None:
    "Restricted Lane Usage (Hazard)",
  useOfSpecifiedLanesOrCarriagewaysAllowed_roadOrCarriagewayOrLaneManagement_None:
    "Restricted Lane Usage",
  doNotUseSpecifiedLanesOrCarriageways_roadMaintenance_roadworks: "Lane Blocked (Roadworks)",
  doNotUseSpecifiedLanesOrCarriageways_environmentalObstruction_None: "Lane Blocked (Hazard)",
  doNotUseSpecifiedLanesOrCarriageways_roadOrCarriagewayOrLaneManagement_None: "Lane Blocked",

  // Freight / Weights
  weightRestrictionInOperation_roadOrCarriagewayOrLaneManagement_None: "Weight Restriction Active",
};

const alertsListEl = document.getElementById("alerts-list");
const statusLineEl = document.getElementById("status-line");

let allAlerts = [];
let scrollIndex = 0;
let scrollTimer = null;
let lastAlertSignature = "";
let isAnimating = false;

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
  return hasKm || hasMuni;
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
  if (allAlerts.length <= MAX_VISIBLE_ALERTS) return allAlerts;
  const start = scrollIndex % allAlerts.length;
  const end = start + MAX_VISIBLE_ALERTS + extra;
  if (end <= allAlerts.length) return allAlerts.slice(start, end);
  return allAlerts.slice(start).concat(allAlerts.slice(0, end - allAlerts.length));
}

function renderWindow() {
  const extra = allAlerts.length > MAX_VISIBLE_ALERTS ? 1 : 0;
  render(visibleSlice(extra));
}

function scrollDown() {
  if (!allAlerts.length || allAlerts.length <= MAX_VISIBLE_ALERTS || isAnimating) return;

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

async function loadAlerts() {
  try {
    const response = await fetch(`${DATA_URL}?t=${Date.now()}`, { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const payload = await response.json();
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

loadAlerts();
setInterval(loadAlerts, FETCH_INTERVAL_MS);
