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
  laneClosures: "Lane Closures",
  narrowLanes: "Narrow Lanes",
  singleAlternateLineTraffic: "Alternating Traffic",
  contraflow: "Contraflow",
  closedPermanentlyForTheWinter: "Closed for Winter",
  weightRestrictionInOperation: "Weight Restriction",
};

const CAUSE_LABELS = {
  accident: "Accident",
  obstruction: "Obstruction",
  roadManagement: "Road Management",
  MaintenanceWorks: "Maintenance",
  ConstructionWorks: "Construction Works",
  InfrastructureDamageObstruction: "Infrastructure Damage",
  AnimalPresenceObstruction: "Animal on Road",
  GeneralObstruction: "Obstruction",
  roadsideServiceDisruption: "Service Disruption",
  ReroutingManagement: "Rerouting",
  GeneralNetworkManagement: "Network Management",
  GeneralInstructionOrMessageToRoadUsers: "Road User Information",
  OperatorAction: "Operator Action",
  PublicEvent: "Public Event",
  SpeedManagement: "Speed Management",
};

const ALERT_TRANSLATIONS = {
  // Road Management Actions
  roadClosed_roadManagement_roadClosed: "Road Closed",
  laneClosures_roadManagement_laneClosures: "Lane Closures",
  narrowLanes_roadManagement_narrowLanes: "Narrow Lanes",
  singleAlternateLineTraffic_roadManagement_singleAlternateLineTraffic: "Alternating Traffic",
  contraflow_roadManagement_contraflow: "Contraflow Active",
  closedPermanentlyForTheWinter_roadManagement_closedPermanentlyForTheWinter: "Closed for Winter",
  weightRestrictionInOperation_roadManagement_weightRestrictionInOperation: "Weight Restriction Active",

  // Accidents & Obstructions
  None_accident_accident: "Accident",
  None_obstruction_brokenDownVehicle: "Broken Down Vehicle",
  None_GeneralObstruction_GeneralObstruction: "Obstruction on Road",
  None_InfrastructureDamageObstruction_InfrastructureDamageObstruction: "Infrastructure Damage",
  None_AnimalPresenceObstruction_AnimalPresenceObstruction: "Animal on Road",

  // Maintenance Works
  None_MaintenanceWorks_roadworks: "Roadworks",
  None_MaintenanceWorks_resurfacingWork: "Resurfacing",
  None_MaintenanceWorks_repairWork: "Repair",
  None_MaintenanceWorks_maintenanceWork: "Maintenance",
  None_MaintenanceWorks_roadsideWork: "Roadside Work",
  None_MaintenanceWorks_grassCuttingWork: "Grass Cutting",
  None_MaintenanceWorks_roadMarkingWork: "Road Marking",

  // Construction
  None_ConstructionWorks_ConstructionWorks: "Construction",

  // Service Disruptions
  None_roadsideServiceDisruption_petrolShortage: "Fuel Shortage",
  None_roadsideServiceDisruption_serviceAreaClosed: "Service Area Closed",

  // Network & Traffic Management
  None_ReroutingManagement_ReroutingManagement: "Rerouting Active",
  None_GeneralNetworkManagement_GeneralNetworkManagement: "Network Management",
  None_GeneralInstructionOrMessageToRoadUsers_GeneralInstructionOrMessageToRoadUsers: "Road User Information",
  None_OperatorAction_OperatorAction: "Operator Action",
  None_SpeedManagement_SpeedManagement: "Speed Restriction",

  // Events
  None_PublicEvent_PublicEvent: "Public Event",
};

const SEVERITY_ORDER = { highest: 4, high: 3, medium: 2, low: 1 };

function mergeAlertsBySituation(alerts) {
  const groups = new Map();
  for (const alert of alerts) {
    const key = alert.situation_id;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(alert);
  }

  const merged = [];
  for (const [, records] of groups) {
    if (records.length === 1) {
      merged.push(records[0]);
      continue;
    }

    const mgmtRecord = records.find((r) => r.management_type);
    const causeRecord = records.find(
      (r) => !r.management_type && r.cause_type && r.cause_type !== "roadManagement",
    );
    const base = { ...(mgmtRecord || records[0]) };

    if (causeRecord && causeRecord !== mgmtRecord) {
      base.cause_type = causeRecord.cause_type;
      base.detailed_cause_type = causeRecord.detailed_cause_type;
    }

    for (const r of records) {
      const rSev = SEVERITY_ORDER[(r.severity || "").toLowerCase()] || 0;
      const bSev = SEVERITY_ORDER[(base.severity || "").toLowerCase()] || 0;
      if (rSev > bSev) base.severity = r.severity;
    }

    if (!base.end_time) {
      const ends = records.map((r) => r.end_time).filter(Boolean);
      if (ends.length) base.end_time = ends.sort().at(-1);
    }

    merged.push(base);
  }
  return merged;
}

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
  const hasAlertc = !!alert.location_from?.alertc_location_name || !!alert.location_to?.alertc_location_name;
  return hasKm || hasAlertc;
}

function formatLocation(alert) {
  const fromKm = formatKm(alert.location_from?.km_point);
  const toKm = formatKm(alert.location_to?.km_point);
  const fromAlertc = alert.location_from?.alertc_location_name || null;
  const toAlertc = alert.location_to?.alertc_location_name || null;

  if (fromKm && toKm) {
    const names = [...new Set([fromAlertc, toAlertc].filter(Boolean))];
    const near = names.length ? ` (${names.join("/")})` : "";
    return `between km ${fromKm} - ${toKm}${near}`;
  }

  const km = fromKm || toKm;
  const alertc = fromAlertc || toAlertc;

  if (km && alertc) return `at km ${km} (${alertc})`;
  if (km) return `at km ${km}`;
  if (alertc) return `near ${alertc}`;
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

    if (alert._record_count > 1) {
      const badge = document.createElement("span");
      badge.className = "record-count";
      badge.textContent = alert._record_count;
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
    const rawAlerts = Array.isArray(payload.alerts) ? payload.alerts : [];
    const incomingAlerts = mergeAlertsBySituation(rawAlerts).filter(hasLocationInfo);
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
