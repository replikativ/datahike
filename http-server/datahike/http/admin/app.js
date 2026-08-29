"use strict";

const byId = (id) => document.getElementById(id);
const tokenInput = byId("token");
const authStatus = byId("auth-status");
const catalogStatus = byId("catalog-status");
const databaseGrid = byId("databases");
const databaseCount = byId("database-count");

tokenInput.value = sessionStorage.getItem("datahike.admin.token") || "";

function headers() {
  const result = { Accept: "application/json" };
  const token = tokenInput.value.trim();
  if (token) result.Authorization = `token ${token}`;
  return result;
}

async function request(path, type = "json", authenticated = true) {
  const response = await fetch(path, {
    cache: "no-store",
    credentials: "same-origin",
    headers: authenticated ? headers() : { Accept: "text/plain" }
  });
  if (!response.ok) {
    const error = new Error(`${response.status} ${response.statusText}`);
    error.status = response.status;
    throw error;
  }
  return type === "json" ? response.json() : response.text();
}

function setText(id, value) {
  byId(id).textContent = value == null || value === "" ? "—" : String(value);
}

function taggedValue(value) {
  return Array.isArray(value) && value.length === 2 && typeof value[0] === "string" && value[0].startsWith("!")
    ? value[1]
    : value;
}

function displayDate(value) {
  const unpacked = taggedValue(value);
  if (unpacked == null || unpacked === "") return null;
  const millis = Number(unpacked);
  const date = new Date(Number.isFinite(millis) ? millis : unpacked);
  return Number.isNaN(date.getTime()) ? String(unpacked) : date.toLocaleString();
}

function field(label, value) {
  const row = document.createElement("div");
  const term = document.createElement("dt");
  const detail = document.createElement("dd");
  term.textContent = label;
  detail.textContent = value == null || value === "" ? "—" : String(value);
  row.append(term, detail);
  return row;
}

function renderDatabases(databases) {
  databaseGrid.replaceChildren();
  databaseCount.textContent = String(databases.length);
  if (databases.length === 0) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = "No active databases are visible to this principal.";
    databaseGrid.append(empty);
    return;
  }

  for (const database of databases) {
    const card = document.createElement("article");
    card.className = "database";
    const title = document.createElement("h3");
    title.textContent = database.name || "Unnamed database";
    const backend = document.createElement("span");
    backend.className = "backend";
    backend.textContent = taggedValue(database.config?.store?.backend) || "unknown backend";
    const details = document.createElement("dl");
    details.append(
      field("Store ID", database["store-id"]),
      field("State", String(taggedValue(database.state) || "active").replace(/^:/, "")),
      field("Created", displayDate(database["created-at"])),
      field("Principal", database["created-by"])
    );
    card.append(title, backend, details);
    databaseGrid.append(card);
  }
}

async function refreshReadiness() {
  try {
    const readiness = (await request("/health/ready", "text", false)).trim();
    setText("ready", readiness);
    byId("ready-dot").className = "dot ready";
  } catch (_error) {
    setText("ready", "Not ready");
    byId("ready-dot").className = "dot failed";
  }
}

async function refresh() {
  authStatus.className = "status";
  catalogStatus.className = "status";
  authStatus.textContent = "Connecting…";
  catalogStatus.textContent = "Loading visible databases…";
  await refreshReadiness();

  try {
    const version = await request("/version");
    setText("version", version["datahike-version"]);
    setText("git-sha", version["git-sha"]?.slice(0, 12));
    setText("auth-title", "Connected to this server");
    authStatus.textContent = "Authenticated.";
  } catch (error) {
    setText("version", null);
    setText("git-sha", null);
    setText("auth-title", "Authenticate to inspect the catalog");
    authStatus.className = "status error";
    authStatus.textContent = error.status === 401 ? "Authentication required or token rejected." : `Version request failed: ${error.message}`;
    catalogStatus.textContent = "Authenticate to load the databases visible to you.";
    renderDatabases([]);
    return;
  }

  try {
    const databases = await request("/databases");
    renderDatabases(databases);
    catalogStatus.textContent = `${databases.length} active database${databases.length === 1 ? "" : "s"} visible to this principal.`;
  } catch (error) {
    renderDatabases([]);
    catalogStatus.className = "status error";
    catalogStatus.textContent = error.status === 404
      ? "The system catalog is not configured on this server."
      : `Catalog request failed: ${error.message}`;
  }
}

byId("auth-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const token = tokenInput.value.trim();
  if (token) sessionStorage.setItem("datahike.admin.token", token);
  else sessionStorage.removeItem("datahike.admin.token");
  refresh();
});

byId("clear-token").addEventListener("click", () => {
  sessionStorage.removeItem("datahike.admin.token");
  tokenInput.value = "";
  refresh();
});

byId("refresh").addEventListener("click", refresh);
refresh();
