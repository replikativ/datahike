"use strict";

const byId = (id) => document.getElementById(id);
const tokenInput = byId("token");
const authStatus = byId("auth-status");
const catalogStatus = byId("catalog-status");
const databaseGrid = byId("databases");
const databaseCount = byId("database-count");
const databaseQuery = byId("database-query");
const pageSize = 24;
let pageOffset = 0;
let searchQuery = "";

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

function number(value) {
  return Number(value || 0).toLocaleString();
}

function milliseconds(value) {
  return value == null ? null : `${Number(value).toFixed(value < 10 ? 2 : 1)} ms`;
}

function hitRate(dispositions) {
  const hits = Number(dispositions?.hit || 0);
  const misses = Number(dispositions?.miss || 0);
  return hits + misses === 0 ? null : `${((100 * hits) / (hits + misses)).toFixed(1)}%`;
}

function renderNodeActivity(node) {
  const section = byId("node-activity");
  section.hidden = !node;
  if (!node) return;
  setText("sampled-queries", number(node["sampled-queries"]));
  setText("average-query", milliseconds(node["average-query-ms"]));
  setText("result-cache-hit-rate", hitRate(node["result-cache"]));
  setText("plan-cache-hit-rate", hitRate(node["plan-cache"]));
  setText("query-errors", number(node["query-errors"]));
}

function renderDatabases(databases, total = databases.length) {
  databaseGrid.replaceChildren();
  databaseCount.textContent = String(total);
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
    const activity = database.activity || {};
    details.append(
      field("Store ID", database["store-id"]),
      field("State", String(taggedValue(database.state) || "active").replace(/^:/, "")),
      field("Runtime", activity["loaded?"] ? `loaded · ${number(activity.leases)} lease${activity.leases === 1 ? "" : "s"}` : "idle"),
      field("Basis t", activity["basis-t"]),
      field("Transactions", number(activity.transactions)),
      field("Datoms written", number(activity["transacted-datoms"])),
      field("Average commit", milliseconds(activity["average-commit-ms"])),
      field("Conflicts", number(activity["head-conflicts"])),
      field("Created", displayDate(database["created-at"])),
      field("Principal", database["created-by"])
    );
    card.append(title, backend, details);
    databaseGrid.append(card);
  }
}

function renderPage(page) {
  const total = Number(page?.total || 0);
  const offset = Number(page?.offset || 0);
  const limit = Number(page?.limit || pageSize);
  const end = Math.min(offset + limit, total);
  byId("page-status").textContent = total === 0 ? "No results" : `${offset + 1}–${end} of ${total}`;
  byId("previous-page").disabled = offset === 0;
  byId("next-page").disabled = !page?.["has-more?"];
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
    const params = new URLSearchParams({ offset: String(pageOffset), limit: String(pageSize) });
    if (searchQuery) params.set("q", searchQuery);
    const status = await request(`/admin/status?${params}`);
    const databases = status.databases || [];
    renderNodeActivity(status.node);
    renderDatabases(databases, status.page?.total);
    renderPage(status.page);
    catalogStatus.textContent = searchQuery
      ? `${status.page?.total || 0} matching database${status.page?.total === 1 ? "" : "s"}.`
      : `${status.page?.total || 0} active database${status.page?.total === 1 ? "" : "s"} visible to this principal.`;
  } catch (error) {
    renderNodeActivity(null);
    renderDatabases([]);
    renderPage(null);
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
  pageOffset = 0;
  refresh();
});

byId("clear-token").addEventListener("click", () => {
  sessionStorage.removeItem("datahike.admin.token");
  tokenInput.value = "";
  refresh();
});

byId("refresh").addEventListener("click", refresh);
byId("database-search").addEventListener("submit", (event) => {
  event.preventDefault();
  searchQuery = databaseQuery.value.trim();
  pageOffset = 0;
  refresh();
});
byId("clear-search").addEventListener("click", () => {
  databaseQuery.value = "";
  searchQuery = "";
  pageOffset = 0;
  refresh();
});
byId("previous-page").addEventListener("click", () => {
  pageOffset = Math.max(0, pageOffset - pageSize);
  refresh();
});
byId("next-page").addEventListener("click", () => {
  pageOffset += pageSize;
  refresh();
});
refresh();
