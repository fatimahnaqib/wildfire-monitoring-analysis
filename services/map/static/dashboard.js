(function () {
  "use strict";

  const DEFAULT_CENTER = [37.0, -120.0];
  const DEFAULT_ZOOM = 5;

  const statsPanel = document.getElementById("stats-panel");
  const eventsBody = document.getElementById("events-body");
  const eventCount = document.getElementById("event-count");
  const statusMessage = document.getElementById("status-message");
  const filterForm = document.getElementById("filter-form");

  const map = L.map("map").setView(DEFAULT_CENTER, DEFAULT_ZOOM);
  L.tileLayer("/osm-tiles/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution:
      '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
  }).addTo(map);

  let markersLayer = L.layerGroup().addTo(map);

  function brightnessColor(brightTi4) {
    const v = Number(brightTi4);
    if (Number.isNaN(v)) return "#f48c06";
    if (v >= 350) return "#d00000";
    if (v >= 300) return "#e85d04";
    return "#f48c06";
  }

  function setStatus(text, isError) {
    statusMessage.textContent = text;
    statusMessage.classList.toggle("error", Boolean(isError));
  }

  function formatStatLabel(key) {
    return key
      .replace(/_/g, " ")
      .replace(/\b\w/g, (c) => c.toUpperCase());
  }

  async function loadStats() {
    try {
      const res = await fetch("/demo/stats");
      if (!res.ok) throw new Error("Stats request failed");
      const data = await res.json();
      const cards = Object.entries(data.stats || data).map(([key, value]) => {
        const display =
          value === null || value === undefined ? "—" : String(value);
        return (
          '<div class="stat-card">' +
          '<div class="label">' + formatStatLabel(key) + "</div>" +
          '<div class="value">' + display + "</div>" +
          "</div>"
        );
      });
      statsPanel.innerHTML =
        '<div class="stats-grid">' + cards.join("") + "</div>";
    } catch (err) {
      statsPanel.innerHTML =
        '<p class="error">Could not load statistics. Is the database seeded?</p>';
      console.error(err);
    }
  }

  function buildQueryParams() {
    const params = new URLSearchParams();
    const lookback = document.getElementById("lookback-days").value;
    const limit = document.getElementById("limit").value;
    const minDate = document.getElementById("min-date").value;
    const maxDate = document.getElementById("max-date").value;

    if (lookback !== "") params.set("lookback_days", lookback);
    if (limit !== "") params.set("limit", limit);
    if (minDate) params.set("min_date", minDate);
    if (maxDate) params.set("max_date", maxDate);
    return params;
  }

  function renderTable(events) {
    eventsBody.innerHTML = "";
    eventCount.textContent = String(events.length);

    const fragment = document.createDocumentFragment();
    events.forEach((ev) => {
      const tr = document.createElement("tr");
      tr.innerHTML =
        "<td>" + (ev.acq_date || "—") + "</td>" +
        "<td>" + (ev.acq_time || "—") + "</td>" +
        "<td>" + Number(ev.latitude).toFixed(4) + "</td>" +
        "<td>" + Number(ev.longitude).toFixed(4) + "</td>" +
        "<td>" + (ev.bright_ti4 ?? "—") + "</td>" +
        "<td>" + (ev.frp ?? "—") + "</td>" +
        "<td>" + (ev.satellite || "—") + "</td>" +
        "<td>" + (ev.confidence || "—") + "</td>";
      fragment.appendChild(tr);
    });
    eventsBody.appendChild(fragment);
  }

  function renderMap(events) {
    markersLayer.clearLayers();
    if (!events.length) {
      map.setView(DEFAULT_CENTER, DEFAULT_ZOOM);
      return;
    }

    const bounds = [];
    events.forEach((ev) => {
      const lat = Number(ev.latitude);
      const lon = Number(ev.longitude);
      if (Number.isNaN(lat) || Number.isNaN(lon)) return;

      const color = brightnessColor(ev.bright_ti4);
      const marker = L.circleMarker([lat, lon], {
        radius: 5,
        fillColor: color,
        color: "#1a1a1a",
        weight: 1,
        fillOpacity: 0.85,
      });
      marker.bindPopup(
        "<strong>Detection</strong><br>" +
          "Date: " + (ev.acq_date || "—") + "<br>" +
          "Brightness: " + (ev.bright_ti4 ?? "—") + "<br>" +
          "Satellite: " + (ev.satellite || "—")
      );
      markersLayer.addLayer(marker);
      bounds.push([lat, lon]);
    });

    if (bounds.length) {
      map.fitBounds(bounds, { padding: [24, 24], maxZoom: 10 });
    }
  }

  async function loadEvents() {
    setStatus("Loading events…");
    try {
      const params = buildQueryParams();
      const res = await fetch("/demo/wildfires?" + params.toString());
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.detail || body.message || res.statusText);
      }
      const data = await res.json();
      const events = data.events || [];
      renderTable(events);
      renderMap(events);
      setStatus(
        "Showing " + events.length + " events (limit " + data.limit + ")."
      );
    } catch (err) {
      setStatus(err.message || "Failed to load events", true);
      console.error(err);
    }
  }

  filterForm.addEventListener("submit", function (e) {
    e.preventDefault();
    loadEvents();
  });

  document.getElementById("reset-filters").addEventListener("click", function () {
    document.getElementById("lookback-days").value = "30";
    document.getElementById("limit").value = "500";
    document.getElementById("min-date").value = "";
    document.getElementById("max-date").value = "";
    loadEvents();
  });

  loadStats();
  loadEvents();
})();
