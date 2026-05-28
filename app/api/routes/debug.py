from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy.orm import Session

from app.db import get_db
from app.services.monitoring.debug import get_latest_timestamp_delta
from app.services.monitoring.service import get_latest_frame_path

router = APIRouter(prefix="/debug", tags=["debug"])


DEBUG_VIEWER_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>GC Debug Viewer</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f7f8fa;
      --panel: #ffffff;
      --line: #d9dee7;
      --text: #151922;
      --muted: #667085;
      --accent: #136f63;
      --warn: #b54708;
      --bad: #b42318;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      height: 56px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 20px;
      border-bottom: 1px solid var(--line);
      background: var(--panel);
    }
    h1 {
      margin: 0;
      font-size: 18px;
      font-weight: 650;
      letter-spacing: 0;
    }
    button {
      height: 34px;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--text);
      border-radius: 6px;
      padding: 0 12px;
      cursor: pointer;
    }
    button:hover { border-color: var(--accent); }
    main {
      display: grid;
      grid-template-columns: 320px minmax(0, 1fr) 340px;
      gap: 12px;
      padding: 12px;
      min-height: calc(100vh - 56px);
      align-items: start;
    }
    .left {
      display: grid;
      grid-template-rows: auto auto;
      gap: 12px;
      align-content: start;
    }
    section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      min-width: 0;
    }
    .section-head {
      height: 44px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 12px;
      border-bottom: 1px solid var(--line);
      font-weight: 650;
    }
    .section-title {
      min-width: 0;
      overflow-wrap: anywhere;
    }
    .section-toggle {
      width: 28px;
      height: 28px;
      display: inline-grid;
      place-items: center;
      padding: 0;
      border-radius: 6px;
      font-size: 15px;
      line-height: 1;
      transition: transform 120ms ease;
      flex: 0 0 auto;
    }
    .collapsible.collapsed > .section-head {
      border-bottom: 0;
    }
    .collapsible.collapsed {
      min-height: 0;
    }
    .collapsible.collapsed > .collapsible-body {
      display: none;
    }
    .collapsible.collapsed .section-toggle {
      transform: rotate(-90deg);
    }
    .camera-list {
      display: grid;
      gap: 8px;
      padding: 10px;
    }
    .camera-section {
      min-height: 290px;
    }
    .camera-section.collapsed {
      min-height: 0;
    }
    .camera-row {
      width: 100%;
      height: auto;
      min-height: 72px;
      text-align: left;
      display: grid;
      gap: 4px;
      border-radius: 6px;
      align-content: center;
    }
    .camera-row.active {
      border-color: var(--accent);
      background: #eef8f5;
    }
    .camera-name {
      font-weight: 650;
      overflow-wrap: anywhere;
    }
    .meta {
      color: var(--muted);
      font-size: 12px;
      overflow-wrap: anywhere;
    }
    .viewer {
      display: grid;
      grid-template-rows: 44px minmax(260px, 1fr) auto;
      height: clamp(520px, calc(100vh - 80px), 860px);
      min-height: 0;
      align-self: start;
      overflow: hidden;
    }
    .frame-wrap {
      display: grid;
      place-items: center;
      background: #111827;
      min-height: 0;
      overflow: hidden;
    }
    .frame-wrap img {
      max-width: 100%;
      max-height: 100%;
      object-fit: contain;
      display: block;
    }
    .empty {
      color: #d0d5dd;
      padding: 20px;
      text-align: center;
    }
    .detail-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
      padding: 10px;
      border-top: 1px solid var(--line);
    }
    .metric {
      min-height: 54px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 8px;
      overflow: hidden;
    }
    .metric span {
      display: block;
      color: var(--muted);
      font-size: 12px;
    }
    .metric strong {
      display: block;
      margin-top: 2px;
      font-size: 15px;
      overflow-wrap: anywhere;
    }
    .side {
      display: flex;
      flex-direction: column;
      gap: 12px;
      align-self: start;
      background: transparent;
      border: 0;
    }
    .side > section {
      width: 100%;
    }
    .panel-body { padding: 10px; }
    .relay-grid, .delta-list {
      display: grid;
      gap: 8px;
    }
    .delta-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      padding: 8px;
      border: 1px solid var(--line);
      border-radius: 6px;
    }
    .ok { color: var(--accent); }
    .warn { color: var(--warn); }
    .bad { color: var(--bad); }
    @media (max-width: 980px) {
      main { grid-template-columns: 1fr; }
      .viewer {
        height: auto;
        max-height: none;
        overflow: visible;
      }
      .frame-wrap { min-height: 360px; }
      .detail-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
  </style>
</head>
<body>
  <header>
    <h1>GC Debug Viewer</h1>
    <button id="refreshButton" type="button">Refresh</button>
  </header>
  <main>
    <div class="left">
      <section class="camera-section collapsible">
        <div class="section-head">
          <span class="section-title">Cameras <span id="cameraCount" class="meta">0</span></span>
          <button class="section-toggle" type="button" aria-label="Toggle Cameras" aria-expanded="true" aria-controls="cameraList" data-collapse-target="cameraList">▾</button>
        </div>
        <div id="cameraList" class="camera-list collapsible-body"></div>
      </section>
      <section class="collapsible">
        <div class="section-head">
          <span class="section-title">gRPC Ingest</span>
          <button class="section-toggle" type="button" aria-label="Toggle gRPC Ingest" aria-expanded="true" aria-controls="ingestStatus" data-collapse-target="ingestStatus">▾</button>
        </div>
        <div id="ingestStatus" class="panel-body relay-grid collapsible-body"></div>
      </section>
    </div>
    <section class="viewer">
      <div class="section-head">
        <span id="selectedTitle">Latest Frame</span>
        <span id="lastUpdated" class="meta"></span>
      </div>
      <div id="frameWrap" class="frame-wrap">
        <div class="empty">No frame selected</div>
      </div>
      <div id="details" class="detail-grid"></div>
    </section>
    <section class="side">
      <section class="collapsible">
        <div class="section-head">
          <span class="section-title">Relay</span>
          <button class="section-toggle" type="button" aria-label="Toggle Relay" aria-expanded="true" aria-controls="relayStatus" data-collapse-target="relayStatus">▾</button>
        </div>
        <div id="relayStatus" class="panel-body relay-grid collapsible-body"></div>
      </section>
      <section class="collapsible">
        <div class="section-head">
          <span class="section-title">Sync</span>
          <button class="section-toggle" type="button" aria-label="Toggle Sync" aria-expanded="true" aria-controls="syncStatus" data-collapse-target="syncStatus">▾</button>
        </div>
        <div id="syncStatus" class="panel-body relay-grid collapsible-body"></div>
      </section>
      <section class="collapsible">
        <div class="section-head">
          <span class="section-title">Timestamp Delta</span>
          <button class="section-toggle" type="button" aria-label="Toggle Timestamp Delta" aria-expanded="true" aria-controls="deltaList" data-collapse-target="deltaList">▾</button>
        </div>
        <div id="deltaList" class="panel-body delta-list collapsible-body"></div>
      </section>
    </section>
  </main>
  <script>
    let selectedDeviceId = null;
    let cameras = [];
    let selectedFrameKey = null;

    const cameraList = document.getElementById("cameraList");
    const cameraCount = document.getElementById("cameraCount");
    const frameWrap = document.getElementById("frameWrap");
    const details = document.getElementById("details");
    const selectedTitle = document.getElementById("selectedTitle");
    const lastUpdated = document.getElementById("lastUpdated");
    const relayStatus = document.getElementById("relayStatus");
    const syncStatus = document.getElementById("syncStatus");
    const ingestStatus = document.getElementById("ingestStatus");
    const deltaList = document.getElementById("deltaList");
    const refreshButton = document.getElementById("refreshButton");

    function valueOrDash(value) {
      return value === null || value === undefined || value === "" ? "-" : value;
    }

    function escapeHtml(value) {
      return String(valueOrDash(value))
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }

    function formatAge(ms) {
      if (ms === null || ms === undefined) return "-";
      if (ms < 1000) return `${ms}ms`;
      return `${(ms / 1000).toFixed(1)}s`;
    }

    function metric(label, value, className = "") {
      return `<div class="metric"><span>${escapeHtml(label)}</span><strong class="${className}">${escapeHtml(value)}</strong></div>`;
    }

    function initCollapsibles() {
      document.querySelectorAll("[data-collapse-target]").forEach((button) => {
        button.addEventListener("click", () => {
          const section = button.closest(".collapsible");
          if (!section) return;
          const collapsed = section.classList.toggle("collapsed");
          button.setAttribute("aria-expanded", collapsed ? "false" : "true");
        });
      });
    }

    function renderCameras() {
      cameraCount.textContent = String(cameras.length);
      if (cameras.length === 0) {
        cameraList.innerHTML = '<div class="meta">No cameras</div>';
        return;
      }
      cameraList.innerHTML = cameras.map((camera) => {
        const active = camera.device_id === selectedDeviceId ? " active" : "";
        const fps = Number(camera.estimated_capture_fps || camera.estimated_fps || 0).toFixed(2);
        return `
          <button class="camera-row${active}" data-device-id="${escapeHtml(camera.device_id)}" type="button">
            <span class="camera-name">${escapeHtml(camera.device_id)}</span>
            <span class="meta">fps ${fps} · frames ${escapeHtml(camera.frame_count)}</span>
            <span class="meta">ts ${escapeHtml(camera.latest_timestamp)}</span>
          </button>
        `;
      }).join("");

      document.querySelectorAll(".camera-row").forEach((button) => {
        button.addEventListener("click", () => {
          selectedDeviceId = button.dataset.deviceId;
          renderCameras();
          renderSelected();
        });
      });
    }

    function getFrameKey(camera) {
      if (!camera) return null;
      return `${camera.latest_sequence ?? "-"}:${camera.latest_timestamp ?? "-"}`;
    }

    function renderSelected(forceImageReload = false) {
      const camera = cameras.find((item) => item.device_id === selectedDeviceId);
      if (!camera) {
        selectedTitle.textContent = "Latest Frame";
        frameWrap.innerHTML = '<div class="empty">No frame selected</div>';
        details.innerHTML = "";
        selectedFrameKey = null;
        return;
      }

      selectedTitle.textContent = camera.device_id;
      const nextFrameKey = getFrameKey(camera);
      if (forceImageReload || nextFrameKey !== selectedFrameKey) {
        frameWrap.innerHTML = `<img alt="${escapeHtml(camera.device_id)}" src="/debug/cameras/${encodeURIComponent(camera.device_id)}/latest-frame?t=${Date.now()}">`;
        selectedFrameKey = nextFrameKey;
      }
      details.innerHTML = [
        metric("Timestamp", camera.latest_timestamp),
        metric("Sequence", camera.latest_sequence),
        metric("Age", formatAge(camera.last_received_age_ms), camera.last_received_age_ms > 3000 ? "warn" : "ok"),
        metric("Bytes", camera.latest_image_bytes),
        metric("Capture FPS", Number(camera.estimated_capture_fps || camera.estimated_fps || 0).toFixed(2)),
        metric("Ingest FPS", Number(camera.estimated_ingest_fps || 0).toFixed(2)),
        metric("Frames", camera.frame_count),
        metric("Gaps", camera.sequence_gap_count, camera.sequence_gap_count > 0 ? "warn" : "ok"),
        metric("Frame ID", camera.latest_frame_id),
      ].join("");
    }

    function renderRelay(status) {
      relayStatus.innerHTML = [
        metric("Mode", status.relay_mode || "-"),
        metric("Selected", status.selected ? "true" : "false", status.selected ? "ok" : ""),
        metric("Enabled", status.enabled ? "true" : "false", status.enabled ? "ok" : ""),
        metric("Running", status.running ? "true" : "false", status.running ? "ok" : "warn"),
        metric("Queue", status.queue_size),
        metric("Errors", status.error_count, status.error_count > 0 ? "bad" : "ok"),
        metric("Sent", status.sent_count),
        metric("Ack", status.ack_received_count),
        metric("Target", status.target),
        metric("Last Error", status.last_error, status.last_error ? "bad" : "ok"),
      ].join("");
    }

    function renderSync(status) {
      const expected = (status.expected_cameras || []).join(", ") || "-";
      syncStatus.innerHTML = [
        metric("Enabled", status.enabled ? "true" : "false", status.enabled ? "ok" : ""),
        metric("Expected", expected),
        metric("Window", status.window_ms ?? "-"),
        metric("Matched", status.matched_count ?? 0, Number(status.matched_count || 0) > 0 ? "ok" : ""),
        metric("Missed", status.missed_count ?? 0, Number(status.missed_count || 0) > 0 ? "warn" : "ok"),
        metric("Duplicate", status.duplicate_count ?? 0, Number(status.duplicate_count || 0) > 0 ? "warn" : "ok"),
        metric("Ignored", status.ignored_count ?? 0, Number(status.ignored_count || 0) > 0 ? "warn" : "ok"),
        metric("Last Frame Set", status.last_frame_set_id ?? "-"),
        metric("Last Span", status.last_span_ms ?? "-"),
        metric("Watermark", status.watermark_timestamp_ms ?? "-"),
        metric("Stale Drops", status.dropped_stale_count ?? 0, Number(status.dropped_stale_count || 0) > 0 ? "warn" : "ok"),
        metric("Last Reason", status.last_reason || "-"),
      ].join("");
    }

    function renderIngest(status) {
      const expected = (status.expected_device_ids || []).join(", ") || status.expected_device_count || "-";
      const observed = (status.observed_device_ids || []).join(", ") || "-";
      const active = (status.active_device_ids || []).join(", ") || "-";
      const missing = (status.missing_device_ids || []).join(", ") || "-";
      const unexpected = (status.unexpected_device_ids || []).join(", ") || "-";
      ingestStatus.innerHTML = [
        metric("Enabled", status.enabled ? "true" : "false", status.enabled ? "ok" : ""),
        metric("Running", status.running ? "true" : "false", status.running ? "ok" : "warn"),
        metric("Bind", status.bind),
        metric("Gate Enabled", status.gate_enabled ? "true" : "false", status.gate_enabled ? "ok" : ""),
        metric("Gate Open", status.gate_open ? "true" : "false", status.gate_open ? "ok" : "warn"),
        metric("Expected", expected),
        metric("Observed", observed),
        metric("Active", active, active === "-" ? "warn" : "ok"),
        metric("Missing", missing, missing === "-" ? "ok" : "warn"),
        metric("Unexpected", unexpected, unexpected === "-" ? "ok" : "warn"),
        metric("Gate Start TS", status.gate_start_timestamp_ms ?? "-"),
        metric("First Accepted TS", status.first_accepted_timestamp_ms ?? "-"),
        metric("Pre-gate Drops", status.pre_gate_dropped_count ?? 0, Number(status.pre_gate_dropped_count || 0) > 0 ? "warn" : "ok"),
        metric("Stale Gate Drops", status.stale_after_gate_dropped_count ?? 0, Number(status.stale_after_gate_dropped_count || 0) > 0 ? "warn" : "ok"),
        metric("Started", status.collection_started ? "true" : "false", status.collection_started ? "ok" : ""),
        metric("Stopped", status.collection_stopped ? "true" : "false", status.collection_stopped ? "warn" : "ok"),
        metric("Stop Reason", status.collection_stop_reason || "-", status.collection_stop_reason ? "warn" : "ok"),
      ].join("");
    }

    function renderDelta(payload) {
      const items = payload.items || [];
      if (items.length === 0) {
        deltaList.innerHTML = '<div class="meta">No timestamp data</div>';
        return;
      }
      deltaList.innerHTML = items.map((item) => {
        const absDelta = Math.abs(item.delta_ms || 0);
        const tone = absDelta > 200 ? "bad" : absDelta > 50 ? "warn" : "ok";
        return `
          <div class="delta-row">
            <span>${escapeHtml(item.device_id)}<br><span class="meta">${escapeHtml(item.latest_timestamp)}</span></span>
            <strong class="${tone}">${escapeHtml(item.delta_ms)}ms</strong>
          </div>
        `;
      }).join("");
    }

    function applyPayload(payload) {
      cameras = payload.cameras || [];
      const ingest = payload.grpc_ingest || {};
      const relay = payload.relay || {};
      const sync = payload.sync || {};
      const delta = payload.timestamp_delta || { items: [] };
      if (!selectedDeviceId && cameras.length > 0) {
        selectedDeviceId = cameras[0].device_id;
      }
      if (selectedDeviceId && !cameras.some((camera) => camera.device_id === selectedDeviceId)) {
        selectedDeviceId = cameras.length > 0 ? cameras[0].device_id : null;
      }
      renderCameras();
      renderSelected();
      renderIngest(ingest);
      renderRelay(relay);
      renderSync(sync);
      renderDelta(delta);
      lastUpdated.textContent = new Date().toLocaleTimeString();
    }

    async function loadFallback(forceImageReload = false) {
      const [cameraResponse, ingestResponse, relayResponse, syncResponse, deltaResponse] = await Promise.all([
        fetch("/monitoring/cameras"),
        fetch("/monitoring/grpc-ingest"),
        fetch("/monitoring/relay"),
        fetch("/monitoring/sync"),
        fetch("/debug/timestamp-delta"),
      ]);
      cameras = (await cameraResponse.json()).items || [];
      if (!selectedDeviceId && cameras.length > 0) {
        selectedDeviceId = cameras[0].device_id;
      }
      if (selectedDeviceId && !cameras.some((camera) => camera.device_id === selectedDeviceId)) {
        selectedDeviceId = cameras.length > 0 ? cameras[0].device_id : null;
      }
      renderCameras();
      renderSelected(forceImageReload);
      renderIngest(await ingestResponse.json());
      renderRelay(await relayResponse.json());
      renderSync(await syncResponse.json());
      renderDelta(await deltaResponse.json());
      lastUpdated.textContent = new Date().toLocaleTimeString();
    }

    function connectEventStream() {
      const source = new EventSource("/monitoring/events");
      source.onmessage = (event) => {
        applyPayload(JSON.parse(event.data));
      };
      source.onerror = () => {
        source.close();
        window.setTimeout(connectEventStream, 2000);
      };
    }

    refreshButton.addEventListener("click", () => loadFallback(true));
    initCollapsibles();
    loadFallback(true);
    connectEventStream();
  </script>
</body>
</html>"""


@router.get(
    "/viewer",
    response_class=HTMLResponse,
    summary="Debug Viewer",
    description="Camera latest-frame, timestamp delta, and relay status debug page.",
)
def get_debug_viewer():
    return HTMLResponse(DEBUG_VIEWER_HTML)


@router.get(
    "/cameras/{device_id}/latest-frame",
    summary="카메라 최신 프레임 이미지 조회",
    description="StreamState 또는 DB 기준 최신 프레임 파일을 이미지 응답으로 반환합니다.",
)
def get_latest_frame(device_id: str, db: Session = Depends(get_db)):
    file_path = get_latest_frame_path(db, device_id)
    if file_path is None:
        raise HTTPException(status_code=404, detail="Latest frame not found")
    return FileResponse(file_path)


@router.get(
    "/timestamp-delta",
    summary="카메라 최신 timestamp 차이 조회",
    description="StreamState 기준 각 카메라의 최신 timestamp와 기준 timestamp 사이의 차이를 반환합니다.",
)
def get_timestamp_delta():
    return get_latest_timestamp_delta()
