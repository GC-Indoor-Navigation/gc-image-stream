import asyncio
import json

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse

from app.services.monitoring.debug import get_latest_timestamp_delta
from app.services.monitoring.service import (
    get_camera_state,
    get_frame_set_relay_status,
    get_grpc_ingest_status,
    get_relay_status,
    get_sync_status,
    list_recent_sync_frame_sets,
    list_camera_states,
)

router = APIRouter(prefix="/monitoring", tags=["monitoring"])


MONITORING_VIEWER_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>GC Monitoring</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f7fa;
      --surface: #ffffff;
      --line: #d8dee8;
      --text: #121926;
      --muted: #667085;
      --good: #0f766e;
      --warn: #b54708;
      --bad: #b42318;
      --accent: #155eef;
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
      background: var(--surface);
    }
    h1 {
      margin: 0;
      font-size: 18px;
      font-weight: 650;
      letter-spacing: 0;
    }
    .meta {
      color: var(--muted);
      font-size: 12px;
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
    main {
      padding: 12px;
      display: grid;
      gap: 12px;
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 10px;
    }
    .card, .panel {
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 8px;
    }
    .card {
      min-height: 86px;
      padding: 12px;
    }
    .card span {
      display: block;
      color: var(--muted);
      font-size: 12px;
    }
    .card strong {
      display: block;
      margin-top: 4px;
      font-size: 24px;
      font-weight: 700;
      overflow-wrap: anywhere;
    }
    .panel-head {
      height: 44px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 12px;
      border-bottom: 1px solid var(--line);
      font-weight: 650;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
      overflow-wrap: anywhere;
    }
    th {
      color: var(--muted);
      font-size: 12px;
      font-weight: 600;
    }
    .status {
      display: inline-flex;
      align-items: center;
      min-width: 88px;
      justify-content: center;
      height: 24px;
      border-radius: 999px;
      border: 1px solid var(--line);
      font-size: 12px;
      font-weight: 600;
    }
    .healthy { color: var(--good); border-color: #99f6e4; background: #f0fdfa; }
    .stale { color: var(--warn); border-color: #fed7aa; background: #fff7ed; }
    .disconnected { color: var(--bad); border-color: #fecaca; background: #fef2f2; }
    .runtime-grid,
    .relay-grid,
    .sync-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      padding: 12px;
    }
    .relay-item {
      min-height: 78px;
      padding: 10px;
      border: 1px solid var(--line);
      border-radius: 6px;
    }
    .relay-item span {
      display: block;
      color: var(--muted);
      font-size: 12px;
    }
    .relay-item strong {
      display: block;
      margin-top: 4px;
      font-size: 18px;
      overflow-wrap: anywhere;
    }
    .links {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
    }
    a {
      color: var(--accent);
      text-decoration: none;
    }
    a:hover { text-decoration: underline; }
    @media (max-width: 1080px) {
      .summary-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .runtime-grid,
      .relay-grid,
      .sync-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 720px) {
      .summary-grid,
      .runtime-grid,
      .relay-grid,
      .sync-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      table, thead, tbody, th, td, tr { display: block; }
      thead { display: none; }
      td { padding-top: 4px; padding-bottom: 8px; }
      tr { border-bottom: 1px solid var(--line); padding: 8px 0; }
    }
  </style>
</head>
<body>
  <header>
    <h1>GC Monitoring</h1>
    <div class="links">
      <a href="/debug/viewer">Debug Viewer</a>
      <button id="refreshButton" type="button">Refresh</button>
    </div>
  </header>
  <main>
    <section class="summary-grid" id="summaryGrid"></section>
    <section class="panel">
      <div class="panel-head">
        <span>gRPC Ingest</span>
        <span class="meta">Connection gate</span>
      </div>
      <div class="runtime-grid" id="ingestGrid"></div>
    </section>
    <section class="panel">
      <div class="panel-head">
        <span>Relay</span>
        <span id="lastUpdated" class="meta"></span>
      </div>
      <div class="relay-grid" id="relayGrid"></div>
    </section>
    <section class="panel">
      <div class="panel-head">
        <span>Frame-set Relay</span>
        <span class="meta">Matched frame sets</span>
      </div>
      <div class="relay-grid" id="frameSetRelayGrid"></div>
    </section>
    <section class="panel">
      <div class="panel-head">
        <span>Sync</span>
        <span class="meta">Frame matching</span>
      </div>
      <div class="sync-grid" id="syncGrid"></div>
    </section>
    <section class="panel">
      <div class="panel-head">
        <span>Cameras</span>
        <span class="meta">Operational view</span>
      </div>
      <div style="overflow:auto;">
        <table>
          <thead>
            <tr>
              <th>Camera</th>
              <th>Status</th>
              <th>FPS</th>
              <th>Frames</th>
              <th>Last Timestamp</th>
              <th>Age</th>
              <th>Sequence</th>
              <th>Gaps</th>
            </tr>
          </thead>
          <tbody id="cameraTable"></tbody>
        </table>
      </div>
    </section>
  </main>
  <script>
    const summaryGrid = document.getElementById("summaryGrid");
    const ingestGrid = document.getElementById("ingestGrid");
    const relayGrid = document.getElementById("relayGrid");
    const frameSetRelayGrid = document.getElementById("frameSetRelayGrid");
    const syncGrid = document.getElementById("syncGrid");
    const cameraTable = document.getElementById("cameraTable");
    const lastUpdated = document.getElementById("lastUpdated");
    const refreshButton = document.getElementById("refreshButton");

    function escapeHtml(value) {
      return String(value ?? "-")
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

    function statusClass(status) {
      return status === "healthy" ? "healthy" : status === "stale" ? "stale" : "disconnected";
    }

    function summaryCard(label, value, tone = "") {
      return `<div class="card"><span>${escapeHtml(label)}</span><strong class="${tone}">${escapeHtml(value)}</strong></div>`;
    }

    function relayCard(label, value, tone = "") {
      return `<div class="relay-item"><span>${escapeHtml(label)}</span><strong class="${tone}">${escapeHtml(value)}</strong></div>`;
    }

    function renderSummary(cameras, relay) {
      const healthy = cameras.filter((camera) => camera.status === "healthy").length;
      const stale = cameras.filter((camera) => camera.status === "stale").length;
      const disconnected = cameras.filter((camera) => camera.status === "disconnected").length;
      const totalFps = cameras.reduce((sum, camera) => sum + Number(camera.estimated_fps || 0), 0);
      summaryGrid.innerHTML = [
        summaryCard("Cameras", cameras.length),
        summaryCard("Healthy", healthy),
        summaryCard("Stale", stale, stale > 0 ? "stale" : "healthy"),
        summaryCard("Disconnected", disconnected, disconnected > 0 ? "disconnected" : "healthy"),
        summaryCard("Total FPS", totalFps.toFixed(2)),
        summaryCard("Relay Queue", relay.queue_size),
      ].join("");
    }

    function renderRelay(relay) {
      relayGrid.innerHTML = [
        relayCard("Enabled", relay.enabled ? "true" : "false", relay.enabled ? "healthy" : ""),
        relayCard("Running", relay.running ? "true" : "false", relay.running ? "healthy" : "stale"),
        relayCard("Target", relay.target || "-"),
        relayCard("Queue", relay.queue_size, relay.queue_size > 0 ? "stale" : "healthy"),
        relayCard("Sent", relay.sent_count),
        relayCard("Ack", relay.ack_received_count),
        relayCard("Errors", relay.error_count, relay.error_count > 0 ? "disconnected" : "healthy"),
        relayCard("Last Error", relay.last_error || "-", relay.last_error ? "disconnected" : ""),
      ].join("");
    }

    function renderFrameSetRelay(relay) {
      frameSetRelayGrid.innerHTML = [
        relayCard("Enabled", relay.enabled ? "true" : "false", relay.enabled ? "healthy" : ""),
        relayCard("Running", relay.running ? "true" : "false", relay.running ? "healthy" : "stale"),
        relayCard("Target", relay.target || "-"),
        relayCard("Queue", relay.queue_size, relay.queue_size > 0 ? "stale" : "healthy"),
        relayCard("Enqueued", relay.enqueued_count ?? 0),
        relayCard("Sent", relay.sent_count ?? 0),
        relayCard("Ack", relay.ack_received_count ?? 0),
        relayCard("Errors", relay.error_count ?? 0, Number(relay.error_count || 0) > 0 ? "disconnected" : "healthy"),
        relayCard("Last Frame Set", relay.last_frame_set_id ?? "-"),
        relayCard("Last Error", relay.last_error || "-", relay.last_error ? "disconnected" : ""),
      ].join("");
    }

    function renderIngest(ingest) {
      const expected = (ingest.expected_device_ids || []).join(", ") || ingest.expected_device_count || "-";
      const observed = (ingest.observed_device_ids || []).join(", ") || "-";
      const missing = (ingest.missing_device_ids || []).join(", ") || "-";
      const unexpected = (ingest.unexpected_device_ids || []).join(", ") || "-";
      ingestGrid.innerHTML = [
        relayCard("Enabled", ingest.enabled ? "true" : "false", ingest.enabled ? "healthy" : ""),
        relayCard("Running", ingest.running ? "true" : "false", ingest.running ? "healthy" : "stale"),
        relayCard("Bind", ingest.bind || "-"),
        relayCard("Gate Enabled", ingest.gate_enabled ? "true" : "false", ingest.gate_enabled ? "healthy" : ""),
        relayCard("Gate Open", ingest.gate_open ? "true" : "false", ingest.gate_open ? "healthy" : "stale"),
        relayCard("Expected Devices", expected),
        relayCard("Observed Devices", observed),
        relayCard("Missing Devices", missing, missing === "-" ? "healthy" : "stale"),
        relayCard("Unexpected Devices", unexpected, unexpected === "-" ? "healthy" : "stale"),
      ].join("");
    }

    function renderSync(sync) {
      const expected = (sync.expected_cameras || []).join(", ") || "-";
      syncGrid.innerHTML = [
        relayCard("Enabled", sync.enabled ? "true" : "false", sync.enabled ? "healthy" : ""),
        relayCard("Expected Cameras", expected),
        relayCard("Window", sync.window_ms ?? "-"),
        relayCard("Matched", sync.matched_count ?? 0, Number(sync.matched_count || 0) > 0 ? "healthy" : ""),
        relayCard("Missed", sync.missed_count ?? 0, Number(sync.missed_count || 0) > 0 ? "stale" : "healthy"),
        relayCard("Duplicate", sync.duplicate_count ?? 0, Number(sync.duplicate_count || 0) > 0 ? "stale" : "healthy"),
        relayCard("Last Reason", sync.last_reason || "-"),
        relayCard("Last Frame Set", sync.last_frame_set_id ?? "-"),
      ].join("");
    }

    function renderCameras(cameras) {
      if (cameras.length === 0) {
        cameraTable.innerHTML = '<tr><td colspan="8" class="meta">No camera state</td></tr>';
        return;
      }
      cameraTable.innerHTML = cameras.map((camera) => `
        <tr>
          <td><a href="/debug/viewer">${escapeHtml(camera.device_id)}</a></td>
          <td><span class="status ${statusClass(camera.status)}">${escapeHtml(camera.status)}</span></td>
          <td>${escapeHtml(Number(camera.estimated_fps || 0).toFixed(2))}</td>
          <td>${escapeHtml(camera.frame_count)}</td>
          <td>${escapeHtml(camera.latest_timestamp)}</td>
          <td>${escapeHtml(formatAge(camera.last_received_age_ms))}</td>
          <td>${escapeHtml(camera.latest_sequence)}</td>
          <td>${escapeHtml(camera.sequence_gap_count)}</td>
        </tr>
      `).join("");
    }

    function applyPayload(payload) {
      const cameras = payload.cameras || [];
      const ingest = payload.grpc_ingest || {};
      const relay = payload.relay || {};
      const frameSetRelay = payload.frame_set_relay || {};
      const sync = payload.sync || {};
      renderSummary(cameras, relay);
      renderIngest(ingest);
      renderRelay(relay);
      renderFrameSetRelay(frameSetRelay);
      renderSync(sync);
      renderCameras(cameras);
      lastUpdated.textContent = new Date().toLocaleTimeString();
    }

    async function loadFallback() {
      const [cameraResponse, ingestResponse, relayResponse, frameSetRelayResponse, syncResponse] = await Promise.all([
        fetch("/monitoring/cameras"),
        fetch("/monitoring/grpc-ingest"),
        fetch("/monitoring/relay"),
        fetch("/monitoring/frame-set-relay"),
        fetch("/monitoring/sync"),
      ]);
      applyPayload({
        cameras: (await cameraResponse.json()).items || [],
        grpc_ingest: await ingestResponse.json(),
        relay: await relayResponse.json(),
        frame_set_relay: await frameSetRelayResponse.json(),
        sync: await syncResponse.json(),
      });
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

    refreshButton.addEventListener("click", loadFallback);
    loadFallback();
    connectEventStream();
  </script>
</body>
</html>"""


@router.get(
    "/viewer",
    response_class=HTMLResponse,
    summary="Monitoring Viewer",
    description="Operational monitoring page for camera and relay status.",
)
def get_monitoring_viewer():
    return HTMLResponse(MONITORING_VIEWER_HTML)


def build_monitoring_snapshot():
    return {
        "cameras": list_camera_states(),
        "grpc_ingest": get_grpc_ingest_status(),
        "relay": get_relay_status(),
        "frame_set_relay": get_frame_set_relay_status(),
        "sync": get_sync_status(),
        "timestamp_delta": get_latest_timestamp_delta(),
    }


@router.get(
    "/grpc-ingest",
    summary="gRPC ingest status",
    description="gRPC ingest listener and multi-device gate status.",
)
def get_grpc_ingest():
    return get_grpc_ingest_status()


@router.get(
    "/events",
    summary="Monitoring event stream",
    description="SSE stream for camera, relay, and timestamp-delta updates.",
)
async def get_monitoring_events(request: Request, once: bool = False):
    async def event_generator():
        while True:
            payload = build_monitoring_snapshot()
            yield f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"
            if once or await request.is_disconnected():
                break
            await asyncio.sleep(1.0)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )


@router.get(
    "/cameras",
    summary="카메라별 stream 상태 목록 조회",
    description="Stream Server in-memory state에 기록된 카메라별 최신 프레임 상태를 반환합니다.",
)
def list_cameras():
    return {
        "items": list_camera_states(),
    }


@router.get(
    "/cameras/{device_id}",
    summary="단일 카메라 stream 상태 조회",
    description="Stream Server in-memory state에 기록된 단일 카메라의 최신 프레임 상태를 반환합니다.",
)
def get_camera(device_id: str):
    camera = get_camera_state(device_id)
    if camera is None:
        raise HTTPException(status_code=404, detail="Camera state not found")
    return camera


@router.get(
    "/relay",
    summary="Stream relay 상태 조회",
    description="Stream Server에서 Processing Server로 전달하는 gRPC relay worker 상태를 반환합니다.",
)
def get_relay():
    return get_relay_status()


@router.get(
    "/frame-set-relay",
    summary="Frame-set relay status",
    description="Stream Server synchronized frame-set relay worker status.",
)
def get_frame_set_relay():
    return get_frame_set_relay_status()


@router.get(
    "/sync",
    summary="Stream sync status",
    description="Stream Server frame synchronization status.",
)
def get_sync():
    return get_sync_status()


@router.get(
    "/sync/recent-frame-sets",
    summary="Recent stream sync frame sets",
    description="Recently matched synchronized frame sets from the Stream Server sync matcher.",
)
def get_recent_sync_frame_sets():
    return {
        "items": list_recent_sync_frame_sets(),
    }
