from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy.orm import Session

from app.db import get_db
from app.services.debug_service import get_latest_timestamp_delta
from app.services.monitoring_service import get_latest_frame_path

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
      grid-template-columns: 280px minmax(0, 1fr) 340px;
      gap: 12px;
      padding: 12px;
      min-height: calc(100vh - 56px);
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
    .camera-list {
      display: grid;
      gap: 8px;
      padding: 10px;
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
      min-height: calc(100vh - 80px);
    }
    .frame-wrap {
      display: grid;
      place-items: center;
      background: #111827;
      min-height: 360px;
      overflow: hidden;
    }
    .frame-wrap img {
      max-width: 100%;
      max-height: calc(100vh - 190px);
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
      display: grid;
      grid-template-rows: auto auto 1fr;
      gap: 12px;
      background: transparent;
      border: 0;
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
      .viewer { min-height: auto; }
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
    <section>
      <div class="section-head">Cameras <span id="cameraCount" class="meta">0</span></div>
      <div id="cameraList" class="camera-list"></div>
    </section>
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
      <section>
        <div class="section-head">Relay</div>
        <div id="relayStatus" class="panel-body relay-grid"></div>
      </section>
      <section>
        <div class="section-head">Timestamp Delta</div>
        <div id="deltaList" class="panel-body delta-list"></div>
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

    function renderCameras() {
      cameraCount.textContent = String(cameras.length);
      if (cameras.length === 0) {
        cameraList.innerHTML = '<div class="meta">No cameras</div>';
        return;
      }
      cameraList.innerHTML = cameras.map((camera) => {
        const active = camera.device_id === selectedDeviceId ? " active" : "";
        const fps = Number(camera.estimated_fps || 0).toFixed(2);
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
        metric("FPS", Number(camera.estimated_fps || 0).toFixed(2)),
        metric("Frames", camera.frame_count),
        metric("Gaps", camera.sequence_gap_count, camera.sequence_gap_count > 0 ? "warn" : "ok"),
        metric("Frame ID", camera.latest_frame_id),
      ].join("");
    }

    function renderRelay(status) {
      relayStatus.innerHTML = [
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
      const relay = payload.relay || {};
      const delta = payload.timestamp_delta || { items: [] };
      if (!selectedDeviceId && cameras.length > 0) {
        selectedDeviceId = cameras[0].device_id;
      }
      if (selectedDeviceId && !cameras.some((camera) => camera.device_id === selectedDeviceId)) {
        selectedDeviceId = cameras.length > 0 ? cameras[0].device_id : null;
      }
      renderCameras();
      renderSelected();
      renderRelay(relay);
      renderDelta(delta);
      lastUpdated.textContent = new Date().toLocaleTimeString();
    }

    async function loadFallback(forceImageReload = false) {
      const [cameraResponse, relayResponse, deltaResponse] = await Promise.all([
        fetch("/monitoring/cameras"),
        fetch("/monitoring/relay"),
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
      renderRelay(await relayResponse.json());
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
