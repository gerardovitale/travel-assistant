// Shared: fetch wrapper, toast, data status.
const API = "/api/v1";

export async function api(path, opts = {}) {
  const url = path.startsWith("http") ? path : `${API}${path}`;
  const res = await fetch(url, {
    headers: { "Accept": "application/json", "Content-Type": "application/json", ...(opts.headers || {}) },
    ...opts,
  });
  if (!res.ok) {
    let detail = `HTTP ${res.status}`;
    try {
      const body = await res.json();
      if (Array.isArray(body.detail)) {
        detail = body.detail.map((e) => e.msg || JSON.stringify(e)).join("; ");
      } else {
        detail = body.detail || detail;
      }
    } catch {}
    throw new Error(detail);
  }
  return res.json();
}

export function qs(params) {
  const u = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === null || v === undefined || v === "") continue;
    if (Array.isArray(v)) { v.forEach((x) => u.append(k, x)); }
    else { u.set(k, v); }
  }
  return u.toString();
}

let _toastTimer = null;
export function toast(msg, kind = "info") {
  const el = document.getElementById("toast");
  if (!el) return;
  el.textContent = msg;
  el.classList.remove("bg-inverse-surface", "bg-error", "bg-tertiary-container");
  if (kind === "error") el.classList.add("bg-error");
  else if (kind === "success") el.classList.add("bg-tertiary-container");
  else el.classList.add("bg-inverse-surface");
  el.style.opacity = "1";
  el.style.pointerEvents = "auto";
  if (_toastTimer) clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => {
    el.style.opacity = "0";
    el.style.pointerEvents = "none";
  }, 3200);
}

function toLocalISOString(isoUtc) {
  const d = new Date(isoUtc);
  const pad = n => String(n).padStart(2, "0");
  const offsetMin = -d.getTimezoneOffset();
  const sign = offsetMin >= 0 ? "+" : "-";
  const abs = Math.abs(offsetMin);
  const off = `${sign}${pad(Math.floor(abs / 60))}:${pad(abs % 60)}`;
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}${off}`;
}

// Data freshness indicator in top bar
async function refreshDataStatus() {
  const el = document.getElementById("data-status");
  if (!el) return;
  try {
    const r = await fetch("/health/data");
    const body = await r.json().catch(() => ({}));
    const dotOk  = '<span class="text-tertiary-container">●</span>';
    const dotErr = '<span class="text-error">●</span>';
    if (r.ok) {
      const raw = body.data_datetime || body.file_date || "—";
      const dt  = raw.includes("T") ? toLocalISOString(raw) : raw;
      const src = body.source === "realtime" ? "near realtime" : (body.source || "—");
      el.innerHTML = `${dotOk} ${src} · ${dt}`;
      el.className = "hidden md:inline-flex items-center gap-1 text-[11px] font-label text-on-surface-variant";
    } else {
      el.innerHTML = `${dotErr} datos ${body.file_date || "obsoletos"}`;
      el.className = "hidden md:inline-flex items-center gap-1 text-[11px] font-label text-on-surface-variant";
    }
  } catch {
    el.textContent = "";
  }
}

document.addEventListener("DOMContentLoaded", () => {
  refreshDataStatus();
});
