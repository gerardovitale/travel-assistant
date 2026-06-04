// Generic, reusable share component for the whole dashboard. Page-agnostic.
//
// Wire any element with a `data-share="<key>"` attribute. Its payload (title/text/url) is resolved,
// in priority order, from either:
//   1. A registered builder — registerShareBuilder(key, () => ({ title, text, url })) — for pages
//      that compute the payload dynamically at click time (e.g. insights with live filters).
//   2. Static attributes — data-share-title / data-share-text / data-share-url. Omit url to default
//      to the current location. Zero-JS pages just set these attributes.
//
// Behaviour: native share sheet when available (mobile), otherwise an anchored popover with
// copy-link + WhatsApp + Telegram + X.
import { toast } from "./app.js";

const builders = new Map();

export function registerShareBuilder(key, fn) {
  builders.set(key, fn);
}

function absoluteUrl(url) {
  if (!url) return location.href;
  try {
    return new URL(url, location.origin).href;
  } catch {
    return location.href;
  }
}

function resolvePayload(btn) {
  const key = btn.dataset.share;
  const builder = key && builders.get(key);
  if (builder) {
    try {
      const p = builder() || {};
      return {
        title: p.title || document.title,
        text: p.text || "",
        url: absoluteUrl(p.url),
      };
    } catch {
      /* fall through to static attributes */
    }
  }
  return {
    title: btn.dataset.shareTitle || document.title,
    text: btn.dataset.shareText || "",
    url: absoluteUrl(btn.dataset.shareUrl),
  };
}

const INTENTS = {
  whatsapp: ({ text, url }) => `https://wa.me/?text=${encodeURIComponent((text ? text + " " : "") + url)}`,
  telegram: ({ text, url }) => `https://t.me/share/url?url=${encodeURIComponent(url)}&text=${encodeURIComponent(text)}`,
  x: ({ text, url }) => `https://twitter.com/intent/tweet?text=${encodeURIComponent(text)}&url=${encodeURIComponent(url)}`,
};

async function copyLink(url) {
  try {
    await navigator.clipboard.writeText(url);
    toast("Enlace copiado", "success");
    return;
  } catch {
    /* fall back to legacy execCommand below */
  }
  const ta = document.createElement("textarea");
  ta.value = url;
  ta.style.position = "fixed";
  ta.style.opacity = "0";
  document.body.appendChild(ta);
  ta.select();
  try {
    document.execCommand("copy");
    toast("Enlace copiado", "success");
  } catch {
    toast("No se pudo copiar el enlace", "error");
  }
  document.body.removeChild(ta);
}

let openPopover = null;
let openTrigger = null;

function closePopover() {
  if (openPopover) {
    openPopover.remove();
    openPopover = null;
  }
  if (openTrigger) {
    openTrigger.setAttribute("aria-expanded", "false");
    openTrigger = null;
  }
  document.removeEventListener("click", onOutsideClick, true);
  document.removeEventListener("keydown", onKeydown, true);
}

function onOutsideClick(e) {
  if (openPopover && !openPopover.contains(e.target)) closePopover();
}

function onKeydown(e) {
  if (e.key === "Escape") {
    const trigger = openTrigger;
    closePopover();
    trigger?.focus();
  }
}

function popoverItem(icon, label, onClick) {
  const b = document.createElement("button");
  b.type = "button";
  b.setAttribute("role", "menuitem");
  b.className =
    "w-full flex items-center gap-3 px-3 py-2 rounded-xl hover:bg-surface-container text-left text-on-surface";
  b.innerHTML = `<span class="material-symbols-outlined text-[20px] text-primary-container">${icon}</span><span class="font-medium">${label}</span>`;
  b.addEventListener("click", () => {
    onClick();
    closePopover();
  });
  return b;
}

function buildPopover(btn, payload) {
  closePopover();
  const pop = document.createElement("div");
  pop.setAttribute("role", "menu");
  pop.className =
    "absolute z-[70] mt-2 right-0 min-w-[12rem] bg-surface-container-lowest border border-outline-variant/50 rounded-2xl shadow-2xl p-1.5 text-sm";
  pop.appendChild(popoverItem("link", "Copiar enlace", () => copyLink(payload.url)));
  pop.appendChild(popoverItem("chat", "WhatsApp", () => window.open(INTENTS.whatsapp(payload), "_blank", "noopener")));
  pop.appendChild(popoverItem("send", "Telegram", () => window.open(INTENTS.telegram(payload), "_blank", "noopener")));
  pop.appendChild(popoverItem("alternate_email", "X", () => window.open(INTENTS.x(payload), "_blank", "noopener")));

  const wrap = btn.closest("[data-share-anchor]") || btn.parentElement;
  if (wrap && !wrap.style.position) wrap.style.position = "relative";
  (wrap || document.body).appendChild(pop);
  openPopover = pop;
  openTrigger = btn;
  btn.setAttribute("aria-expanded", "true");
  pop.querySelector('[role="menuitem"]')?.focus();
  // Defer listener attach so the click that opened the popover doesn't immediately close it.
  setTimeout(() => {
    document.addEventListener("click", onOutsideClick, true);
    document.addEventListener("keydown", onKeydown, true);
  }, 0);
}

async function onShareClick(e) {
  const btn = e.currentTarget;
  const payload = resolvePayload(btn);
  if (navigator.share) {
    try {
      await navigator.share({ title: payload.title, text: payload.text, url: payload.url });
      return;
    } catch (err) {
      if (err && err.name === "AbortError") return;
      /* otherwise fall back to popover */
    }
  }
  buildPopover(btn, payload);
}

// Idempotent: safe to call repeatedly after dynamic re-renders.
export function initShare(root = document) {
  root.querySelectorAll("[data-share]").forEach((btn) => {
    if (btn.dataset.shareWired === "1") return;
    btn.dataset.shareWired = "1";
    btn.addEventListener("click", onShareClick);
  });
}

document.addEventListener("DOMContentLoaded", () => initShare(document));
