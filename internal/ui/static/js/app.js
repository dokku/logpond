// Logpond browser-side glue.
//
// Alpine components consumed by the three views. The pattern: each
// component is a small factory returning an Alpine x-data block. HTMX
// handles all server round-trips; this file only manages local state
// (theme, focus, pause, etc.).

document.addEventListener("alpine:init", () => {
  Alpine.data("themeController", () => ({
    theme: localStorage.getItem("logpond.theme") || "auto",
    apply() {
      this._sync();
      this.$el.addEventListener("theme-cycle", () => this.cycle());
    },
    cycle() {
      const order = ["auto", "light", "dark"];
      const i = order.indexOf(this.theme);
      this.theme = order[(i + 1) % order.length];
      localStorage.setItem("logpond.theme", this.theme);
      this._sync();
    },
    _sync() {
      if (this.theme === "auto") {
        this.$el.removeAttribute("data-theme");
      } else {
        this.$el.setAttribute("data-theme", this.theme);
      }
    },
  }));

  Alpine.data("searchView", () => ({
    timePreset: "1h",
    customFrom: "",
    customTo: "",
    init() {
      document.addEventListener("facet-toggle", (e) => this.onFacetToggle(e.detail));
    },
    onFacetToggle(detail) {
      const input = document.querySelector("input[name='q']");
      if (!input) return;
      const current = input.value.trim();
      const token = composeFacetToken(detail.field, detail.value);
      let next;
      if (detail.checked) {
        next = current ? current + " " + token : token;
      } else {
        next = stripFacetToken(current, token);
      }
      input.value = next;
      input.dispatchEvent(new Event("input", { bubbles: true }));
      const form = input.closest("form");
      if (form) htmx.trigger(form, "submit");
    },
  }));

  Alpine.data("searchBar", () => ({
    q: "",
    focused: false,
    activeIndex: 0,
    init() {
      this.q = this.$refs.input ? this.$refs.input.value : "";
    },
    cursorPos() {
      return this.$refs.input ? this.$refs.input.selectionStart : 0;
    },
    clear() {
      this.q = "";
      if (this.$refs.input) {
        this.$refs.input.value = "";
        this.$refs.input.dispatchEvent(new Event("input", { bubbles: true }));
        this.$refs.input.focus();
      }
    },
    onKeydown(e) {
      const list = document.querySelector("#suggestions .suggestion-list");
      if (!list) return;
      const items = list.querySelectorAll(".suggestion");
      switch (e.key) {
        case "ArrowDown":
          e.preventDefault();
          this.activeIndex = Math.min(this.activeIndex + 1, items.length - 1);
          break;
        case "ArrowUp":
          e.preventDefault();
          this.activeIndex = Math.max(this.activeIndex - 1, 0);
          break;
        case "Enter":
        case "Tab":
          if (items.length > 0 && this.focused) {
            e.preventDefault();
            const sel = items[this.activeIndex];
            if (sel) this.applySuggestion(sel);
          }
          break;
        case "Escape":
          this.focused = false;
          break;
      }
    },
    applySuggestion(el) {
      const text = el.getAttribute("data-text");
      const kind = el.getAttribute("data-kind");
      const input = this.$refs.input;
      if (!input || !text) return;
      const before = input.value.slice(0, input.selectionStart);
      const after = input.value.slice(input.selectionStart);
      // Replace the partial token at the cursor with the suggestion.
      const partial = before.match(/[A-Za-z0-9_.\-@]*$/)[0];
      const head = before.slice(0, before.length - partial.length);
      const suffix = kind === "field" || kind === "core" || kind === "custom" ? ":" : " ";
      input.value = head + text + suffix + after;
      const newPos = (head + text + suffix).length;
      input.setSelectionRange(newPos, newPos);
      this.q = input.value;
      this.focused = false;
      input.focus();
    },
    select(text, kind) {
      const items = document.querySelectorAll("#suggestions .suggestion");
      for (const el of items) {
        if (el.getAttribute("data-text") === text) {
          this.applySuggestion(el);
          return;
        }
      }
    },
  }));

  Alpine.data("tailView", () => ({
    running: false,
    paused: false,
    status: "idle",
    dropped: 0,
    canStart() {
      const input = document.querySelector(".search-bar input[name='q']");
      return input && input.value.trim().length > 0;
    },
    start() {
      const input = document.querySelector(".search-bar input[name='q']");
      if (!input || !this.canStart()) return;
      this.running = true;
      this.status = "connecting";
      // The WebSocket element is wired via HTMX-ws extension; we kick a
      // ws-send by dispatching a synthetic message containing the
      // current query. The WS endpoint expects JSON {q: "..."}.
      const ws = document.querySelector("#tail-events");
      if (!ws) return;
      const payload = JSON.stringify({ q: input.value.trim() });
      const detail = { message: payload };
      ws.dispatchEvent(new CustomEvent("htmx:wsBeforeSend", { detail }));
      // HTMX-ws sends on form submit; build a one-off WebSocket for the
      // subscription frame separately.
      if (ws._wsSubmit) ws._wsSubmit(payload);
      this.status = "live";
    },
    stop() {
      this.running = false;
      this.status = "idle";
      const ws = document.querySelector("#tail-events");
      if (ws && ws._lpClose) ws._lpClose();
    },
    togglePause() {
      this.paused = !this.paused;
    },
    clearEvents() {
      const el = this.$refs.events;
      if (el) {
        // Keep the placeholder; remove appended rows.
        el.querySelectorAll(".tail-row").forEach((n) => n.remove());
      }
    },
  }));
});

function composeFacetToken(field, value) {
  // Quote the value if it contains whitespace or punctuation.
  if (/[\s"():]/.test(value)) {
    return field + ':"' + value.replace(/"/g, '\\"') + '"';
  }
  return field + ":" + value;
}

function stripFacetToken(query, token) {
  const idx = query.indexOf(token);
  if (idx === -1) return query;
  let next = query.slice(0, idx) + query.slice(idx + token.length);
  next = next.replace(/\s{2,}/g, " ").trim();
  return next;
}

// Render data-ts attributes into local-time on the user's clock. The
// timestamp ISO sits in the DOM; this helper rewrites the visible text
// after the page parses. Re-runs on htmx:afterSwap so newly-inserted
// rows pick up the same treatment.
function renderLocalTimes(root) {
  (root || document).querySelectorAll("time[data-ts]").forEach((el) => {
    const iso = el.getAttribute("data-ts");
    if (!iso) return;
    const d = new Date(iso);
    if (isNaN(d.getTime())) return;
    const hh = String(d.getHours()).padStart(2, "0");
    const mm = String(d.getMinutes()).padStart(2, "0");
    const ss = String(d.getSeconds()).padStart(2, "0");
    const ms = String(d.getMilliseconds()).padStart(3, "0");
    el.textContent = `${hh}:${mm}:${ss}.${ms}`;
    if (!el.title) el.title = iso + " (UTC)";
  });
}

document.addEventListener("DOMContentLoaded", () => renderLocalTimes(document));
document.body && document.body.addEventListener("htmx:afterSwap", (e) => renderLocalTimes(e.target));
