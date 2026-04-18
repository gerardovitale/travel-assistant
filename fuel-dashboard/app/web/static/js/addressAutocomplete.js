const DEBOUNCE_MS = 300;
const MIN_CHARS = 3;

function debounce(fn, ms) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), ms);
  };
}

export function attachAutocomplete(input) {
  if (!input) return;

  // Wrap input in a relative-positioned container so the dropdown anchors to it
  const wrapper = document.createElement("div");
  const hasFlex1 = input.classList.contains("flex-1");
  wrapper.className = "relative" + (hasFlex1 ? " flex-1" : "");
  input.parentNode.insertBefore(wrapper, input);
  wrapper.appendChild(input);
  if (hasFlex1) {
    input.classList.remove("flex-1");
    input.classList.add("w-full");
  }

  const dropdown = document.createElement("ul");
  dropdown.className =
    "absolute z-[500] left-0 right-0 mt-1 bg-white rounded-lg border border-outline-variant shadow-xl max-h-56 overflow-y-auto hidden";
  wrapper.appendChild(dropdown);

  let activeIndex = -1;
  let currentSuggestions = [];

  function showDropdown(suggestions) {
    currentSuggestions = suggestions;
    activeIndex = -1;
    dropdown.innerHTML = "";
    if (!suggestions.length) {
      dropdown.classList.add("hidden");
      return;
    }
    suggestions.forEach((s, i) => {
      const li = document.createElement("li");
      li.className =
        "flex items-center gap-2 px-3 py-2 hover:bg-surface-container-low cursor-pointer text-sm text-on-surface";
      li.textContent = s.display_name;
      li.addEventListener("mousedown", (e) => {
        e.preventDefault();
        selectSuggestion(i);
      });
      dropdown.appendChild(li);
    });
    dropdown.classList.remove("hidden");
  }

  function hideDropdown() {
    dropdown.classList.add("hidden");
    activeIndex = -1;
    currentSuggestions = [];
  }

  function selectSuggestion(index) {
    const s = currentSuggestions[index];
    if (!s) return;
    input.value = s.display_name;
    hideDropdown();
  }

  function highlightItem(index) {
    const items = dropdown.querySelectorAll("li");
    items.forEach((li, i) => {
      li.classList.toggle("bg-surface-container-low", i === index);
    });
  }

  function buildDisplayName(props) {
    const parts = [props.name, props.street, props.city, props.state];
    const seen = new Set();
    const unique = [];
    for (const p of parts) {
      if (p && !seen.has(p)) { seen.add(p); unique.push(p); }
    }
    return unique.join(", ");
  }

  async function fetchSuggestions(query) {
    try {
      const url = new URL("https://photon.komoot.io/api/");
      url.searchParams.set("q", query);
      url.searchParams.set("limit", "5");
      url.searchParams.set("bbox", "-9.3,35.9,4.3,43.8");
      const resp = await fetch(url);
      if (!resp.ok) return [];
      const data = await resp.json();
      const suggestions = [];
      for (const f of (data.features || [])) {
        const props = f.properties || {};
        if (props.countrycode !== "ES") continue;
        const [lon, lat] = f.geometry?.coordinates || [];
        if (lat == null) continue;
        const display_name = buildDisplayName(props);
        if (display_name) suggestions.push({ display_name, lat, lon });
      }
      return suggestions;
    } catch {
      return [];
    }
  }

  const onInput = debounce(async () => {
    const query = input.value.trim();
    if (query.length < MIN_CHARS) {
      hideDropdown();
      return;
    }
    const suggestions = await fetchSuggestions(query);
    showDropdown(suggestions);
  }, DEBOUNCE_MS);

  input.addEventListener("input", onInput);

  input.addEventListener("keydown", (e) => {
    const items = dropdown.querySelectorAll("li");
    if (!items.length) return;
    if (e.key === "ArrowDown") {
      e.preventDefault();
      activeIndex = Math.min(activeIndex + 1, items.length - 1);
      highlightItem(activeIndex);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      activeIndex = activeIndex <= 0 ? items.length - 1 : activeIndex - 1;
      highlightItem(activeIndex);
    } else if (e.key === "Enter" && activeIndex >= 0) {
      e.preventDefault();
      selectSuggestion(activeIndex);
    } else if (e.key === "Escape") {
      hideDropdown();
    }
  });

  input.addEventListener("blur", () => {
    // Small delay so mousedown on a suggestion fires before blur hides the dropdown
    setTimeout(hideDropdown, 150);
  });
}
