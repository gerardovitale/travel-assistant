export function initBrandsDropdown(toggleId, listId) {
  const toggle = document.getElementById(toggleId);
  const list = document.getElementById(listId);
  if (!toggle || !list) return;

  let isOpen = false;
  function setOpen(val) {
    isOpen = val;
    list.style.display = isOpen ? "block" : "none";
  }

  toggle.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();
    setOpen(!isOpen);
  });

  list.addEventListener("click", (e) => {
    e.stopPropagation();
  });

  document.addEventListener("click", () => {
    if (isOpen) setOpen(false);
  });
}

export function populateBrandsList(listId, labelId, selectedSet, labelsMap) {
  const list = document.getElementById(listId);
  const labelEl = document.getElementById(labelId);
  if (!list || !labelEl) return;

  for (const [raw, pretty] of Object.entries(labelsMap)) {
    const item = document.createElement("label");
    item.dataset.testid = `brand-option-${raw}`;
    item.className = "flex items-center gap-2 px-3 py-2 hover:bg-surface-container-low cursor-pointer text-sm";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.value = raw;
    cb.dataset.testid = `brand-checkbox-${raw}`;
    cb.className = "rounded border-outline-variant text-primary-container focus:ring-primary-container/40";
    cb.addEventListener("change", () => {
      if (cb.checked) selectedSet.add(raw);
      else selectedSet.delete(raw);
      updateBrandsLabel(labelEl, selectedSet);
    });
    const span = document.createElement("span");
    span.textContent = pretty;
    item.appendChild(cb);
    item.appendChild(span);
    list.appendChild(item);
  }
}

export function updateBrandsLabel(el, selectedSet) {
  const n = selectedSet.size;
  if (n === 0) el.textContent = "Todas las marcas";
  else if (n === 1) el.textContent = "1 marca seleccionada";
  else el.textContent = `${n} marcas seleccionadas`;
  el.classList.toggle("text-outline", n === 0);
  el.classList.toggle("text-on-surface", n > 0);
}

export function getSelectedLabels(selectedSet) {
  return selectedSet.size > 0 ? [...selectedSet] : [];
}
