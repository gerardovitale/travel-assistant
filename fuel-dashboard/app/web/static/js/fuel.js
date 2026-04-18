import { api } from "./app.js";

// Human labels in Spanish for fuel columns.
export const FUEL_LABELS = {
  diesel_a_price: "Diésel A",
  diesel_b_price: "Diésel B",
  diesel_premium_price: "Diésel Premium",
  gasoline_95_e5_price: "Gasolina 95 E5",
  gasoline_95_e10_price: "Gasolina 95 E10",
  gasoline_95_e5_premium_price: "Gasolina 95 Premium",
  gasoline_98_e5_price: "Gasolina 98 E5",
  gasoline_98_e10_price: "Gasolina 98 E10",
  biodiesel_price: "Biodiésel",
  bioethanol_price: "Bioetanol",
  compressed_natural_gas_price: "GNC",
  liquefied_natural_gas_price: "GNL",
  liquefied_petroleum_gases_price: "GLP",
  hydrogen_price: "Hidrógeno",
};

export const GROUP_LABELS = {
  diesel: "Diésel",
  gasoline_95: "Gasolina 95",
  gasoline_98: "Gasolina 98",
  biofuel: "Biocombustible",
  natural_gas: "Gas natural",
};

let _catalog = null;
export async function getCatalog() {
  if (_catalog) return _catalog;
  _catalog = await api("/fuel/catalog");
  return _catalog;
}

export async function populateFuelSelect(selectEl, { defaultValue = "gasoline_95_e5_price" } = {}) {
  const cat = await getCatalog();
  selectEl.innerHTML = "";

  // Groups
  for (const [group, members] of Object.entries(cat.groups)) {
    const og = document.createElement("optgroup");
    og.label = GROUP_LABELS[group] || group;
    for (const fuel of members) {
      const opt = document.createElement("option");
      opt.value = fuel;
      opt.textContent = FUEL_LABELS[fuel] || fuel;
      og.appendChild(opt);
    }
    selectEl.appendChild(og);
  }
  // Singletons
  if (cat.singletons?.length) {
    const og = document.createElement("optgroup");
    og.label = "Otros";
    for (const fuel of cat.singletons) {
      const opt = document.createElement("option");
      opt.value = fuel;
      opt.textContent = FUEL_LABELS[fuel] || fuel;
      og.appendChild(opt);
    }
    selectEl.appendChild(og);
  }
  if (defaultValue) selectEl.value = defaultValue;
}

export async function populateGroupSelect(selectEl, { defaultValue = "gasoline_95" } = {}) {
  const cat = await getCatalog();
  selectEl.innerHTML = "";
  for (const group of Object.keys(cat.groups)) {
    const opt = document.createElement("option");
    opt.value = group;
    opt.textContent = GROUP_LABELS[group] || group;
    selectEl.appendChild(opt);
  }
  if (defaultValue) selectEl.value = defaultValue;
}

let _labels = null;
export async function getLabels() {
  if (_labels) return _labels;
  const r = await api("/labels?top_n=25");
  _labels = r.labels;
  return _labels;
}

let _provinces = null;
export async function getProvinces() {
  if (_provinces) return _provinces;
  const r = await api("/provinces");
  _provinces = r.provinces;
  return _provinces;
}
