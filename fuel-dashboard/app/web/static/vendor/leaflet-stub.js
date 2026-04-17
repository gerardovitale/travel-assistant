(function () {
  function createLayer() {
    return {
      addTo(target) {
        if (target && typeof target._addLayer === "function") target._addLayer(this);
        return this;
      },
      bindPopup() { return this; },
      bindTooltip() { return this; },
      setStyle() { return this; },
      setRadius() { return this; },
      clearLayers() { this._layers = []; return this; },
      getBounds() {
        return {
          isValid() { return true; },
          contains() { return true; },
        };
      },
    };
  }

  function createMap() {
    const state = { layers: [] };
    return {
      setView() { return this; },
      fitBounds() { return this; },
      invalidateSize() { return this; },
      removeLayer(layer) {
        state.layers = state.layers.filter((item) => item !== layer);
        return this;
      },
      getBounds() {
        return {
          contains() { return true; },
        };
      },
      panTo() { return this; },
      _addLayer(layer) {
        state.layers.push(layer);
      },
    };
  }

  const L = {
    map() {
      return createMap();
    },
    tileLayer() {
      return createLayer();
    },
    layerGroup(layers = []) {
      const layer = createLayer();
      layer._layers = layers.slice();
      layer.addLayer = function addLayer(child) {
        this._layers.push(child);
        return this;
      };
      return layer;
    },
    circleMarker() {
      return createLayer();
    },
    marker() {
      return createLayer();
    },
    polyline() {
      return createLayer();
    },
    geoJSON() {
      return createLayer();
    },
    divIcon(options) {
      return options;
    },
    latLngBounds() {
      return {
        isValid() { return true; },
        contains() { return true; },
      };
    },
  };

  window.L = L;
})();
