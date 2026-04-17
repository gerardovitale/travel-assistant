(function () {
  window.Plotly = {
    newPlot(el, traces, layout, config) {
      el.dataset.plotReady = "true";
      el.dataset.plotTraces = String((traces || []).length);
      el.dataset.plotTitle = layout && layout.title ? String(layout.title) : "";
      el.innerHTML = '<div data-testid="plotly-stub"></div>';
      return Promise.resolve();
    },
  };
})();
