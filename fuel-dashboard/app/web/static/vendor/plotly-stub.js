(function () {
  window.Plotly = {
    newPlot(el, traces, layout, config) {
      el.dataset.plotReady = "true";
      el.dataset.plotTraces = String((traces || []).length);
      el.dataset.plotTitle = layout && layout.title ? String(layout.title) : "";
      // Record each trace's visibility so a diff-toggle assertion has a value to read.
      (traces || []).forEach((t, i) => {
        el.dataset[`trace${i}Visible`] = String(t.visible === undefined ? true : t.visible);
      });
      // Same for the secondary (%) axis, hidden along with its trace when a diff toggle is off.
      if (layout && layout.yaxis2) el.dataset.yaxis2Visible = String(layout.yaxis2.visible !== false);
      el.innerHTML = '<div data-testid="plotly-stub"></div>';
      return Promise.resolve();
    },
  };
})();
