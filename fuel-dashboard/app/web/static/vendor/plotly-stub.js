(function () {
  window.Plotly = {
    newPlot(el, traces, layout, config) {
      el.dataset.plotReady = "true";
      el.dataset.plotTraces = String((traces || []).length);
      el.dataset.plotTitle = layout && layout.title ? String(layout.title) : "";
      // Record each trace's visibility so a diff-toggle assertion has a value to read.
      (traces || []).forEach((t, i) => {
        el.dataset[`trace${i}Visible`] = String(t.visible === undefined ? true : t.visible);
        // First/last x and point count, so date-range assertions can check the data window.
        const x = t.x || [];
        el.dataset[`trace${i}Points`] = String(x.length);
        if (x.length) el.dataset[`trace${i}X`] = `${x[0]}..${x[x.length - 1]}`;
      });
      // Same for the secondary (%) axis, hidden along with its trace when a diff toggle is off.
      if (layout && layout.yaxis2) el.dataset.yaxis2Visible = String(layout.yaxis2.visible !== false);
      // An explicit x-axis range ("start..end"); empty when Plotly would autorange.
      const range = layout && layout.xaxis && layout.xaxis.range;
      el.dataset.xRange = range ? range.join("..") : "";
      el.innerHTML = '<div data-testid="plotly-stub"></div>';
      return Promise.resolve();
    },
  };
})();
