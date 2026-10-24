import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { changeOverDays, coverageStart, priceChange, regionGapHeadline, trendHeadline } from "../../app/web/static/js/insights_story.js";
import { formatDay, formatSignedPriceDelta } from "../../app/web/static/js/format.js";

const pts = (...pairs) => pairs.map(([date, avg_price]) => ({ date, avg_price }));

describe("priceChange", () => {
    it("is last minus first", () => assert.ok(Math.abs(priceChange(pts(["2026-10-01", 1.6], ["2026-10-23", 1.532])) + 0.068) < 1e-9));
    it("is null with fewer than two points", () => assert.equal(priceChange(pts(["2026-10-23", 1.5])), null));
    it("is null without data", () => assert.equal(priceChange(null), null));
});

describe("changeOverDays", () => {
    const series = pts(["2026-10-14", 1.55], ["2026-10-16", 1.54], ["2026-10-20", 1.535], ["2026-10-23", 1.529]);
    it("compares with the closest point on or before the cutoff", () =>
        assert.ok(Math.abs(changeOverDays(series, 7) - (1.529 - 1.54)) < 1e-9));
    it("is null when history is shorter than the window", () => assert.equal(changeOverDays(series, 30), null));
});

describe("trendHeadline", () => {
    it("says how much gasoline dropped", () =>
        assert.equal(trendHeadline("gasoline_95_e5_price", "quarter", -0.068), "La gasolina 95 ha bajado 0,068 €/l en los últimos 3 meses"));
    it("says how much diesel rose", () =>
        assert.equal(trendHeadline("diesel_a_price", "month", 0.012), "El diésel ha subido 0,012 €/l en el último mes"));
    it("reads tiny changes as stable", () =>
        assert.equal(trendHeadline("diesel_a_price", "year", 0.0004), "El diésel se ha mantenido estable en el último año"));
    it("handles missing data", () =>
        assert.equal(trendHeadline("gasoline_95_e5_price", "half_year", null), "La gasolina 95: sin datos suficientes en los últimos 6 meses"));
    it("names the first data day when history is shorter than the period", () =>
        assert.equal(trendHeadline("gasoline_95_e5_price", "year", -0.03, { since: "17 feb 2026" }), "La gasolina 95 ha bajado 0,030 €/l desde el 17 feb 2026"));
});

describe("coverageStart", () => {
    it("is the first data day when the series starts after the period start", () =>
        assert.equal(coverageStart(pts(["2026-02-17 00:00:00", 1.5], ["2026-04-17 00:00:00", 1.4]), "2025-04-18"), "2026-02-17"));
    it("is null when the series covers the whole period", () =>
        assert.equal(coverageStart(pts(["2026-01-18", 1.5], ["2026-04-17", 1.4]), "2026-01-18"), null));
    it("is null without data or window", () => {
        assert.equal(coverageStart([], "2026-01-18"), null);
        assert.equal(coverageStart(pts(["2026-01-18", 1.5]), null), null);
    });
});

describe("formatDay", () => {
    it("formats a data day in Spanish without shifting it", () => assert.equal(formatDay("2026-02-17"), "17 feb 2026"));
    it("shows a dash without a date", () => assert.equal(formatDay(null), "—"));
});

describe("regionGapHeadline", () => {
    it("says how much cheaper the islands are", () =>
        assert.equal(
            regionGapHeadline("gasoline_95_e5_price", -0.175),
            "En las islas, Ceuta y Melilla la gasolina 95 cuesta 0,175 €/l menos que en la península",
        ));
    it("says when they are dearer", () =>
        assert.equal(regionGapHeadline("diesel_a_price", 0.02), "En las islas, Ceuta y Melilla el diésel cuesta 0,020 €/l más que en la península"));
    it("reads a rounded-away gap as equal", () =>
        assert.equal(regionGapHeadline("diesel_a_price", 0.0002), "En las islas, Ceuta y Melilla el diésel cuesta lo mismo que en la península"));
    it("handles missing data", () =>
        assert.equal(regionGapHeadline("diesel_a_price", null), "Península vs. islas, Ceuta y Melilla: sin datos suficientes para el diésel"));
});

describe("formatSignedPriceDelta", () => {
    it("uses euros with a true minus sign", () => assert.equal(formatSignedPriceDelta(-0.012), "−0,012 €/l"));
    it("adds a plus for rises", () => assert.equal(formatSignedPriceDelta(0.077), "+0,077 €/l"));
    it("drops the sign when it rounds to zero", () => assert.equal(formatSignedPriceDelta(-0.0004), "0,000 €/l"));
    it("returns — for non-numbers", () => assert.equal(formatSignedPriceDelta(null), "—"));
});
