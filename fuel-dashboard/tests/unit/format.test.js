import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
    displaySign,
    formatDelta,
    formatFreshness,
    formatMin,
    formatNum,
    formatPrice,
    formatSignedPct,
    titleCase,
} from "../../app/web/static/js/format.js";

describe("formatMin", () => {
    it("returns — for null", () => assert.equal(formatMin(null), "—"));
    it("returns — for undefined", () => assert.equal(formatMin(undefined), "—"));
    it("returns — for NaN", () => assert.equal(formatMin(NaN), "—"));
    it("returns — for numeric string", () => assert.equal(formatMin("70"), "—"));
    it("formats minutes under 60", () => assert.equal(formatMin(45), "45 min"));
    it("rounds fractional minutes under 60", () => assert.equal(formatMin(44.6), "45 min"));
    it("formats exactly 60 minutes as 1h", () => assert.equal(formatMin(60), "1h"));
    it("formats 70 minutes as 1h 10min", () => assert.equal(formatMin(70), "1h 10min"));
    it("formats 125 minutes as 2h 5min", () => assert.equal(formatMin(125), "2h 5min"));
    it("rounds fractional minutes above 60", () => assert.equal(formatMin(69.6), "1h 10min"));
    it("formats 120 minutes as 2h (no trailing 0min)", () => assert.equal(formatMin(120), "2h"));
});

describe("formatPrice", () => {
    it("formats €/l with es-ES comma decimals", () => assert.equal(formatPrice(1.431), "1,431\u00a0€/l"));
    it("returns — for null", () => assert.equal(formatPrice(null), "—"));
});

describe("formatNum", () => {
    it("uses a comma decimal", () => assert.equal(formatNum(48, 1), "48,0"));
    it("respects digits", () => assert.equal(formatNum(0.6243, 2), "0,62"));
    it("returns — for undefined", () => assert.equal(formatNum(undefined), "—"));
});

describe("formatSignedPct", () => {
    it("uses a true minus sign and comma", () => assert.equal(formatSignedPct(-2.4), "\u22122,4%"));
    it("prefixes + for positive values", () => assert.equal(formatSignedPct(0.6), "+0,6%"));
    it("has no sign for zero", () => assert.equal(formatSignedPct(0), "0,0%"));
    it("supports 2 digits", () => assert.equal(formatSignedPct(-1.834, 2), "\u22121,83%"));
    it("returns — for null", () => assert.equal(formatSignedPct(null), "—"));
    it("drops the sign when the value rounds to zero", () => assert.equal(formatSignedPct(-0.04), "0,0%"));
    it("rounds before choosing the sign", () => assert.equal(formatSignedPct(0.049, 1), "0,0%"));
    it("formatDelta keeps 2 digits", () => assert.equal(formatDelta(2.4), "+2,40%"));
    it("rounds negative halves away from zero like positive ones", () => {
        assert.equal(formatSignedPct(0.25), "+0,3%");
        assert.equal(formatSignedPct(-0.25), "\u22120,3%");
        assert.equal(formatDelta(-0.125), "\u22120,13%");
    });
});

describe("displaySign", () => {
    it("is 0 when the value rounds to zero", () => assert.equal(displaySign(-0.03), 0));
    it("follows the sign otherwise", () => {
        assert.equal(displaySign(-0.06), -1);
        assert.equal(displaySign(1.2), 1);
    });
    it("respects digits", () => assert.equal(displaySign(-0.002, 2), 0));
    it("is 0 for null", () => assert.equal(displaySign(null), 0));
});

describe("formatFreshness", () => {
    const now = new Date(2026, 3, 17, 12, 0);
    const iso = (h, m, day = 17) => new Date(2026, 3, day, h, m).toISOString();
    it("shows realtime with today's time", () =>
        assert.equal(formatFreshness(iso(9, 30), { realtime: true, now }), "Tiempo real · hoy 09:30"));
    it("shows ayer for yesterday", () =>
        assert.equal(formatFreshness(iso(9, 30, 16), { realtime: true, now }), "Tiempo real · ayer 09:30"));
    it("shows a daily file date", () => assert.equal(formatFreshness("2026-04-14", { now }), "Datos del 14 abr"));
    it("shows Datos de hoy for today's file", () => assert.equal(formatFreshness("2026-04-17", { now }), "Datos de hoy"));
    it("marks stale data", () =>
        assert.equal(formatFreshness("2026-04-14", { stale: true, now }), "Datos del 14 abr (sin actualizar)"));
    it("treats a slightly future timestamp as today", () =>
        assert.equal(formatFreshness(iso(0, 30, 18), { realtime: true, now }), "Tiempo real · hoy 00:30"));
    it("drops the time for non-realtime ISO timestamps", () =>
        assert.equal(formatFreshness(iso(9, 30), { now }), "Datos de hoy"));
    it("shows a date (not hoy) for timestamps more than a day ahead", () =>
        assert.equal(formatFreshness("2026-04-20", { now }), "Datos del 20 abr"));
    it("adds the year for data from another year", () =>
        assert.equal(formatFreshness("2025-04-14", { now }), "Datos del 14 abr 2025"));
    it("returns null for an unusable date on a healthy response", () => {
        assert.equal(formatFreshness("unknown", { now }), null);
        assert.equal(formatFreshness(undefined, { now }), null);
    });
    it("handles a missing stale date", () => assert.equal(formatFreshness(undefined, { stale: true }), "Datos no actualizados"));
});

describe("titleCase", () => {
    it("capitalises each word", () => assert.equal(titleCase("santa cruz de tenerife"), "Santa Cruz De Tenerife"));
    it("does not capitalise after accented letters", () => assert.equal(titleCase("león"), "León"));
    it("handles ñ", () => assert.equal(titleCase("a coruña"), "A Coruña"));
});
