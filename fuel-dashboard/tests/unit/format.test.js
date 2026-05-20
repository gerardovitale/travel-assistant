import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { formatMin } from "../../app/web/static/js/format.js";

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
