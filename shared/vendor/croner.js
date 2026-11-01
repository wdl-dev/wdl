// node_modules/croner/dist/croner.js
function w(i3) {
  return Date.UTC(i3.y, i3.m - 1, i3.d, i3.h, i3.i, i3.s);
}
function C(i3, e) {
  return i3.y === e.y && i3.m === e.m && i3.d === e.d && i3.h === e.h && i3.i === e.i && i3.s === e.s;
}
function O(i3) {
  let e = i3.substring(9);
  return e.includes("Z") || e.includes("+") || e.includes("-");
}
function j(i3, e) {
  let t = new Date(Date.parse(i3));
  if (isNaN(t)) throw new Error("Invalid ISO8601 passed to timezone parser.");
  return O(i3) ? { tp: y(t.getUTCFullYear(), t.getUTCMonth() + 1, t.getUTCDate(), t.getUTCHours(), t.getUTCMinutes(), t.getUTCSeconds(), "Etc/UTC"), ms: t.getUTCMilliseconds() } : { tp: y(t.getFullYear(), t.getMonth() + 1, t.getDate(), t.getHours(), t.getMinutes(), t.getSeconds(), e), ms: t.getMilliseconds() };
}
function D(i3, e, t) {
  let { tp: n, ms: s } = j(i3, e);
  return new Date(P(n, t).getTime() + s);
}
function P(i3, e) {
  let t = v(i3);
  if (t.kind === "single") return t.date;
  if (t.kind === "ambiguous") return t.earlier;
  if (e) throw new Error("Invalid date passed to fromTZ()");
  return t.after;
}
function v(i3) {
  let e = w(i3), t = (a) => M(new Date(a), i3.tz), n = e - t(e), s = g(new Date(n), i3.tz);
  if (C(s, i3)) {
    let a = w(s) - n, o = n - (t(n - x) - a);
    if (o < n && C(g(new Date(o), i3.tz), i3)) return { kind: "ambiguous", earlier: new Date(o), later: new Date(n) };
    let u = n + (a - t(n + x));
    return u > n && C(g(new Date(u), i3.tz), i3) ? { kind: "ambiguous", earlier: new Date(n), later: new Date(u) } : { kind: "single", date: new Date(n) };
  }
  let r = e - (w(s) - n);
  return C(g(new Date(r), i3.tz), i3) ? { kind: "single", date: new Date(r) } : { kind: "gap", before: new Date(Math.min(n, r)), after: new Date(Math.max(n, r)) };
}
var x = 3 * 36e5;
function M(i3, e) {
  return w(g(i3, e)) - (i3.getTime() - i3.getUTCMilliseconds());
}
function _(i3, e, t) {
  let n = M(new Date(i3), t);
  for (; e - i3 > 1e3; ) {
    let s = i3 + Math.floor((e - i3) / 2e3) * 1e3;
    M(new Date(s), t) === n ? i3 = s : e = s;
  }
  return e;
}
function U(i3, e) {
  let t = v(g(new Date(i3), e));
  if (t.kind === "ambiguous") return i3 < t.later.getTime() ? "earlier" : "later";
}
var E = /* @__PURE__ */ new Map();
function g(i3, e) {
  if (e === void 0) return y(i3.getFullYear(), i3.getMonth() + 1, i3.getDate(), i3.getHours(), i3.getMinutes(), i3.getSeconds());
  let t = E.get(e), n;
  try {
    t || (t = new Intl.DateTimeFormat("en-US", { timeZone: e, year: "numeric", month: "numeric", day: "numeric", hour: "numeric", minute: "numeric", second: "numeric", hour12: false }), E.set(e, t)), n = t.formatToParts(i3);
  } catch (r) {
    let a = r instanceof Error ? r.message : String(r);
    throw new RangeError(`toTZ: Invalid timezone '${e}' or date. Please provide a valid IANA timezone (e.g., 'America/New_York', 'Europe/Stockholm'). Original error: ${a}`);
  }
  let s = { year: 0, month: 0, day: 0, hour: 0, minute: 0, second: 0 };
  for (let r of n) (r.type === "year" || r.type === "month" || r.type === "day" || r.type === "hour" || r.type === "minute" || r.type === "second") && (s[r.type] = parseInt(r.value, 10));
  if (isNaN(s.year) || isNaN(s.month) || isNaN(s.day) || isNaN(s.hour) || isNaN(s.minute) || isNaN(s.second)) throw new Error(`toTZ: Failed to parse all date components from timezone '${e}'. This may indicate an invalid date or timezone configuration. Parsed components: ${JSON.stringify(s)}`);
  return s.hour === 24 && (s.hour = 0), { y: s.year, m: s.month, d: s.day, h: s.hour, i: s.minute, s: s.second, tz: e };
}
function y(i3, e, t, n, s, r, a) {
  return { y: i3, m: e, d: t, h: n, i: s, s: r, tz: a };
}
var R = [1, 2, 4, 8, 16];
var b = class i {
  pattern;
  timezone;
  mode;
  alternativeWeekdays;
  sloppyRanges;
  second;
  minute;
  hour;
  day;
  month;
  dayOfWeek;
  year;
  lastDayOfMonth;
  lastWeekday;
  nearestWeekdays;
  starDOM;
  starDOW;
  starYear;
  useAndLogic;
  isFixedTime;
  constructor(e, t, n) {
    this.pattern = e, this.timezone = t, this.mode = n?.mode ?? "auto", this.alternativeWeekdays = n?.alternativeWeekdays ?? false, this.sloppyRanges = n?.sloppyRanges ?? false, this.second = Array(60).fill(0), this.minute = Array(60).fill(0), this.hour = Array(24).fill(0), this.day = Array(31).fill(0), this.month = Array(12).fill(0), this.dayOfWeek = Array(7).fill(0), this.year = Array(1e4).fill(0), this.lastDayOfMonth = false, this.lastWeekday = false, this.nearestWeekdays = Array(31).fill(0), this.starDOM = false, this.starDOW = false, this.starYear = false, this.useAndLogic = false, this.isFixedTime = false, this.parse();
  }
  parse() {
    if (!(typeof this.pattern == "string" || this.pattern instanceof String)) throw new TypeError("CronPattern: Pattern has to be of type string.");
    this.pattern.indexOf("@") >= 0 && (this.pattern = this.handleNicknames(this.pattern).trim());
    let e = this.pattern.match(/\S+/g) || [""], t = e.length;
    if (e.length < 5 || e.length > 7) throw new TypeError("CronPattern: invalid configuration format ('" + this.pattern + "'), exactly five, six, or seven space separated parts are required.");
    if (this.mode !== "auto") {
      let r;
      switch (this.mode) {
        case "5-part":
          r = 5;
          break;
        case "6-part":
          r = 6;
          break;
        case "7-part":
          r = 7;
          break;
        case "5-or-6-parts":
          r = [5, 6];
          break;
        case "6-or-7-parts":
          r = [6, 7];
          break;
        default:
          r = 0;
      }
      if (!(Array.isArray(r) ? r.includes(t) : t === r)) {
        let o = Array.isArray(r) ? r.join(" or ") : r.toString();
        throw new TypeError(`CronPattern: mode '${this.mode}' requires exactly ${o} parts, but pattern '${this.pattern}' has ${t} parts.`);
      }
    }
    if (e.length === 5 && e.unshift("0"), e.length === 6 && e.push("*"), this.pattern.indexOf("?") >= 0) for (let r = 0; r < e.length; r++) e[r] = e[r].replace(/\?/g, "*");
    if (e[3].toUpperCase() === "LW" ? (this.lastWeekday = true, e[3] = "") : e[3].toUpperCase().indexOf("L") >= 0 && (e[3] = e[3].replace(/L/gi, ""), this.lastDayOfMonth = true), e[3] == "*" && (this.starDOM = true), e[6] == "*" && (this.starYear = true), e[4].length >= 3 && (e[4] = this.replaceAlpha(e[4], i.MONTH_NAMES)), e[5].length >= 3 && (e[5] = this.alternativeWeekdays ? this.replaceAlpha(e[5], i.DAY_NAMES_QUARTZ) : this.replaceAlpha(e[5], i.DAY_NAMES)), e[5].startsWith("+") && (this.useAndLogic = true, e[5] = e[5].substring(1), e[5] === "")) throw new TypeError("CronPattern: Day-of-week field cannot be empty after '+' modifier.");
    switch (e[5] == "*" && (this.starDOW = true), this.mode) {
      case "5-part":
        e[0] = "0", e[6] = "*";
        break;
      case "6-part":
        e[6] = "*";
        break;
      case "5-or-6-parts":
        e[6] = "*";
        break;
      case "6-or-7-parts":
        break;
      case "7-part":
      case "auto":
        break;
    }
    this.throwAtIllegalCharacters(e), this.partToArray("second", e[0], 0, 1), this.partToArray("minute", e[1], 0, 1), this.partToArray("hour", e[2], 0, 1), this.partToArray("day", e[3], -1, 1), this.partToArray("month", e[4], -1, 1);
    let n = this.alternativeWeekdays ? -1 : 0;
    this.partToArray("dayOfWeek", e[5], n, 63), this.partToArray("year", e[6], 0, 1), !this.alternativeWeekdays && this.dayOfWeek[7] && (this.dayOfWeek[0] = this.dayOfWeek[7]);
    let s = (r, a) => !r.includes("*") && !r.includes("/") && a.filter((o) => o).length === 1;
    this.isFixedTime = s(e[0], this.second) && s(e[1], this.minute) && s(e[2], this.hour);
  }
  partToArray(e, t, n, s) {
    let r = this[e];
    if (t === "" && !(e === "day" && (this.lastDayOfMonth || this.lastWeekday))) throw new TypeError("CronPattern: configuration entry " + e + " (" + t + ") is empty, check for trailing spaces.");
    if (t === "*") return r.fill(s);
    let a = t.split(",");
    if (a.length > 1) for (let o of a) this.partToArray(e, o, n, s);
    else t.includes("-") && t.includes("/") ? this.handleRangeWithStepping(t, e, n, s) : t.includes("-") ? this.handleRange(t, e, n, s) : t.includes("/") ? this.handleStepping(t, e, n, s) : t !== "" && this.handleNumber(t, e, n, s);
  }
  throwAtIllegalCharacters(e) {
    for (let t = 0; t < e.length; t++) if ((t === 3 ? /[^/*0-9,\-WwLl]+/ : t === 5 ? /[^/*0-9,\-#Ll]+/ : /[^/*0-9,\-]+/).test(e[t])) throw new TypeError("CronPattern: configuration entry " + t + " (" + e[t] + ") contains illegal characters.");
  }
  handleNumber(e, t, n, s) {
    let r = this.extractNth(e, t), a = e.toUpperCase().includes("W");
    if (t !== "day" && a) throw new TypeError("CronPattern: Nearest weekday modifier (W) only allowed in day-of-month.");
    a && (t = "nearestWeekdays");
    let o = parseInt(r[0], 10) + n;
    if (isNaN(o)) throw new TypeError("CronPattern: " + t + " is not a number: '" + e + "'");
    this.setPart(t, o, r[1] || s);
  }
  setPart(e, t, n) {
    if (!Object.prototype.hasOwnProperty.call(this, e)) throw new TypeError("CronPattern: Invalid part specified: " + e);
    if (e === "dayOfWeek") {
      if (t === 7 && (t = 0), t < 0 || t > 6) throw new RangeError("CronPattern: Invalid value for dayOfWeek: " + t);
      this.setNthWeekdayOfMonth(t, n);
      return;
    }
    if (e === "second" || e === "minute") {
      if (t < 0 || t >= 60) throw new RangeError("CronPattern: Invalid value for " + e + ": " + t);
    } else if (e === "hour") {
      if (t < 0 || t >= 24) throw new RangeError("CronPattern: Invalid value for " + e + ": " + t);
    } else if (e === "day" || e === "nearestWeekdays") {
      if (t < 0 || t >= 31) throw new RangeError("CronPattern: Invalid value for " + e + ": " + t);
    } else if (e === "month") {
      if (t < 0 || t >= 12) throw new RangeError("CronPattern: Invalid value for " + e + ": " + t);
    } else if (e === "year" && (t < 1 || t >= 1e4)) throw new RangeError("CronPattern: Invalid value for " + e + ": " + t + " (supported range: 1-9999)");
    this[e][t] = n;
  }
  validateNotNaN(e, t) {
    if (isNaN(e)) throw new TypeError(t);
  }
  validateRange(e, t, n, s, r) {
    if (e > t) throw new TypeError("CronPattern: From value is larger than to value: '" + r + "'");
    if (n !== void 0) {
      if (n === 0) throw new TypeError("CronPattern: Syntax error, illegal stepping: 0");
      if (n > this[s].length) throw new TypeError("CronPattern: Syntax error, steps cannot be greater than maximum value of part (" + this[s].length + ")");
    }
  }
  handleRangeWithStepping(e, t, n, s) {
    if (e.toUpperCase().includes("W")) throw new TypeError("CronPattern: Syntax error, W is not allowed in ranges with stepping.");
    let r = this.extractNth(e, t), a = r[0].match(/^(\d+)-(\d+)\/(\d+)$/);
    if (a === null) throw new TypeError("CronPattern: Syntax error, illegal range with stepping: '" + e + "'");
    let [, o, u, h] = a, c = parseInt(o, 10) + n, d = parseInt(u, 10) + n, m = parseInt(h, 10);
    this.validateNotNaN(c, "CronPattern: Syntax error, illegal lower range (NaN)"), this.validateNotNaN(d, "CronPattern: Syntax error, illegal upper range (NaN)"), this.validateNotNaN(m, "CronPattern: Syntax error, illegal stepping: (NaN)"), this.validateRange(c, d, m, t, e);
    for (let p = c; p <= d; p += m) this.setPart(t, p, r[1] || s);
  }
  extractNth(e, t) {
    let n = e, s;
    if (n.includes("#")) {
      if (t !== "dayOfWeek") throw new Error("CronPattern: nth (#) only allowed in day-of-week field");
      s = n.split("#")[1], n = n.split("#")[0];
    } else if (n.toUpperCase().endsWith("L")) {
      if (t !== "dayOfWeek") throw new Error("CronPattern: L modifier only allowed in day-of-week field (use L alone for day-of-month)");
      s = "L", n = n.slice(0, -1);
    }
    return [n, s];
  }
  handleRange(e, t, n, s) {
    if (e.toUpperCase().includes("W")) throw new TypeError("CronPattern: Syntax error, W is not allowed in a range.");
    let r = this.extractNth(e, t), a = r[0].split("-");
    if (a.length !== 2) throw new TypeError("CronPattern: Syntax error, illegal range: '" + e + "'");
    let o = parseInt(a[0], 10) + n, u = parseInt(a[1], 10) + n;
    this.validateNotNaN(o, "CronPattern: Syntax error, illegal lower range (NaN)"), this.validateNotNaN(u, "CronPattern: Syntax error, illegal upper range (NaN)"), this.validateRange(o, u, void 0, t, e);
    for (let h = o; h <= u; h++) this.setPart(t, h, r[1] || s);
  }
  handleStepping(e, t, n, s) {
    if (e.toUpperCase().includes("W")) throw new TypeError("CronPattern: Syntax error, W is not allowed in parts with stepping.");
    let r = this.extractNth(e, t), a = r[0].split("/");
    if (a.length !== 2) throw new TypeError("CronPattern: Syntax error, illegal stepping: '" + e + "'");
    if (this.sloppyRanges) a[0] === "" && (a[0] = "*");
    else {
      if (a[0] === "") throw new TypeError("CronPattern: Syntax error, stepping with missing prefix ('" + e + "') is not allowed. Use wildcard (*/step) or range (min-max/step) instead.");
      if (a[0] !== "*") throw new TypeError("CronPattern: Syntax error, stepping with numeric prefix ('" + e + "') is not allowed. Use wildcard (*/step) or range (min-max/step) instead.");
    }
    let o = 0;
    a[0] !== "*" && (o = parseInt(a[0], 10) + n);
    let u = parseInt(a[1], 10);
    this.validateNotNaN(u, "CronPattern: Syntax error, illegal stepping: (NaN)"), this.validateRange(0, this[t].length - 1, u, t, e);
    for (let h = o; h < this[t].length; h += u) t === "year" && h === 0 || this.setPart(t, h, r[1] || s);
  }
  replaceAlpha(e, t) {
    let n = e;
    for (let [s, r] of Object.entries(t)) n = n.replace(new RegExp(s, "gi"), r);
    return n;
  }
  static DAY_NAMES = { "-sun": "-7", sun: "0", mon: "1", tue: "2", wed: "3", thu: "4", fri: "5", sat: "6" };
  static DAY_NAMES_QUARTZ = { sun: "1", mon: "2", tue: "3", wed: "4", thu: "5", fri: "6", sat: "7" };
  static MONTH_NAMES = { jan: "1", feb: "2", mar: "3", apr: "4", may: "5", jun: "6", jul: "7", aug: "8", sep: "9", oct: "10", nov: "11", dec: "12" };
  static NICKNAME_MAP = { "@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *", "@monthly": "0 0 1 * *", "@weekly": "0 0 * * 0", "@daily": "0 0 * * *", "@midnight": "0 0 * * *", "@hourly": "0 * * * *" };
  handleNicknames(e) {
    let t = e.trim().toLowerCase();
    if (t === "@reboot") throw new TypeError("CronPattern: @reboot is not supported in this environment. This is an event-based trigger that requires system startup detection.");
    return i.NICKNAME_MAP[t] || e;
  }
  setNthWeekdayOfMonth(e, t) {
    if (typeof t != "number" && t.toUpperCase() === "L") this.dayOfWeek[e] = this.dayOfWeek[e] | 32;
    else if (t === 63) this.dayOfWeek[e] = 63;
    else if (t < 6 && t > 0) this.dayOfWeek[e] = this.dayOfWeek[e] | R[t - 1];
    else throw new TypeError(`CronPattern: nth weekday out of range, should be 1-5 or L. Value: ${t}, Type: ${typeof t}`);
  }
};
var $ = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
var l = [["month", "year", 0], ["day", "month", -1], ["hour", "day", 0], ["minute", "hour", 0], ["second", "minute", 0]];
var f = class i2 {
  tz;
  instantMs;
  instantFields;
  ms;
  second;
  minute;
  hour;
  day;
  month;
  year;
  constructor(e, t) {
    if (this.tz = t, e && e instanceof Date) if (!isNaN(e)) this.fromDate(e);
    else throw new TypeError("CronDate: Invalid date passed to CronDate constructor");
    else if (e == null) this.fromDate(/* @__PURE__ */ new Date());
    else if (e && typeof e == "string") this.fromString(e);
    else if (e instanceof i2) this.fromCronDate(e);
    else throw new TypeError("CronDate: Invalid type (" + typeof e + ") passed to CronDate constructor");
  }
  getLastDayOfMonth(e, t) {
    return t !== 1 ? $[t] : new Date(Date.UTC(e, t + 1, 0)).getUTCDate();
  }
  getLastWeekday(e, t) {
    let n = this.getLastDayOfMonth(e, t), r = new Date(Date.UTC(e, t, n)).getUTCDay();
    return r === 0 ? n - 2 : r === 6 ? n - 1 : n;
  }
  getNearestWeekday(e, t, n) {
    let s = this.getLastDayOfMonth(e, t);
    if (n > s) return -1;
    let a = new Date(Date.UTC(e, t, n)).getUTCDay();
    return a === 0 ? n === s ? n - 2 : n + 1 : a === 6 ? n === 1 ? n + 2 : n - 1 : n;
  }
  isNthWeekdayOfMonth(e, t, n, s) {
    let a = new Date(Date.UTC(e, t, n)).getUTCDay(), o = 0;
    for (let u = 1; u <= n; u++) new Date(Date.UTC(e, t, u)).getUTCDay() === a && o++;
    if (s & 63 && R[o - 1] & s) return true;
    if (s & 32) {
      let u = this.getLastDayOfMonth(e, t);
      for (let h = n + 1; h <= u; h++) if (new Date(Date.UTC(e, t, h)).getUTCDay() === a) return false;
      return true;
    }
    return false;
  }
  fromDate(e) {
    if (this.tz !== void 0) if (typeof this.tz == "number") this.ms = e.getUTCMilliseconds(), this.second = e.getUTCSeconds(), this.minute = e.getUTCMinutes() + this.tz, this.hour = e.getUTCHours(), this.day = e.getUTCDate(), this.month = e.getUTCMonth(), this.year = e.getUTCFullYear(), this.apply();
    else try {
      let t = g(e, this.tz);
      this.ms = e.getMilliseconds(), this.second = t.s, this.minute = t.i, this.hour = t.h, this.day = t.d, this.month = t.m - 1, this.year = t.y, this.cacheInstant(e.getTime() - this.ms);
    } catch (t) {
      let n = t instanceof Error ? t.message : String(t);
      throw new TypeError(`CronDate: Failed to convert date to timezone '${this.tz}'. This may happen with invalid timezone names or dates. Original error: ${n}`);
    }
    else this.ms = e.getMilliseconds(), this.second = e.getSeconds(), this.minute = e.getMinutes(), this.hour = e.getHours(), this.day = e.getDate(), this.month = e.getMonth(), this.year = e.getFullYear(), this.cacheInstant(e.getTime() - this.ms);
  }
  cacheInstant(e) {
    this.instantMs = e;
    let { year: t, month: n, day: s, hour: r, minute: a, second: o } = this;
    this.instantFields = [t, n, s, r, a, o, this.tz];
  }
  cachedInstant() {
    let e = this.instantFields;
    if (!(e === void 0 || e[6] !== this.tz) && !(e[5] !== this.second || e[4] !== this.minute || e[3] !== this.hour) && !(e[2] !== this.day || e[1] !== this.month || e[0] !== this.year)) return this.instantMs;
  }
  fromCronDate(e) {
    this.tz = e.tz, this.year = e.year, this.month = e.month, this.day = e.day, this.hour = e.hour, this.minute = e.minute, this.second = e.second, this.ms = e.ms, this.instantMs = e.instantMs, this.instantFields = e.instantFields;
  }
  apply() {
    if (this.month > 11 || this.month < 0 || this.day > this.getLastDayOfMonth(this.year, this.month) || this.day < 1 || this.hour > 23 || this.minute > 59 || this.second > 59 || this.hour < 0 || this.minute < 0 || this.second < 0) {
      let e = /* @__PURE__ */ new Date(0);
      return e.setUTCFullYear(this.year, this.month, this.day), e.setUTCHours(this.hour, this.minute, this.second, this.ms), this.ms = e.getUTCMilliseconds(), this.second = e.getUTCSeconds(), this.minute = e.getUTCMinutes(), this.hour = e.getUTCHours(), this.day = e.getUTCDate(), this.month = e.getUTCMonth(), this.year = e.getUTCFullYear(), true;
    } else return false;
  }
  fromString(e) {
    if (typeof this.tz == "number") {
      if (O(e)) return this.fromDate(D(e));
      let t = D(e, "Etc/UTC");
      this.ms = t.getUTCMilliseconds(), this.second = t.getUTCSeconds(), this.minute = t.getUTCMinutes(), this.hour = t.getUTCHours(), this.day = t.getUTCDate(), this.month = t.getUTCMonth(), this.year = t.getUTCFullYear(), this.apply();
    } else return this.fromDate(D(e, this.tz));
  }
  findNext(e, t, n, s) {
    return this._findMatch(e, t, n, s, 1);
  }
  _matchDayValue(e, t, n, s) {
    let r = e;
    if (!r) {
      for (let a = 0; a < n.nearestWeekdays.length; a++) if (n.nearestWeekdays[a]) {
        let o = this.getNearestWeekday(this.year, this.month, a + 1);
        if (o !== -1 && o === t) {
          r = 1;
          break;
        }
      }
    }
    if (n.lastWeekday) {
      let a = this.getLastWeekday(this.year, this.month);
      t === a && (r = 1);
    }
    if (n.lastDayOfMonth) {
      let a = this.getLastDayOfMonth(this.year, this.month);
      t === a && (r = 1);
    }
    if (!n.starDOW) {
      let a = new Date(Date.UTC(this.year, this.month, 1, 0, 0, 0, 0)).getUTCDay(), o = n.dayOfWeek[(a + (t - 1)) % 7];
      o && o & 63 && (o = this.isNthWeekdayOfMonth(this.year, this.month, t, o) ? 1 : 0), n.useAndLogic ? r = r && o : !s.domAndDow && !n.starDOM ? r = r || o : r = r && o;
    }
    return r;
  }
  _findMatch(e, t, n, s, r) {
    let a = this[t], o = this[t] + s, u = r === 1 ? (h) => h < n[t].length : (h) => h >= 0;
    for (let h = o; u(h); h += r) {
      let c = n[t][h];
      if (t === "day" && (c = this._matchDayValue(c, h - s, n, e)), c) return this[t] = h - s, a !== this[t] ? 2 : 1;
    }
    return 3;
  }
  recurse(e, t, n) {
    let s = this.year, r = n;
    for (; typeof r == "number"; ) {
      if (e.starYear && Math.abs(this.year - s) > 400) return null;
      r = this.recurseStep(e, t, r);
    }
    return r;
  }
  recurseStep(e, t, n) {
    if (n === 0 && !e.starYear) {
      if (this.year >= 0 && this.year < e.year.length && e.year[this.year] === 0) {
        let r = -1;
        for (let a = this.year + 1; a < e.year.length && a < 1e4; a++) if (e.year[a] === 1) {
          r = a;
          break;
        }
        if (r === -1) return null;
        this.year = r, this.month = 0, this.day = 1, this.hour = 0, this.minute = 0, this.second = 0, this.ms = 0;
      }
      if (this.year >= 1e4) return null;
    }
    let s = this.findNext(t, l[n][0], e, l[n][2]);
    if (s > 1) {
      let r = n + 1;
      for (; r < l.length; ) this[l[r][0]] = -l[r][2], r++;
      if (s === 3) {
        if (this[l[n][1]]++, this[l[n][0]] = -l[n][2], this.apply(), n === 0 && !e.starYear) {
          for (; this.year >= 0 && this.year < e.year.length && e.year[this.year] === 0 && this.year < 1e4; ) this.year++;
          if (this.year >= 1e4 || this.year >= e.year.length) return null;
        }
        return 0;
      } else if (this.apply()) return n - 1;
    }
    return n += 1, n >= l.length ? this : (e.starYear ? this.year >= 3e3 : this.year >= 1e4) ? null : n;
  }
  increment(e, t, n) {
    return this.instantMs = void 0, this.instantFields = void 0, this.second += t.interval !== void 0 && t.interval > 1 && n ? t.interval : 1, this.ms = 0, this.apply(), this.recurse(e, t, 0);
  }
  decrement(e, t) {
    return this.instantMs = void 0, this.instantFields = void 0, this.second -= t.interval !== void 0 && t.interval > 1 ? t.interval : 1, this.ms = 0, this.apply(), this.recurseBackward(e, t, 0);
  }
  recurseBackward(e, t, n) {
    let s = this.year, r = n;
    for (; typeof r == "number"; ) {
      if (e.starYear && Math.abs(this.year - s) > 400) return null;
      r = this.recurseBackwardStep(e, t, r);
    }
    return r;
  }
  recurseBackwardStep(e, t, n) {
    if (n === 0 && !e.starYear) {
      if (this.year >= 0 && this.year < e.year.length && e.year[this.year] === 0) {
        let r = -1;
        for (let a = this.year - 1; a >= 0; a--) if (e.year[a] === 1) {
          r = a;
          break;
        }
        if (r === -1) return null;
        this.year = r, this.month = 11, this.day = 31, this.hour = 23, this.minute = 59, this.second = 59, this.ms = 0;
      }
      if (this.year < 0) return null;
    }
    let s = this.findPrevious(t, l[n][0], e, l[n][2]);
    if (s > 1) {
      let r = n + 1;
      for (; r < l.length; ) {
        let a = l[r][0], o = l[r][2], u = this.getMaxPatternValue(a, e, o);
        this[a] = u, r++;
      }
      if (s === 3) {
        if (this[l[n][1]]--, n === 0) {
          let u = this.getLastDayOfMonth(this.year, this.month);
          this.day > u && (this.day = u);
        }
        if (n === 1) if (this.day <= 0) this.day = 1;
        else {
          let u = this.year, h = this.month;
          for (; h < 0; ) h += 12, u--;
          for (; h > 11; ) h -= 12, u++;
          let c = this.getLastDayOfMonth(u, h);
          this.day > c && (this.day = c);
        }
        this.apply();
        let a = l[n][0], o = l[n][2];
        if (this[a] = this.getMaxPatternValue(a, e, o), this.apply(), n === 0) {
          let u = l[1][2], h = this.getMaxPatternValue("day", e, u);
          h !== this.day && (this.day = h, this.hour = this.getMaxPatternValue("hour", e, l[2][2]), this.minute = this.getMaxPatternValue("minute", e, l[3][2]), this.second = this.getMaxPatternValue("second", e, l[4][2]));
        }
        if (n === 0 && !e.starYear) {
          for (; this.year >= 0 && this.year < e.year.length && e.year[this.year] === 0; ) this.year--;
          if (this.year < 0) return null;
        }
        return 0;
      } else if (this.apply()) return n - 1;
    }
    return n += 1, n >= l.length ? this : this.year < 0 ? null : n;
  }
  getMaxPatternValue(e, t, n) {
    let s = t[e].length - 1 - n;
    for (let r = t[e].length - 1; r >= 0; r--) if (t[e][r]) {
      s = r - n;
      break;
    }
    if (e === "day") {
      let r = this.getLastDayOfMonth(this.year, this.month);
      return t.lastDayOfMonth || !t.starDOW ? r : Math.min(s, r);
    }
    return s;
  }
  findPrevious(e, t, n, s) {
    return this._findMatch(e, t, n, s, -1);
  }
  getDate(e) {
    if (!e && typeof this.tz != "number") {
      let t = this.cachedInstant();
      if (t !== void 0) return new Date(t + (this.tz === void 0 ? this.ms : 0));
    }
    return e || this.tz === void 0 ? new Date(this.year, this.month, this.day, this.hour, this.minute, this.second, this.ms) : typeof this.tz == "number" ? new Date(Date.UTC(this.year, this.month, this.day, this.hour, this.minute - this.tz, this.second, this.ms)) : P(y(this.year, this.month + 1, this.day, this.hour, this.minute, this.second, this.tz), false);
  }
  getTime() {
    return this.getDate(false).getTime();
  }
  match(e, t) {
    if (!e.starYear && (this.year < 0 || this.year >= e.year.length || e.year[this.year] === 0)) return false;
    for (let n = 0; n < l.length; n++) {
      let s = l[n][0], r = l[n][2], a = this[s];
      if (a + r < 0 || a + r >= e[s].length) return false;
      let o = e[s][a + r];
      if (s === "day" && (o = this._matchDayValue(o, a, e, t)), !o) return false;
    }
    return true;
  }
};
function I(i3) {
  i3 === void 0 && (i3 = {}), delete i3.name, i3.legacyMode !== void 0 && i3.domAndDow === void 0 ? i3.domAndDow = !i3.legacyMode : i3.domAndDow === void 0 && (i3.domAndDow = false), i3.legacyMode = !i3.domAndDow;
  let e = (n, s) => n === void 0 ? s : n;
  if (i3.paused = e(i3.paused, false), i3.maxRuns = e(i3.maxRuns, 1 / 0), i3.catch = e(i3.catch, false), i3.unref = e(i3.unref, false), i3.mode = e(i3.mode, "auto"), i3.alternativeWeekdays = e(i3.alternativeWeekdays, false), i3.sloppyRanges = e(i3.sloppyRanges, false), i3.allowPast = e(i3.allowPast, false), i3.interval = i3.interval === void 0 ? 0 : parseInt(i3.interval.toString(), 10), i3.utcOffset = i3.utcOffset === void 0 ? void 0 : parseInt(i3.utcOffset.toString(), 10), i3.dayOffset = i3.dayOffset === void 0 ? 0 : parseInt(i3.dayOffset.toString(), 10), !["auto", "5-part", "6-part", "7-part", "5-or-6-parts", "6-or-7-parts"].includes(i3.mode)) throw new Error("CronOptions: mode must be one of 'auto', '5-part', '6-part', '7-part', '5-or-6-parts', or '6-or-7-parts'.");
  i3.timezone === "" && (i3.timezone = void 0);
  let t = i3.timezone ?? i3.utcOffset;
  if (i3.startAt && (i3.startAt = W(new f(i3.startAt, t))), i3.stopAt && (i3.stopAt = W(new f(i3.stopAt, t))), i3.interval !== null) {
    if (isNaN(i3.interval)) throw new Error("CronOptions: Supplied value for interval is not a number");
    if (i3.interval < 0) throw new Error("CronOptions: Supplied value for interval can not be negative");
  }
  if (i3.utcOffset !== void 0) {
    if (isNaN(i3.utcOffset)) throw new Error("CronOptions: Invalid value passed for utcOffset, should be number representing minutes offset from UTC.");
    if (i3.utcOffset < -870 || i3.utcOffset > 870) throw new Error("CronOptions: utcOffset out of bounds.");
    if (i3.utcOffset !== void 0 && i3.timezone) throw new Error("CronOptions: Combining 'utcOffset' with 'timezone' is not allowed.");
  }
  if (i3.unref !== true && i3.unref !== false) throw new Error("CronOptions: Unref should be either true, false or undefined(false).");
  if (i3.dayOffset !== void 0 && i3.dayOffset !== 0 && isNaN(i3.dayOffset)) throw new Error("CronOptions: Invalid value passed for dayOffset, should be a number representing days to offset.");
  return i3;
}
function W(i3) {
  return i3.ms ? (i3.ms = 0, new f(new Date(i3.getTime() + 1e3), i3.tz)) : i3;
}
function T(i3) {
  return typeof i3 == "function";
}
function z(i3) {
  return T(i3);
}
function F(i3) {
  typeof Deno < "u" && typeof Deno.unrefTimer < "u" ? Deno.unrefTimer(i3) : i3 && typeof i3.unref < "u" && i3.unref();
}
var N = 1440 * 60 * 1e3;
var L = 30 * 1e3;
var k = [];
var Y = class {
  name;
  options;
  _states;
  fn;
  _startAtRoundingMs = 0;
  getTz() {
    return this.options.timezone ?? this.options.utcOffset;
  }
  applyDayOffset(e) {
    let t = this._offsetMs();
    return t ? new Date(e.getTime() + t) : e;
  }
  _offsetMs() {
    return (this.options.dayOffset ?? 0) * N;
  }
  get _startAt() {
    return this.options.startAt;
  }
  get _stopAt() {
    return this.options.stopAt;
  }
  _nextTarget(e, t) {
    let n = this._offsetMs(), s = this._next(this._shift(e), new Date(t.getTime() - n), { startAt: this._shift(this._startAt), stopAt: this._shift(this._stopAt) });
    return s ? this.applyDayOffset(s.getDate(false)) : null;
  }
  _previousTarget(e, t) {
    let n = this._previous(this._shift(e ?? t), { startAt: this._shift(this._startAt), stopAt: this._shift(this._stopAt) });
    return n ? this.applyDayOffset(n.getDate(false)) : null;
  }
  _shift(e) {
    if (e != null) return new f(new Date(this._toMs(e) - this._offsetMs()), this.getTz());
  }
  constructor(e, t, n) {
    let s, r;
    if (T(t)) r = t;
    else if (typeof t == "object") s = t;
    else if (t !== void 0) throw new Error("Cron: Invalid argument passed for optionsIn. Should be one of function, or object (options).");
    if (T(n)) r = n;
    else if (typeof n == "object") s = n;
    else if (n !== void 0) throw new Error("Cron: Invalid argument passed for funcIn. Should be one of function, or object (options).");
    this.name = s?.name;
    let a = s?.startAt;
    if (this.options = I(s), a && this._startAt && (this._startAtRoundingMs = this._startAt.getTime() - new f(a, this.getTz()).getTime()), this._states = { kill: false, blocking: false, previousRun: void 0, currentRun: void 0, once: void 0, currentTimeout: void 0, maxRuns: s ? s.maxRuns : void 0, paused: s ? s.paused : false, pattern: new b("* * * * *", void 0, { mode: "auto" }) }, e && (e instanceof Date || typeof e == "string" && e.indexOf(":") > 0) ? this._states.once = new f(e, this.getTz()) : this._states.pattern = new b(e, this.options.timezone, { mode: this.options.mode, alternativeWeekdays: this.options.alternativeWeekdays, sloppyRanges: this.options.sloppyRanges }), this.name) {
      if (k.find((u) => u.name === this.name)) throw new Error("Cron: Tried to initialize new named job '" + this.name + "', but name already taken.");
      k.push(this);
    }
    return r !== void 0 && z(r) && (this.fn = r, this.schedule()), this;
  }
  nextRun(e) {
    return this._nextTarget(e, /* @__PURE__ */ new Date());
  }
  nextRuns(e, t) {
    this._states.maxRuns !== void 0 && e > this._states.maxRuns && (e = this._states.maxRuns);
    let n = t || this._states.currentRun || void 0;
    return this._enumerateRuns(e, n, "next");
  }
  previousRuns(e, t) {
    return this._enumerateRuns(e, t || void 0, "previous");
  }
  enumerate(e) {
    return new A(this, e);
  }
  _enumerateRuns(e, t, n) {
    let s = [], r = /* @__PURE__ */ new Date(), a = t;
    for (; e-- > 0; ) {
      let o = n === "next" ? this._nextTarget(a, r) : this._previousTarget(a, r);
      if (!o) break;
      s.push(o), a = o;
    }
    return s;
  }
  match(e) {
    if (this._states.once) {
      let n = new f(e, this.getTz());
      n.ms = 0;
      let s = new f(this._states.once, this.getTz());
      return s.ms = 0, n.getTime() === s.getTime();
    }
    let t = new f(e, this.getTz());
    return t.ms = 0, t.match(this._states.pattern, this.options);
  }
  getPattern() {
    if (!this._states.once) return this._states.pattern ? this._states.pattern.pattern : void 0;
  }
  getOnce() {
    return this._states.once ? this._states.once.getDate() : null;
  }
  isRunning() {
    return !this._states.paused && !this._states.kill && this.fn !== void 0 && this._nextTarget(this._states.currentRun, /* @__PURE__ */ new Date()) !== null;
  }
  isStopped() {
    return this._states.kill;
  }
  isBusy() {
    return this._states.blocking;
  }
  currentRun() {
    return this._states.currentRun ? this._states.currentRun.getDate() : null;
  }
  previousRun() {
    return this._states.previousRun ? this._states.previousRun.getDate() : null;
  }
  msToNext(e) {
    let t = /* @__PURE__ */ new Date(), n = this._nextTarget(e, t);
    return n ? n.getTime() - this._toMs(e ?? t) : null;
  }
  stop() {
    this._states.kill = true, this._states.currentTimeout && clearTimeout(this._states.currentTimeout);
    let e = k.indexOf(this);
    e >= 0 && k.splice(e, 1);
  }
  pause() {
    return this._states.paused = true, !this._states.kill;
  }
  resume() {
    return this._states.paused = false, !this._states.kill;
  }
  schedule(e) {
    return this._schedule(e);
  }
  _schedule(e, t) {
    if (e && this.fn) throw new Error("Cron: It is not allowed to schedule two functions using the same Croner instance.");
    e && (this.fn = e);
    let n = t ?? /* @__PURE__ */ new Date(), s = this._nextTarget(void 0, n), r = s ? s.getTime() - n.getTime() : null, a = this._states.currentRun;
    a && a.getTime() > n.getTime() && (a = void 0);
    let o = this._nextTarget(a, n);
    return r === null || isNaN(r) || o === null ? this : (r > L && (r = L), r < 0 && (r = this._states.paused ? 1e3 : 0), this._states.currentTimeout = setTimeout(() => this._checkTrigger(o), r), this._states.currentTimeout && this.options.unref && F(this._states.currentTimeout), this);
  }
  async _trigger(e = /* @__PURE__ */ new Date()) {
    this._states.blocking = true, this._states.currentRun = new f(e, this.getTz());
    try {
      if (this.options.catch) try {
        this.fn !== void 0 && await this.fn(this, this.options.context);
      } catch (t) {
        if (T(this.options.catch)) try {
          this.options.catch(t, this);
        } catch {
        }
      }
      else this.fn !== void 0 && await this.fn(this, this.options.context);
    } finally {
      this._states.previousRun = new f(e, this.getTz()), this._states.blocking = false;
    }
  }
  async trigger() {
    await this._trigger();
  }
  runsLeft() {
    return this._states.maxRuns;
  }
  _checkTrigger(e) {
    let t = /* @__PURE__ */ new Date(), n = !this._states.paused && t.getTime() >= e.getTime(), s = this._states.blocking && this.options.protect;
    n && !s ? (this._states.maxRuns !== void 0 && this._states.maxRuns--, this._trigger(t)) : n && s && T(this.options.protect) && setTimeout(() => this.options.protect(this), 0), this._schedule(void 0, n ? void 0 : t);
  }
  _next(e, t, n) {
    let s = n ? n.startAt : this._startAt, r = n ? n.stopAt : this._stopAt, a = !!e, o = !!(e || this._states.currentRun), u = false;
    !e && s && this.options.interval && (e = this._calculatePreviousRun(s, t), e && (o = true), u = !e);
    let h = e ?? t ?? /* @__PURE__ */ new Date(), c = this._toMs(h);
    e = new f(h, this.getTz()), s && c < s.getTime() && (e = u ? s : new f(new Date(s.getTime() - 1e3), this.getTz()), c = e.getTime());
    let d = this._states.once || new f(e, this.getTz());
    if (!u && d !== this._states.once && (d = this._findOccurrence(e, c, 1, o)), this._states.once) {
      let m = this._states.once.getTime(), p = c - m;
      if (s && m < s.getTime() - this._startAtRoundingMs || this._states.currentRun && this.applyDayOffset(this._states.once.getDate()).getTime() <= this._states.currentRun.getTime() || (a ? p >= 0 : p > 1e3 && !this.options.allowPast)) return null;
    }
    return d === null || this._states.maxRuns !== void 0 && this._states.maxRuns <= 0 || this._states.kill || r && d.getTime() >= r.getTime() ? null : d;
  }
  _previous(e, t) {
    let n = this._toMs(e ?? /* @__PURE__ */ new Date());
    if (t.stopAt && n > t.stopAt.getTime() && (n = t.stopAt.getTime()), this._states.once) return this._states.once.getTime() < n ? this._states.once : null;
    let s = this._findOccurrence(new f(new Date(n), this.getTz()), n, -1);
    return s === null || t.startAt && s.getTime() < t.startAt.getTime() ? null : s;
  }
  _toMs(e) {
    return e instanceof Date ? e.getTime() : new f(e, this.getTz()).getTime();
  }
  _findOccurrence(e, t, n, s = false) {
    let r = this._walk(e, t, n, s), a = this.getTz();
    if (this._states.pattern.isFixedTime && n === 1 || typeof a == "number" || U(t, a) !== (n === 1 ? "earlier" : "later")) return r;
    let o = Math.floor(t / 1e3) * 1e3, u = n === 1 ? _(o, o + N, a) : _(o - N, o, a) - 1e3;
    if (r && (u - r.getTime()) * n > 0) return r;
    let h = this._walk(new f(new Date(u), a), u - n, n, false, true);
    return !r || !h ? r ?? h : (r.getTime() - h.getTime()) * n > 0 ? h : r;
  }
  _walk(e, t, n, s, r = false) {
    let a = this._states.pattern, o = (h, c) => n === 1 ? h.increment(a, this.options, c) : h.decrement(a, this.options), u = new f(e, this.getTz());
    for ((!r || !u.match(a, this.options)) && (u = o(u, s)); u; ) {
      let h = this._runAt(u, t, n);
      if (h) return new f(h, this.getTz());
      u = o(u, false);
    }
    return null;
  }
  _runAt(e, t, n) {
    let s = (m) => (m.getTime() - t) * n > 0, r = e.tz;
    if (typeof r == "number") {
      let m = e.getDate();
      return s(m) ? m : null;
    }
    let a = y(e.year, e.month + 1, e.day, e.hour, e.minute, e.second, r), o = v(a), u = this._states.pattern.isFixedTime;
    if (o.kind === "single") return s(o.date) ? o.date : null;
    if (o.kind === "ambiguous") {
      let { earlier: m, later: p } = o;
      return (u ? [m] : n === 1 ? [m, p] : [p, m]).find(s) ?? null;
    }
    if (!u) return null;
    let h = new Date(_(o.before.getTime(), o.after.getTime(), r)), c = g(h, r);
    return c.y === a.y && c.m === a.m && c.d === a.d && s(h) ? h : null;
  }
  _calculatePreviousRun(e, t) {
    let n = new f(t, this.getTz()).getTime();
    if (e.getTime() > n) return;
    let s = this.options.interval * 1e3, r = e;
    for (; r.getTime() + s <= n; ) {
      let a = new f(r, this.getTz()).increment(this._states.pattern, this.options, true);
      if (a === null) break;
      r = a;
    }
    return r;
  }
};
var A = class {
  cron;
  cursor;
  now;
  done;
  initialMaxRuns;
  remainingRuns;
  constructor(e, t) {
    this.cron = e, this.initialMaxRuns = e.runsLeft() ?? 1 / 0, this.reset(t);
  }
  next() {
    if (this.done || this.remainingRuns <= 0) return this.done = true, { value: void 0, done: true };
    let e = this.cron._nextTarget(this.cursor, this.now);
    return e === null ? (this.done = true, { value: void 0, done: true }) : (this.cursor = e, this.remainingRuns--, { value: e, done: false });
  }
  peek() {
    return this.done || this.remainingRuns <= 0 ? null : this.cron._nextTarget(this.cursor, this.now);
  }
  reset(e) {
    this.cursor = e instanceof Date ? new Date(e.getTime()) : e ?? void 0, this.now = /* @__PURE__ */ new Date(), this.done = false, this.remainingRuns = this.initialMaxRuns;
  }
  [Symbol.iterator]() {
    return this;
  }
};
export {
  Y as Cron
};
