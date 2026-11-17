// node_modules/croner/dist/croner.js
function g(s3, e, t, n = 0, r = 0, a = 0, i = 0) {
  let o = s3 >= 0 && s3 < 100, u = Date.UTC(o ? s3 + 400 : s3, e, t, n, r, a, i);
  return o ? u - 126227808e5 : u;
}
function D(s3) {
  return g(s3.y, s3.m - 1, s3.d, s3.h, s3.i, s3.s);
}
function w(s3, e) {
  return s3.y === e.y && s3.m === e.m && s3.d === e.d && s3.h === e.h && s3.i === e.i && s3.s === e.s;
}
function O(s3) {
  let e = s3.substring(9);
  return e.includes("Z") || e.includes("+") || e.includes("-");
}
function Z(s3, e) {
  let t = new Date(Date.parse(s3));
  if (isNaN(t)) throw new Error("Invalid ISO8601 passed to timezone parser.");
  return O(s3) ? { tp: T(t.getUTCFullYear(), t.getUTCMonth() + 1, t.getUTCDate(), t.getUTCHours(), t.getUTCMinutes(), t.getUTCSeconds(), "Etc/UTC"), ms: t.getUTCMilliseconds() } : { tp: T(t.getFullYear(), t.getMonth() + 1, t.getDate(), t.getHours(), t.getMinutes(), t.getSeconds(), e), ms: t.getMilliseconds() };
}
function v(s3, e, t) {
  let { tp: n, ms: r } = Z(s3, e);
  return new Date(N(n, t).getTime() + r);
}
function N(s3, e) {
  let t = _(s3);
  if (t.kind === "single") return t.date;
  if (t.kind === "ambiguous") return t.earlier;
  if (e) throw new Error("Invalid date passed to fromTZ()");
  return t.after;
}
function _(s3) {
  let e = D(s3), t = (i) => R(new Date(i), s3.tz), n = e - t(e), r = y(new Date(n), s3.tz);
  if (w(r, s3)) {
    let i = D(r) - n, o = n - (t(n - E) - i);
    if (o < n && w(y(new Date(o), s3.tz), s3)) return { kind: "ambiguous", earlier: new Date(o), later: new Date(n) };
    let u = n + (i - t(n + E));
    return u > n && w(y(new Date(u), s3.tz), s3) ? { kind: "ambiguous", earlier: new Date(n), later: new Date(u) } : { kind: "single", date: new Date(n) };
  }
  let a = e - (D(r) - n);
  return w(y(new Date(a), s3.tz), s3) ? { kind: "single", date: new Date(a) } : { kind: "gap", before: new Date(Math.min(n, a)), after: new Date(Math.max(n, a)) };
}
var E = 3 * 36e5;
function R(s3, e) {
  return D(y(s3, e)) - (s3.getTime() - s3.getUTCMilliseconds());
}
function k(s3, e, t) {
  let n = R(new Date(s3), t);
  for (; e - s3 > 1e3; ) {
    let r = s3 + Math.floor((e - s3) / 2e3) * 1e3;
    R(new Date(r), t) === n ? s3 = r : e = r;
  }
  return e;
}
function U(s3, e) {
  let t = _(y(new Date(s3), e));
  if (t.kind === "ambiguous") return s3 < t.later.getTime() ? "earlier" : "later";
}
var H = g(1, 0, 1);
var S = 864e5;
var W = /* @__PURE__ */ new Map();
function y(s3, e) {
  if (e === void 0) return T(s3.getFullYear(), s3.getMonth() + 1, s3.getDate(), s3.getHours(), s3.getMinutes(), s3.getSeconds());
  let t = W.get(e), n;
  try {
    t || (t = new Intl.DateTimeFormat("en-US", { timeZone: e, year: "numeric", month: "numeric", day: "numeric", hour: "numeric", minute: "numeric", second: "numeric", hour12: false }), W.set(e, t)), n = t.formatToParts(s3);
  } catch (i) {
    let o = i instanceof Error ? i.message : String(i);
    throw new RangeError(`toTZ: Invalid timezone '${e}' or date. Please provide a valid IANA timezone (e.g., 'America/New_York', 'Europe/Stockholm'). Original error: ${o}`);
  }
  let r = { year: 0, month: 0, day: 0, hour: 0, minute: 0, second: 0 };
  for (let i of n) (i.type === "year" || i.type === "month" || i.type === "day" || i.type === "hour" || i.type === "minute" || i.type === "second") && (r[i.type] = parseInt(i.value, 10));
  if (isNaN(r.year) || isNaN(r.month) || isNaN(r.day) || isNaN(r.hour) || isNaN(r.minute) || isNaN(r.second)) throw new Error(`toTZ: Failed to parse all date components from timezone '${e}'. This may indicate an invalid date or timezone configuration. Parsed components: ${JSON.stringify(r)}`);
  let a = s3.getTime() - H;
  return (a < -S || a < S && r.month === 12) && (r.year = 1 - r.year), r.hour === 24 && (r.hour = 0), { y: r.year, m: r.month, d: r.day, h: r.hour, i: r.minute, s: r.second, tz: e };
}
function T(s3, e, t, n, r, a, i) {
  return { y: s3, m: e, d: t, h: n, i: r, s: a, tz: i };
}
var x = [1, 2, 4, 8, 16];
var C = class s {
  pattern;
  timezone;
  mode;
  alternativeWeekdays;
  sloppyRanges;
  fixedTimeRule;
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
    this.pattern = e, this.timezone = t, this.mode = n?.mode ?? "auto", this.alternativeWeekdays = n?.alternativeWeekdays ?? false, this.sloppyRanges = n?.sloppyRanges ?? false, this.fixedTimeRule = n?.fixedTimeRule ?? "single-value", this.second = Array(60).fill(0), this.minute = Array(60).fill(0), this.hour = Array(24).fill(0), this.day = Array(31).fill(0), this.month = Array(12).fill(0), this.dayOfWeek = Array(7).fill(0), this.year = [], this.lastDayOfMonth = false, this.lastWeekday = false, this.nearestWeekdays = Array(31).fill(0), this.starDOM = false, this.starDOW = false, this.starYear = false, this.useAndLogic = false, this.isFixedTime = false, this.parse();
  }
  parse() {
    if (!(typeof this.pattern == "string" || this.pattern instanceof String)) throw new TypeError("CronPattern: Pattern has to be of type string.");
    this.pattern.indexOf("@") >= 0 && (this.pattern = this.handleNicknames(this.pattern).trim());
    let e = this.pattern.match(/\S+/g) || [""], t = e.length;
    if (e.length < 5 || e.length > 7) throw new TypeError("CronPattern: invalid configuration format ('" + this.pattern + "'), exactly five, six, or seven space separated parts are required.");
    if (this.mode !== "auto") {
      let i;
      switch (this.mode) {
        case "5-part":
          i = 5;
          break;
        case "6-part":
          i = 6;
          break;
        case "7-part":
          i = 7;
          break;
        case "5-or-6-parts":
          i = [5, 6];
          break;
        case "6-or-7-parts":
          i = [6, 7];
          break;
        default:
          i = 0;
      }
      if (!(Array.isArray(i) ? i.includes(t) : t === i)) {
        let u = Array.isArray(i) ? i.join(" or ") : i.toString();
        throw new TypeError(`CronPattern: mode '${this.mode}' requires exactly ${u} parts, but pattern '${this.pattern}' has ${t} parts.`);
      }
    }
    if (e.length === 5 && e.unshift("0"), e.length === 6 && e.push("*"), this.pattern.indexOf("?") >= 0) for (let i = 0; i < e.length; i++) e[i] = e[i].replace(/\?/g, "*");
    if (e[3].toUpperCase() === "LW") this.lastWeekday = true, e[3] = "";
    else if (e[3].toUpperCase().indexOf("L") >= 0) {
      let i = e[3].split(",");
      if (i.length > 1 && i.some((o) => o.toUpperCase() === "LW")) throw new TypeError(`CronPattern: Syntax error, LW can not be in a list in day-of-month ('${e[3]}').`);
      if (i.some((o) => o === "" || o.toUpperCase() !== "L" && /l/i.test(o))) throw new TypeError("CronPattern: Syntax error, L has to be a value of its own in day-of-month ('" + e[3] + "').");
      e[3] = i.filter((o) => o.toUpperCase() !== "L").join(","), this.lastDayOfMonth = true;
    }
    if (e[3] == "*" && (this.starDOM = true), e[6] == "*" && (this.starYear = true), e[4].length >= 3 && (e[4] = this.replaceAlpha(e[4], s.MONTH_NAMES)), e[5].length >= 3 && (e[5] = this.alternativeWeekdays ? this.replaceAlpha(e[5], s.DAY_NAMES_QUARTZ) : this.replaceAlpha(e[5], s.DAY_NAMES)), e[5].startsWith("+") && (this.useAndLogic = true, e[5] = e[5].substring(1), e[5] === "")) throw new TypeError("CronPattern: Day-of-week field cannot be empty after '+' modifier.");
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
    this.partToArray("dayOfWeek", e[5], n, 63), e[6] === "*" ? this.year = Array(1e4).fill(1) : (this.year = Array(1e4).fill(0), this.partToArray("year", e[6], 0, 1)), !this.alternativeWeekdays && this.dayOfWeek[7] && (this.dayOfWeek[0] = this.dayOfWeek[7]);
    let r = this.fixedTimeRule === "single-value", a = (i, o) => !i.includes("*") && !i.includes("/") && (!r || o.filter((u) => u).length === 1);
    this.isFixedTime = a(e[0], this.second) && a(e[1], this.minute) && a(e[2], this.hour);
  }
  partToArray(e, t, n, r) {
    let a = this[e];
    if (t === "" && !(e === "day" && (this.lastDayOfMonth || this.lastWeekday))) throw new TypeError("CronPattern: configuration entry " + e + " (" + t + ") is empty, check for trailing spaces.");
    if (t === "*") return a.fill(r);
    let i = t.split(",");
    if (i.length > 1) for (let o of i) this.partToArray(e, o, n, r);
    else t.includes("-") && t.includes("/") ? this.handleRangeWithStepping(t, e, n, r) : t.includes("-") ? this.handleRange(t, e, n, r) : t.includes("/") ? this.handleStepping(t, e, n, r) : t !== "" && this.handleNumber(t, e, n, r);
  }
  throwAtIllegalCharacters(e) {
    for (let t = 0; t < e.length; t++) if ((t === 3 ? /[^/*0-9,\-WwLl]+/ : t === 5 ? /[^/*0-9,\-#Ll]+/ : /[^/*0-9,\-]+/).test(e[t])) throw new TypeError("CronPattern: configuration entry " + t + " (" + e[t] + ") contains illegal characters.");
  }
  handleNumber(e, t, n, r) {
    let a = this.extractNth(e, t), i = e.toUpperCase().includes("W");
    if (t !== "day" && i) throw new TypeError("CronPattern: Nearest weekday modifier (W) only allowed in day-of-month.");
    i && (t = "nearestWeekdays");
    let o = i ? a[0].replace(/W$/i, "") : a[0], u = s.toNumber(o) + n;
    if (isNaN(u)) throw new TypeError("CronPattern: " + t + " is not a number: '" + e + "'");
    this.setPart(t, u, a[1] || r);
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
  static toNumber(e) {
    return /^\d+$/.test(e) ? parseInt(e, 10) : NaN;
  }
  validateNotNaN(e, t) {
    if (isNaN(e)) throw new TypeError(t);
  }
  validateRange(e, t, n, r, a) {
    if (e > t) throw new TypeError("CronPattern: From value is larger than to value: '" + a + "'");
    if (n !== void 0) {
      if (n === 0) throw new TypeError("CronPattern: Syntax error, illegal stepping: 0");
      if (n > this[r].length) throw new TypeError("CronPattern: Syntax error, steps cannot be greater than maximum value of part (" + this[r].length + ")");
    }
  }
  handleRangeWithStepping(e, t, n, r) {
    if (e.toUpperCase().includes("W")) throw new TypeError("CronPattern: Syntax error, W is not allowed in ranges with stepping.");
    let a = this.extractNth(e, t), i = a[0].match(/^(\d+)-(\d+)\/(\d+)$/);
    if (i === null) throw new TypeError("CronPattern: Syntax error, illegal range with stepping: '" + e + "'");
    let [, o, u, l] = i, h = parseInt(o, 10) + n, m = parseInt(u, 10) + n, d = parseInt(l, 10);
    this.validateNotNaN(h, "CronPattern: Syntax error, illegal lower range (NaN)"), this.validateNotNaN(m, "CronPattern: Syntax error, illegal upper range (NaN)"), this.validateNotNaN(d, "CronPattern: Syntax error, illegal stepping: (NaN)"), this.validateRange(h, m, d, t, e);
    for (let p = h; p <= m; p += d) this.setPart(t, p, a[1] || r);
  }
  extractNth(e, t) {
    let n = e, r;
    if (n.includes("#")) {
      if (t !== "dayOfWeek") throw new Error("CronPattern: nth (#) only allowed in day-of-week field");
      let a = n.split("#");
      if (a.length !== 2 || a[1] === "") throw new TypeError("CronPattern: Syntax error, illegal nth weekday: '" + e + "'");
      n = a[0], r = a[1];
    } else if (n.toUpperCase().endsWith("L")) {
      if (t !== "dayOfWeek") throw new Error("CronPattern: L modifier only allowed in day-of-week field (use L alone for day-of-month)");
      r = "L", n = n.slice(0, -1);
    }
    return [n, r];
  }
  handleRange(e, t, n, r) {
    if (e.toUpperCase().includes("W")) throw new TypeError("CronPattern: Syntax error, W is not allowed in a range.");
    let a = this.extractNth(e, t), i = a[0].split("-");
    if (i.length !== 2) throw new TypeError("CronPattern: Syntax error, illegal range: '" + e + "'");
    let o = s.toNumber(i[0]) + n, u = s.toNumber(i[1]) + n;
    this.validateNotNaN(o, "CronPattern: Syntax error, illegal lower range (NaN)"), this.validateNotNaN(u, "CronPattern: Syntax error, illegal upper range (NaN)"), this.validateRange(o, u, void 0, t, e);
    for (let l = o; l <= u; l++) this.setPart(t, l, a[1] || r);
  }
  handleStepping(e, t, n, r) {
    if (e.toUpperCase().includes("W")) throw new TypeError("CronPattern: Syntax error, W is not allowed in parts with stepping.");
    let a = this.extractNth(e, t), i = a[0].split("/");
    if (i.length !== 2) throw new TypeError("CronPattern: Syntax error, illegal stepping: '" + e + "'");
    if (this.sloppyRanges) i[0] === "" && (i[0] = "*");
    else {
      if (i[0] === "") throw new TypeError("CronPattern: Syntax error, stepping with missing prefix ('" + e + "') is not allowed. Use wildcard (*/step) or range (min-max/step) instead.");
      if (i[0] !== "*") throw new TypeError("CronPattern: Syntax error, stepping with numeric prefix ('" + e + "') is not allowed. Use wildcard (*/step) or range (min-max/step) instead.");
    }
    let o = 0;
    i[0] !== "*" && (o = s.toNumber(i[0]) + n, this.validateNotNaN(o, "CronPattern: Syntax error, illegal stepping start (NaN)"));
    let u = s.toNumber(i[1]);
    this.validateNotNaN(u, "CronPattern: Syntax error, illegal stepping: (NaN)"), this.validateRange(0, this[t].length - 1, u, t, e);
    for (let l = o; l < this[t].length; l += u) t === "year" && l === 0 || this.setPart(t, l, a[1] || r);
  }
  replaceAlpha(e, t) {
    let n = e;
    for (let [r, a] of Object.entries(t)) n = n.replace(new RegExp(r, "gi"), (i, o, u) => {
      let l = o + i.length - 3, h = u.charAt(l - 1), m = u.charAt(l + 3);
      if (/[a-z\d/#]/i.test(h) || /[a-km-z\d]/i.test(m)) throw new TypeError(`CronPattern: Syntax error, a month or day name has to stand alone ('${e}').`);
      return a;
    });
    return n;
  }
  static DAY_NAMES = { "-sun": "-7", sun: "0", mon: "1", tue: "2", wed: "3", thu: "4", fri: "5", sat: "6" };
  static DAY_NAMES_QUARTZ = { sun: "1", mon: "2", tue: "3", wed: "4", thu: "5", fri: "6", sat: "7" };
  static MONTH_NAMES = { jan: "1", feb: "2", mar: "3", apr: "4", may: "5", jun: "6", jul: "7", aug: "8", sep: "9", oct: "10", nov: "11", dec: "12" };
  static NICKNAME_MAP = { "@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *", "@monthly": "0 0 1 * *", "@weekly": "0 0 * * SUN", "@daily": "0 0 * * *", "@midnight": "0 0 * * *", "@hourly": "0 * * * *" };
  handleNicknames(e) {
    let t = e.trim().toLowerCase();
    if (t === "@reboot") throw new TypeError("CronPattern: @reboot is not supported in this environment. This is an event-based trigger that requires system startup detection.");
    return s.NICKNAME_MAP[t] || e;
  }
  setNthWeekdayOfMonth(e, t) {
    if (typeof t != "number" && t.toUpperCase() === "L") this.dayOfWeek[e] = this.dayOfWeek[e] | 32;
    else if (t === 63) this.dayOfWeek[e] = 63;
    else if (t < 6 && t > 0) this.dayOfWeek[e] = this.dayOfWeek[e] | x[t - 1];
    else throw new TypeError(`CronPattern: nth weekday out of range, should be 1-5 or L. Value: ${t}, Type: ${typeof t}`);
  }
};
var B = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
var f = [["month", "year", 0], ["day", "month", -1], ["hour", "day", 0], ["minute", "hour", 0], ["second", "minute", 0]];
var c = class s2 {
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
    else if (e instanceof s2) this.fromCronDate(e);
    else throw new TypeError("CronDate: Invalid type (" + typeof e + ") passed to CronDate constructor");
  }
  getLastDayOfMonth(e, t) {
    return t !== 1 ? B[t] : new Date(g(e, t + 1, 0)).getUTCDate();
  }
  getLastWeekday(e, t) {
    let n = this.getLastDayOfMonth(e, t), a = new Date(g(e, t, n)).getUTCDay();
    return a === 0 ? n - 2 : a === 6 ? n - 1 : n;
  }
  getNearestWeekday(e, t, n) {
    let r = this.getLastDayOfMonth(e, t);
    if (n > r) return -1;
    let i = new Date(g(e, t, n)).getUTCDay();
    return i === 0 ? n === r ? n - 2 : n + 1 : i === 6 ? n === 1 ? n + 2 : n - 1 : n;
  }
  isNthWeekdayOfMonth(e, t, n, r) {
    let i = new Date(g(e, t, n)).getUTCDay(), o = 0;
    for (let u = 1; u <= n; u++) new Date(g(e, t, u)).getUTCDay() === i && o++;
    if (r & 63 && x[o - 1] & r) return true;
    if (r & 32) {
      let u = this.getLastDayOfMonth(e, t);
      for (let l = n + 1; l <= u; l++) if (new Date(g(e, t, l)).getUTCDay() === i) return false;
      return true;
    }
    return false;
  }
  fromDate(e) {
    if (this.tz !== void 0) if (typeof this.tz == "number") this.ms = e.getUTCMilliseconds(), this.second = e.getUTCSeconds(), this.minute = e.getUTCMinutes() + this.tz, this.hour = e.getUTCHours(), this.day = e.getUTCDate(), this.month = e.getUTCMonth(), this.year = e.getUTCFullYear(), this.apply();
    else try {
      let t = y(e, this.tz);
      this.ms = e.getMilliseconds(), this.second = t.s, this.minute = t.i, this.hour = t.h, this.day = t.d, this.month = t.m - 1, this.year = t.y, this.cacheInstant(e.getTime() - this.ms);
    } catch (t) {
      let n = t instanceof Error ? t.message : String(t);
      throw new TypeError(`CronDate: Failed to convert date to timezone '${this.tz}'. This may happen with invalid timezone names or dates. Original error: ${n}`);
    }
    else this.ms = e.getMilliseconds(), this.second = e.getSeconds(), this.minute = e.getMinutes(), this.hour = e.getHours(), this.day = e.getDate(), this.month = e.getMonth(), this.year = e.getFullYear(), this.cacheInstant(e.getTime() - this.ms);
  }
  cacheInstant(e) {
    this.instantMs = e;
    let { year: t, month: n, day: r, hour: a, minute: i, second: o } = this;
    this.instantFields = [t, n, r, a, i, o, this.tz];
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
      if (O(e)) return this.fromDate(v(e));
      let t = v(e, "Etc/UTC");
      this.ms = t.getUTCMilliseconds(), this.second = t.getUTCSeconds(), this.minute = t.getUTCMinutes(), this.hour = t.getUTCHours(), this.day = t.getUTCDate(), this.month = t.getUTCMonth(), this.year = t.getUTCFullYear(), this.apply();
    } else return this.fromDate(v(e, this.tz));
  }
  findNext(e, t, n, r) {
    return this._findMatch(e, t, n, r, 1);
  }
  _matchDayValue(e, t, n, r) {
    let a = e;
    if (!a) {
      for (let i = 0; i < n.nearestWeekdays.length; i++) if (n.nearestWeekdays[i]) {
        let o = this.getNearestWeekday(this.year, this.month, i + 1);
        if (o !== -1 && o === t) {
          a = 1;
          break;
        }
      }
    }
    if (n.lastWeekday) {
      let i = this.getLastWeekday(this.year, this.month);
      t === i && (a = 1);
    }
    if (n.lastDayOfMonth) {
      let i = this.getLastDayOfMonth(this.year, this.month);
      t === i && (a = 1);
    }
    if (!n.starDOW) {
      let i = new Date(g(this.year, this.month, 1, 0, 0, 0, 0)).getUTCDay(), o = n.dayOfWeek[(i + (t - 1)) % 7];
      o && o & 63 && (o = this.isNthWeekdayOfMonth(this.year, this.month, t, o) ? 1 : 0), n.useAndLogic ? a = a && o : !r.domAndDow && !n.starDOM ? a = a || o : a = a && o;
    }
    return a;
  }
  _findMatch(e, t, n, r, a) {
    let i = this[t], o = t === "day" ? this.getLastDayOfMonth(this.year, this.month) : 0, u = this[t] + r, l = a === 1 ? (h) => h < n[t].length : (h) => h >= 0;
    for (let h = u; l(h); h += a) {
      let m = n[t][h];
      if (t === "day") {
        if (h - r > o) continue;
        m = this._matchDayValue(m, h - r, n, e);
      }
      if (m) return this[t] = h - r, i !== this[t] ? 2 : 1;
    }
    return 3;
  }
  recurse(e, t, n) {
    let r = this.year, a = n;
    for (; typeof a == "number"; ) {
      if (e.starYear && Math.abs(this.year - r) > 400) return null;
      a = this.recurseStep(e, t, a);
    }
    return a;
  }
  recurseStep(e, t, n) {
    if (n === 0 && !e.starYear) {
      if (this.year >= 0 && this.year < e.year.length && e.year[this.year] === 0) {
        let a = -1;
        for (let i = this.year + 1; i < e.year.length && i < 1e4; i++) if (e.year[i] === 1) {
          a = i;
          break;
        }
        if (a === -1) return null;
        this.year = a, this.month = 0, this.day = 1, this.hour = 0, this.minute = 0, this.second = 0, this.ms = 0;
      }
      if (this.year >= 1e4) return null;
    }
    let r = this.findNext(t, f[n][0], e, f[n][2]);
    if (r > 1) {
      let a = n + 1;
      for (; a < f.length; ) this[f[a][0]] = -f[a][2], a++;
      if (r === 3) {
        if (this[f[n][1]]++, this[f[n][0]] = -f[n][2], this.apply(), n === 0 && !e.starYear) {
          for (; this.year >= 0 && this.year < e.year.length && e.year[this.year] === 0 && this.year < 1e4; ) this.year++;
          if (this.year >= 1e4 || this.year >= e.year.length) return null;
        }
        return 0;
      } else if (this.apply()) return n - 1;
    }
    return n += 1, n >= f.length ? this : (e.starYear ? this.year >= 3e3 : this.year >= 1e4) ? null : n;
  }
  increment(e, t, n) {
    return this.instantMs = void 0, this.instantFields = void 0, this.second += t.interval !== void 0 && t.interval > 1 && n ? t.interval : 1, this.ms = 0, this.apply(), this.recurse(e, t, 0);
  }
  decrement(e, t, n = true) {
    this.instantMs = void 0, this.instantFields = void 0;
    let r = t.interval !== void 0 && t.interval >= 1;
    return (this.ms === 0 || r) && (this.second -= r && n ? t.interval : 1), this.ms = 0, this.apply(), this.year < 1 ? null : this.recurseBackward(e, t, 0);
  }
  recurseBackward(e, t, n) {
    let r = this.year, a = n;
    for (; typeof a == "number"; ) {
      if (e.starYear && Math.abs(this.year - r) > 400) return null;
      a = this.recurseBackwardStep(e, t, a);
    }
    return a;
  }
  recurseBackwardStep(e, t, n) {
    if (n === 0 && !e.starYear) {
      if (this.year >= 0 && this.year < e.year.length && e.year[this.year] === 0) {
        let a = -1;
        for (let i = this.year - 1; i >= 0; i--) if (e.year[i] === 1) {
          a = i;
          break;
        }
        if (a === -1) return null;
        this.year = a, this.month = 11, this.day = 31, this.hour = 23, this.minute = 59, this.second = 59, this.ms = 0;
      }
      if (this.year < 1) return null;
    }
    let r = this.findPrevious(t, f[n][0], e, f[n][2]);
    if (r > 1) {
      let a = n + 1;
      for (; a < f.length; ) {
        let i = f[a][0], o = f[a][2], u = this.getMaxPatternValue(i, e, o);
        this[i] = u, a++;
      }
      if (r === 3) {
        if (this[f[n][1]]--, n === 0) {
          let u = this.getLastDayOfMonth(this.year, this.month);
          this.day > u && (this.day = u);
        }
        if (n === 1) if (this.day <= 0) this.day = 1;
        else {
          let u = this.year, l = this.month;
          for (; l < 0; ) l += 12, u--;
          for (; l > 11; ) l -= 12, u++;
          let h = this.getLastDayOfMonth(u, l);
          this.day > h && (this.day = h);
        }
        this.apply();
        let i = f[n][0], o = f[n][2];
        if (this[i] = this.getMaxPatternValue(i, e, o), this.apply(), n === 0) {
          let u = f[1][2], l = this.getMaxPatternValue("day", e, u);
          l !== this.day && (this.day = l, this.hour = this.getMaxPatternValue("hour", e, f[2][2]), this.minute = this.getMaxPatternValue("minute", e, f[3][2]), this.second = this.getMaxPatternValue("second", e, f[4][2]));
        }
        if (n === 0 && !e.starYear) {
          for (; this.year >= 0 && this.year < e.year.length && e.year[this.year] === 0; ) this.year--;
          if (this.year < 1) return null;
        }
        return 0;
      } else if (this.apply()) return n - 1;
    }
    return n += 1, n >= f.length ? this : this.year < 1 ? null : n;
  }
  getMaxPatternValue(e, t, n) {
    let r = t[e].length - 1 - n;
    for (let a = t[e].length - 1; a >= 0; a--) if (t[e][a]) {
      r = a - n;
      break;
    }
    if (e === "day") {
      let a = this.getLastDayOfMonth(this.year, this.month);
      return t.lastWeekday || t.nearestWeekdays.some((i) => i) || t.lastDayOfMonth || !t.starDOW ? a : Math.min(r, a);
    }
    return r;
  }
  findPrevious(e, t, n, r) {
    return this._findMatch(e, t, n, r, -1);
  }
  getDate(e) {
    if (!e && typeof this.tz != "number") {
      let t = this.cachedInstant();
      if (t !== void 0) return new Date(t + (this.tz === void 0 ? this.ms : 0));
    }
    if (e || this.tz === void 0) {
      if (this.year >= 0 && this.year < 100) {
        let t = /* @__PURE__ */ new Date(0);
        return t.setFullYear(this.year, this.month, this.day), t.setHours(this.hour, this.minute, this.second, this.ms), t;
      }
      return new Date(this.year, this.month, this.day, this.hour, this.minute, this.second, this.ms);
    } else return typeof this.tz == "number" ? new Date(g(this.year, this.month, this.day, this.hour, this.minute - this.tz, this.second, this.ms)) : N(T(this.year, this.month + 1, this.day, this.hour, this.minute, this.second, this.tz), false);
  }
  getTime() {
    return this.getDate(false).getTime();
  }
  match(e, t) {
    if (!e.starYear && (this.year < 0 || this.year >= e.year.length || e.year[this.year] === 0)) return false;
    for (let n = 0; n < f.length; n++) {
      let r = f[n][0], a = f[n][2], i = this[r];
      if (i + a < 0 || i + a >= e[r].length) return false;
      let o = e[r][i + a];
      if (r === "day" && (o = this._matchDayValue(o, i, e, t)), !o) return false;
    }
    return true;
  }
};
function F(s3) {
  s3 === void 0 && (s3 = {}), delete s3.name, s3.legacyMode !== void 0 && s3.domAndDow === void 0 ? s3.domAndDow = !s3.legacyMode : s3.domAndDow === void 0 && (s3.domAndDow = false), s3.legacyMode = !s3.domAndDow;
  let e = (n, r) => n === void 0 ? r : n;
  if (s3.paused = e(s3.paused, false), s3.maxRuns = e(s3.maxRuns, 1 / 0), s3.catch = e(s3.catch, false), s3.unref = e(s3.unref, false), s3.mode = e(s3.mode, "auto"), s3.alternativeWeekdays = e(s3.alternativeWeekdays, false), s3.sloppyRanges = e(s3.sloppyRanges, false), s3.fixedTimeRule = e(s3.fixedTimeRule, "single-value"), s3.allowPast = e(s3.allowPast, false), s3.interval = s3.interval === void 0 ? 0 : parseInt(s3.interval.toString(), 10), s3.utcOffset = s3.utcOffset === void 0 ? void 0 : parseInt(s3.utcOffset.toString(), 10), s3.dayOffset = s3.dayOffset === void 0 ? 0 : parseInt(s3.dayOffset.toString(), 10), !["auto", "5-part", "6-part", "7-part", "5-or-6-parts", "6-or-7-parts"].includes(s3.mode)) throw new Error("CronOptions: mode must be one of 'auto', '5-part', '6-part', '7-part', '5-or-6-parts', or '6-or-7-parts'.");
  if (!["single-value", "no-wildcard"].includes(s3.fixedTimeRule)) throw new Error("CronOptions: fixedTimeRule must be one of 'single-value' or 'no-wildcard'.");
  s3.timezone === "" && (s3.timezone = void 0);
  let t = s3.timezone ?? s3.utcOffset;
  if (s3.startAt && (s3.startAt = I(new c(s3.startAt, t))), s3.stopAt && (s3.stopAt = I(new c(s3.stopAt, t))), s3.interval !== null) {
    if (isNaN(s3.interval)) throw new Error("CronOptions: Supplied value for interval is not a number");
    if (s3.interval < 0) throw new Error("CronOptions: Supplied value for interval can not be negative");
  }
  if (s3.utcOffset !== void 0) {
    if (isNaN(s3.utcOffset)) throw new Error("CronOptions: Invalid value passed for utcOffset, should be number representing minutes offset from UTC.");
    if (s3.utcOffset < -870 || s3.utcOffset > 870) throw new Error("CronOptions: utcOffset out of bounds.");
    if (s3.utcOffset !== void 0 && s3.timezone) throw new Error("CronOptions: Combining 'utcOffset' with 'timezone' is not allowed.");
  }
  if (s3.unref !== true && s3.unref !== false) throw new Error("CronOptions: Unref should be either true, false or undefined(false).");
  if (s3.dayOffset !== void 0 && s3.dayOffset !== 0 && isNaN(s3.dayOffset)) throw new Error("CronOptions: Invalid value passed for dayOffset, should be a number representing days to offset.");
  return s3;
}
function I(s3) {
  return s3.ms ? (s3.ms = 0, new c(new Date(s3.getTime() + 1e3), s3.tz)) : s3;
}
function b(s3) {
  return typeof s3 == "function";
}
function L(s3) {
  return b(s3);
}
function Y(s3) {
  typeof Deno < "u" && typeof Deno.unrefTimer < "u" ? Deno.unrefTimer(s3) : s3 && typeof s3.unref < "u" && s3.unref();
}
var P = 1440 * 60 * 1e3;
var $ = 30 * 1e3;
var M = [];
var j = class {
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
    return (this.options.dayOffset ?? 0) * P;
  }
  get _startAt() {
    return this.options.startAt;
  }
  get _stopAt() {
    return this.options.stopAt;
  }
  _nextTarget(e, t) {
    let n = this._offsetMs(), r = this._next(this._shift(e), new Date(t.getTime() - n), { startAt: this._shift(this._startAt), stopAt: this._shift(this._stopAt) });
    return r ? this.applyDayOffset(r.getDate(false)) : null;
  }
  _previousTarget(e, t) {
    let n = this._toMs(e ?? t) - this._offsetMs(), r = this._previous(new Date(n), { startAt: this._shift(this._startAt), stopAt: this._shift(this._stopAt) });
    return r ? this.applyDayOffset(r.getDate(false)) : null;
  }
  _shift(e) {
    if (e != null) return new c(new Date(this._toMs(e) - this._offsetMs()), this.getTz());
  }
  constructor(e, t, n) {
    let r, a;
    if (b(t)) a = t;
    else if (typeof t == "object") r = t;
    else if (t !== void 0) throw new Error("Cron: Invalid argument passed for optionsIn. Should be one of function, or object (options).");
    if (b(n)) a = n;
    else if (typeof n == "object") r = n;
    else if (n !== void 0) throw new Error("Cron: Invalid argument passed for funcIn. Should be one of function, or object (options).");
    this.name = r?.name;
    let i = r?.startAt;
    this.options = F(r), i && this._startAt && (this._startAtRoundingMs = this._startAt.getTime() - new c(i, this.getTz()).getTime());
    let o = !!e && (e instanceof Date || typeof e == "string" && e.indexOf(":") > 0);
    if (this._states = { kill: false, blocking: false, previousRun: void 0, currentRun: void 0, once: void 0, currentTimeout: void 0, maxRuns: r ? r.maxRuns : void 0, paused: r ? r.paused : false, pattern: o ? new C("* * * * *", void 0, { mode: "auto" }) : new C(e, this.options.timezone, { mode: this.options.mode, alternativeWeekdays: this.options.alternativeWeekdays, sloppyRanges: this.options.sloppyRanges, fixedTimeRule: this.options.fixedTimeRule }) }, o && (this._states.once = new c(e, this.getTz())), this.name) {
      if (M.find((l) => l.name === this.name)) throw new Error("Cron: Tried to initialize new named job '" + this.name + "', but name already taken.");
      M.push(this);
    }
    return a !== void 0 && L(a) && (this.fn = a, this.schedule()), this;
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
    let r = [], a = /* @__PURE__ */ new Date(), i = t;
    for (; e-- > 0; ) {
      let o = n === "next" ? this._nextTarget(i, a) : this._previousTarget(i, a);
      if (!o) break;
      r.push(o), i = o;
    }
    return r;
  }
  match(e) {
    if (this._states.once) {
      let n = new c(e, this.getTz());
      n.ms = 0;
      let r = new c(this._states.once, this.getTz());
      return r.ms = 0, n.getTime() === r.getTime();
    }
    let t = new c(e, this.getTz());
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
    let e = M.indexOf(this);
    e >= 0 && M.splice(e, 1);
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
    let n = t ?? /* @__PURE__ */ new Date(), r = this._nextTarget(void 0, n), a = r ? r.getTime() - n.getTime() : null, i = this._states.currentRun;
    i && i.getTime() > n.getTime() && (i = void 0);
    let o = this._nextTarget(i, n);
    return a === null || isNaN(a) || o === null ? this : (a > $ && (a = $), a < 0 && (a = this._states.paused ? 1e3 : 0), this._states.currentTimeout = setTimeout(() => this._checkTrigger(o), a), this._states.currentTimeout && this.options.unref && Y(this._states.currentTimeout), this);
  }
  async _trigger(e = /* @__PURE__ */ new Date()) {
    this._states.blocking = true, this._states.currentRun = new c(e, this.getTz());
    try {
      if (this.options.catch) try {
        this.fn !== void 0 && await this.fn(this, this.options.context);
      } catch (t) {
        if (b(this.options.catch)) try {
          Promise.resolve(this.options.catch(t, this)).catch(() => {
          });
        } catch {
        }
      }
      else this.fn !== void 0 && await this.fn(this, this.options.context);
    } finally {
      this._states.previousRun = new c(e, this.getTz()), this._states.blocking = false;
    }
  }
  async trigger() {
    await this._trigger();
  }
  runsLeft() {
    return this._states.maxRuns;
  }
  _checkTrigger(e) {
    let t = /* @__PURE__ */ new Date(), n = !this._states.paused && t.getTime() >= e.getTime(), r = this._states.blocking && this.options.protect;
    n && !r ? (this._states.maxRuns !== void 0 && this._states.maxRuns--, this._trigger(t)) : n && r && b(this.options.protect) && setTimeout(() => this.options.protect(this), 0), this._schedule(void 0, n ? void 0 : t);
  }
  _next(e, t, n) {
    let r = n ? n.startAt : this._startAt, a = n ? n.stopAt : this._stopAt, i = !!e, o = !!(e || this._states.currentRun), u = false;
    !e && r && this.options.interval && (e = this._calculatePreviousRun(r, t), e && (o = true), u = !e);
    let l = e ?? t ?? /* @__PURE__ */ new Date(), h = this._toMs(l);
    e = new c(l, this.getTz()), r && h < r.getTime() && (e = u ? r : new c(new Date(r.getTime() - 1e3), this.getTz()), h = e.getTime());
    let m = this._states.once || new c(e, this.getTz());
    if (!u && m !== this._states.once && (m = this._findOccurrence(e, h, 1, o)), this._states.once) {
      let d = this._states.once.getTime(), p = h - d;
      if (r && d < r.getTime() - this._startAtRoundingMs || this._states.currentRun && this.applyDayOffset(this._states.once.getDate()).getTime() <= this._states.currentRun.getTime() || (i ? p >= 0 : p > 1e3 && !this.options.allowPast)) return null;
    }
    return m === null || this._states.maxRuns !== void 0 && this._states.maxRuns <= 0 || this._states.kill || a && m.getTime() >= a.getTime() ? null : m;
  }
  _previous(e, t) {
    let n = this._toMs(e ?? /* @__PURE__ */ new Date());
    if (t.stopAt && n > t.stopAt.getTime() && (n = t.stopAt.getTime()), this._states.once) return this._states.once.getTime() < n ? this._states.once : null;
    let r = this._findOccurrence(new c(new Date(n), this.getTz()), n, -1);
    return r === null || t.startAt && r.getTime() < t.startAt.getTime() ? null : r;
  }
  _toMs(e) {
    if (e instanceof Date) return e.getTime();
    let t = new c(e, this.getTz());
    return t.getTime() + (typeof t.tz == "string" ? t.ms : 0);
  }
  _findOccurrence(e, t, n, r = false) {
    let a = this.options.interval ?? 0;
    if (a > 1 && (n === -1 || r)) {
      t += (a - 1) * 1e3 * n;
      let m = new Date(t);
      if (isNaN(m.getTime()) || (e = new c(m, this.getTz()), !(e.year >= 1 && e.year <= 9999))) return null;
      r = false;
    }
    let i = this._walk(e, t, n, r), o = this.getTz();
    if (this._states.pattern.isFixedTime && n === 1 || typeof o == "number" || U(t, o) !== (n === 1 ? "earlier" : "later")) return i;
    let u = Math.floor(t / 1e3) * 1e3, l = n === 1 ? k(u, u + P, o) : k(u - P, u, o) - 1e3;
    if (i && (l - i.getTime()) * n > 0) return i;
    let h = this._walk(new c(new Date(l), o), l - n, n, false, true);
    return !i || !h ? i ?? h : (i.getTime() - h.getTime()) * n > 0 ? h : i;
  }
  _walk(e, t, n, r, a = false) {
    let i = this._states.pattern, o = (l, h) => n === 1 ? l.increment(i, this.options, h) : l.decrement(i, this.options, h), u = new c(e, this.getTz());
    for ((!a || !u.match(i, this.options)) && (u = o(u, r)); u; ) {
      let l = this._runAt(u, t, n);
      if (l) return new c(l, this.getTz());
      u = o(u, false);
    }
    return null;
  }
  _runAt(e, t, n) {
    let r = (d) => (d.getTime() - t) * n > 0, a = e.tz;
    if (typeof a == "number") {
      let d = e.getDate();
      return r(d) ? d : null;
    }
    let i = T(e.year, e.month + 1, e.day, e.hour, e.minute, e.second, a), o = _(i), u = this._states.pattern.isFixedTime;
    if (o.kind === "single") return r(o.date) ? o.date : null;
    if (o.kind === "ambiguous") {
      let { earlier: d, later: p } = o;
      return (u ? [d] : n === 1 ? [d, p] : [p, d]).find(r) ?? null;
    }
    if (!u) return null;
    let l = new Date(k(o.before.getTime(), o.after.getTime(), a)), h = y(l, a);
    return h.y === i.y && h.m === i.m && h.d === i.d && r(l) ? l : null;
  }
  _calculatePreviousRun(e, t) {
    let n = new c(t, this.getTz()).getTime();
    if (e.getTime() > n) return;
    let r = this.options.interval * 1e3, a = e;
    for (; a.getTime() + r <= n; ) {
      let i = this._findOccurrence(a, a.getTime(), 1, true);
      if (i === null) break;
      a = i;
    }
    return a;
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
  j as Cron
};
