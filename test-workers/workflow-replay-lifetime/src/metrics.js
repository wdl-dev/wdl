export const gauges = new Map();
export const metrics = {
  increment() {},
  setGauge(name, _labels, value) { gauges.set(name, value); },
};
