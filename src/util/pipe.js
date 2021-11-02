
module.exports = (...fns) => (initialVal) => fns.reduce(
  (pipedVal, fn) => fn(pipedVal),
  initialVal
);

module.exports.pipe = module.exports;

module.exports.pipelog = (thing, log = console.log) => {
  log(thing);
  return thing;
}
