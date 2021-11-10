
module.exports = {
  
  initialValue: (init) => ({
    pipe: (...fns) => fns.reduce((pipedVal, fn) => fn(pipedVal), init)
  }),

  pipeLog: (thing, log = console.log) => {
    log(thing);
    return thing;
  }
};
