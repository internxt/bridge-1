'use strict';

module.exports = function RouteFactory(options) {
  return ([
    require('./routes/buckets'),
    require('./routes/pubkeys'),
    require('./routes/users'),
    require('./routes/frames'),
    require('./routes/contacts'),
    require('./routes/reports'),
    require('./routes/health'),
    require('./routes/stripe'),
    require('./routes/gateway')
  ]).map(function(Router) {
    return new Router({
      config: options.config,
      network: options.network,
      storage: options.storage,
      mailer: options.mailer,
      contracts: options.contracts,
      redis: options.redis
    }).getEndpointDefinitions();
  }).reduce(function(set1, set2) {
    return set1.concat(set2);
  }, []);
};
