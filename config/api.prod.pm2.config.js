module.exports = {
  apps: [{
    name: 'bridge-api',
    script: './bin/storj-bridge.js',
    env: {
      STORJ_NETWORK: 'INXT',
      STORJ_BRIDGE: 'https://api.internxt.com',
      NODE_ENV: 'production'
    }
  }]
};
