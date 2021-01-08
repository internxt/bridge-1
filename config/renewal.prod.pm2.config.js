module.exports = {
  apps: [{
    name: 'bridge-renewal',
    script: './bin/storj-contract-renew.js',
    cwd: '/root/bridge',
    args: '-c /root/.inxt-bridge/config/production',
    env: {
      STORJ_NETWORK: 'INXT',
      STORJ_BRIDGE: 'https://api.internxt.com',
      NODE_ENV: 'production'
    }
  }]
};

