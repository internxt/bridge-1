module.exports = {
    apps: [{
        name: 'bridge-monitor',
        script: './bin/storj-monitor.js',
        args: '-c /root/.inxt-bridge/config/production',
        env: {
            STORJ_NETWORK: 'INXT',
            STORJ_BRIDGE: 'https://api.internxt.com',
            NODE_ENV: 'production'
        }
    }]
}
