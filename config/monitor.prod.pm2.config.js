module.exports = {
    apps: [{
        name: 'bridge-monitor',
        script: 'storj-monitor',
        args: '-- -c /root/.storj-bridge/config/production',
        env: {
            STORJ_NETWORK: 'INXT',
            STORJ_BRIDGE: 'https://api.internxt.com',
            NODE_ENV: 'production'
        }
    }]
}
