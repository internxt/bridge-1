module.exports = {
    apps: [{
        name: 'bridge-cleaner',
        script: 'storj-cleaner',
        args: '-c /root/.storj-bridge/config/production',
        env: {
            STORJ_NETWORK: 'INXT',
            STORJ_BRIDGE: 'https://api.internxt.com',
            NODE_ENV: 'production'
        }
    }]
}