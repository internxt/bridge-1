module.exports = {
    apps: [{
        name: 'bridge-replicator',
        script: './bin/inxt-replicator.js',
        cwd: '/root/bridge',
        args: '-c /root/.inxt-bridge/config/production',
        env: {
            STORJ_NETWORK: 'INXT',
            STORJ_BRIDGE: 'https://api.internxt.com',
            NODE_ENV: 'production'
        }
    }]
}

