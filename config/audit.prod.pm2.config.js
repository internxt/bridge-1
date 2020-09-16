module.exports = {
    apps: [{
        name: 'bridge-audit',
        script: './bin/storj-audit-tool.js',
        cwd: '/root/bridge',
        args: '-c /root/.inxt-bridge/config/production - o /root/shards',
        env: {
            STORJ_NETWORK: 'INXT',
            STORJ_BRIDGE: 'https://api.internxt.com',
            NODE_ENV: 'production'
        }
    }]
}

