const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

    if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
    }

    if (req.url === '/version') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            versionCode: 9,
            versionName: '1.0.8',
            apkUrl: 'https://calc-vault-server.onrender.com/download/app-release.apk',
            changelog: 'Private Suite Messenger: Photos, Videos, Voice Notes, View Once, Persistent Chat, Panic Button, and WebRTC network handover fix'
        }));
        return;
    }

    if (req.url === '/download/app-release.apk') {
        const apkPath = path.join(__dirname, 'public', 'app-release.apk');
        if (fs.existsSync(apkPath)) {
            const stat = fs.statSync(apkPath);
            res.writeHead(200, {
                'Content-Type': 'application/vnd.android.package-archive',
                'Content-Length': stat.size,
                'Content-Disposition': 'attachment; filename="Calculator_Vault.apk"'
            });
            fs.createReadStream(apkPath).pipe(res);
            return;
        } else {
            res.writeHead(404, { 'Content-Type': 'text/plain' });
            res.end('APK not found on server\n');
            return;
        }
    }

    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Calculator Vault Signaling Server is Active\n');
});

const wss = new WebSocket.Server({ server });

// Map of roomId -> Map of clientId -> { ws, inVault, lastSeen }
const rooms = new Map();

// Automatic dead-socket purger every 15 seconds
const heartbeatInterval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            return ws.terminate();
        }
        ws.isAlive = false;
        try {
            ws.ping();
        } catch (_) {}
    });
}, 15000);

wss.on('connection', (ws) => {
    ws.isAlive = true;
    let currentRoom = null;
    let currentClientId = null;

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('message', (messageText) => {
        ws.isAlive = true;
        try {
            const data = JSON.parse(messageText);
            const { type, roomId, clientId, inVault = false, payload } = data;

            switch (type) {
                case 'join': {
                    if (!roomId) break;
                    currentRoom = roomId;
                    currentClientId = clientId || `anon_${Math.random()}`;

                    if (!rooms.has(roomId)) {
                        rooms.set(roomId, new Map());
                    }
                    const roomClients = rooms.get(roomId);

                    // If client already had a socket connected, terminate old ghost socket
                    const existing = roomClients.get(currentClientId);
                    if (existing && existing.ws !== ws) {
                        try { existing.ws.terminate(); } catch (_) {}
                    }

                    roomClients.set(currentClientId, {
                        ws,
                        inVault: !!inVault,
                        lastSeen: Date.now()
                    });

                    // Check if ANOTHER distinct client is actively in the vault
                    let partnerInVault = false;
                    for (const [cId, client] of roomClients.entries()) {
                        if (cId !== currentClientId && client.ws.readyState === WebSocket.OPEN && client.inVault) {
                            partnerInVault = true;
                            break;
                        }
                    }

                    // Inform the connecting client if partner is in vault
                    ws.send(JSON.stringify({
                        type: 'joined',
                        roomId,
                        peerPresent: partnerInVault
                    }));

                    // If this client joined inVault, notify other live clients
                    if (inVault) {
                        for (const [cId, client] of roomClients.entries()) {
                            if (cId !== currentClientId && client.ws.readyState === WebSocket.OPEN) {
                                client.ws.send(JSON.stringify({
                                    type: 'peer_joined',
                                    roomId
                                }));
                            }
                        }
                    }
                    break;
                }

                case 'vault_status': {
                    if (currentRoom && rooms.has(currentRoom) && currentClientId) {
                        const roomClients = rooms.get(currentRoom);
                        const client = roomClients.get(currentClientId);
                        if (client) {
                            client.inVault = !!inVault;
                            client.lastSeen = Date.now();
                        }

                        // Broadcast partner's presence change to all other peers in the room
                        for (const [cId, cl] of roomClients.entries()) {
                            if (cId !== currentClientId && cl.ws.readyState === WebSocket.OPEN) {
                                cl.ws.send(JSON.stringify({
                                    type: inVault ? 'peer_joined' : 'peer_left',
                                    roomId: currentRoom
                                }));
                            }
                        }
                    }
                    break;
                }

                case 'ping': {
                    if (ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({ type: 'pong' }));
                    }
                    break;
                }

                case 'chat':
                case 'call':
                case 'offer':
                case 'answer':
                case 'ice-candidate':
                case 'hangup':
                case 'ringing': {
                    // Relay signaling & chat messages to all other peers in the same room
                    if (currentRoom && rooms.has(currentRoom)) {
                        const roomClients = rooms.get(currentRoom);
                        for (const [cId, client] of roomClients.entries()) {
                            if (cId !== currentClientId && client.ws.readyState === WebSocket.OPEN) {
                                client.ws.send(JSON.stringify({
                                    type,
                                    roomId: currentRoom,
                                    payload
                                }));
                            }
                        }
                    }
                    break;
                }

                default:
                    break;
            }
        } catch (err) {
            console.error('Error handling message:', err);
        }
    });

    ws.on('close', () => {
        if (currentRoom && rooms.has(currentRoom) && currentClientId) {
            const roomClients = rooms.get(currentRoom);
            const client = roomClients.get(currentClientId);
            if (client && client.ws === ws) {
                const wasInVault = client.inVault;
                roomClients.delete(currentClientId);
                if (roomClients.size === 0) {
                    rooms.delete(currentRoom);
                } else if (wasInVault) {
                    for (const [cId, cl] of roomClients.entries()) {
                        if (cl.ws.readyState === WebSocket.OPEN) {
                            cl.ws.send(JSON.stringify({
                                type: 'peer_left',
                                roomId: currentRoom
                            }));
                        }
                    }
                }
            }
        }
    });
});

server.listen(PORT, () => {
    console.log(`Calc Vault Signaling Server running on port ${PORT}`);
});
