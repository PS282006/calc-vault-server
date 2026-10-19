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
            versionCode: 4,
            versionName: '1.0.3',
            apkUrl: 'https://calc-vault-server.onrender.com/download/app-release.apk',
            changelog: 'Native ear proximity sensor, crystal-clear 32kbps VoLTE voice, in-app auto updates'
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

// Map of roomId -> Set of client sockets
const rooms = new Map();

wss.on('connection', (ws) => {
    let currentRoom = null;

    ws.on('message', (messageText) => {
        try {
            const data = JSON.parse(messageText);
            const { type, roomId, payload } = data;

            switch (type) {
                case 'join': {
                    currentRoom = roomId;
                    if (!rooms.has(roomId)) {
                        rooms.set(roomId, new Set());
                    }
                    const roomClients = rooms.get(roomId);
                    roomClients.add(ws);

                    // Notify client whether peer is already present
                    const peerPresent = roomClients.size > 1;
                    ws.send(JSON.stringify({
                        type: 'joined',
                        roomId,
                        peerPresent
                    }));

                    // If a peer was waiting, inform them someone joined
                    for (const client of roomClients) {
                        if (client !== ws && client.readyState === WebSocket.OPEN) {
                            client.send(JSON.stringify({
                                type: 'peer_joined',
                                roomId
                            }));
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

                case 'call':
                case 'offer':
                case 'answer':
                case 'ice-candidate':
                case 'hangup':
                case 'ringing': {
                    // Relay signaling messages to all other peers in the same room
                    if (currentRoom && rooms.has(currentRoom)) {
                        const roomClients = rooms.get(currentRoom);
                        for (const client of roomClients) {
                            if (client !== ws && client.readyState === WebSocket.OPEN) {
                                client.send(JSON.stringify({
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
        if (currentRoom && rooms.has(currentRoom)) {
            const roomClients = rooms.get(currentRoom);
            roomClients.delete(ws);
            if (roomClients.size === 0) {
                rooms.delete(currentRoom);
            } else {
                for (const client of roomClients) {
                    if (client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({
                            type: 'peer_left',
                            roomId: currentRoom
                        }));
                    }
                }
            }
        }
    });
});

server.listen(PORT, () => {
    console.log(`Calc Vault Signaling Server running on port ${PORT}`);
});
