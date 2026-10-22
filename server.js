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
        console.log(`[${new Date().toISOString()}] OTA Version check request from ${req.socket.remoteAddress}`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            versionCode: 20,
            versionName: '1.4.4',
            apkUrl: 'https://calc-vault-server.onrender.com/download/app-release.apk',
            changelog: '1:1 Samsung One UI Calculator replica, normal notification chime, instant call decline, and reliable answering'
        }));
        return;
    }

    if (req.url && req.url.startsWith('/decline-call')) {
        try {
            const urlObj = new URL(req.url, `http://${req.headers.host}`);
            const rId = urlObj.searchParams.get('roomId');
            console.log(`[${new Date().toISOString()}] /decline-call HTTP received for room ${rId}`);
            if (rId && rooms.has(rId)) {
                activeCalls.delete(rId);
                const roomClients = rooms.get(rId);
                for (const [cId, client] of roomClients.entries()) {
                    if (client.ws.readyState === WebSocket.OPEN) {
                        client.ws.send(JSON.stringify({
                            type: 'hangup',
                            roomId: rId,
                            payload: { reason: 'declined' }
                        }));
                    }
                }
            }
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true }));
            return;
        } catch (err) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: err.message }));
            return;
        }
    }

    if (req.url === '/download/app-release.apk') {
        console.log(`[${new Date().toISOString()}] OTA APK download request from ${req.socket.remoteAddress}`);
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

// Map of roomId -> { callerClientId, isVideo, offer, timestamp }
const activeCalls = new Map();

// Map of roomId -> Array of stored message objects: { id, action, payload, timestamp }
const roomHistory = new Map();

const DATA_DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) {
    try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (_) {}
}

function loadRoomHistory(roomId) {
    if (roomHistory.has(roomId)) return roomHistory.get(roomId);
    const filePath = path.join(DATA_DIR, `history_${roomId}.json`);
    if (fs.existsSync(filePath)) {
        try {
            const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
            if (Array.isArray(data)) {
                roomHistory.set(roomId, data);
                return data;
            }
        } catch (_) {}
    }
    const empty = [];
    roomHistory.set(roomId, empty);
    return empty;
}

function saveRoomHistory(roomId) {
    const list = roomHistory.get(roomId);
    if (!list) return;
    const filePath = path.join(DATA_DIR, `history_${roomId}.json`);
    try {
        fs.writeFileSync(filePath, JSON.stringify(list.slice(-300)), 'utf8');
    } catch (_) {}
}

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

                    // Send complete offline/stored message history to the newly connected/joined client!
                    const history = loadRoomHistory(roomId);
                    if (history.length > 0) {
                        ws.send(JSON.stringify({
                            type: 'sync_history',
                            roomId,
                            messages: history.map(item => item.payload)
                        }));
                    }

                    // Check if an incoming call was already in progress for this room (< 45s old)
                    const pendingCall = activeCalls.get(roomId);
                    if (pendingCall && (Date.now() - pendingCall.timestamp) < 45000 && pendingCall.callerClientId !== currentClientId) {
                        ws.send(JSON.stringify({
                            type: 'call',
                            roomId,
                            payload: { video: pendingCall.isVideo }
                        }));
                        if (pendingCall.offer) {
                            ws.send(JSON.stringify({
                                type: 'offer',
                                roomId,
                                payload: pendingCall.offer
                            }));
                        }
                    }

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

                case 'sync_request': {
                    if (currentRoom) {
                        const history = loadRoomHistory(currentRoom);
                        ws.send(JSON.stringify({
                            type: 'sync_history',
                            roomId: currentRoom,
                            messages: history.map(item => item.payload)
                        }));
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

                case 'chat': {
                    if (currentRoom && payload) {
                        const action = payload.action;
                        const history = loadRoomHistory(currentRoom);

                        if (action === 'message' && payload.message) {
                            const msgId = payload.message.id;
                            // Avoid duplicate storage
                            if (!history.some(item => item.id === msgId)) {
                                history.push({
                                    id: msgId,
                                    action: 'message',
                                    payload: payload,
                                    timestamp: Date.now()
                                });
                                if (history.length > 300) history.shift();
                                saveRoomHistory(currentRoom);
                            }
                        } else if (action === 'unsend' && payload.id) {
                            const unsendId = payload.id;
                            const idx = history.findIndex(item => item.id === unsendId);
                            if (idx !== -1) {
                                history.splice(idx, 1);
                                saveRoomHistory(currentRoom);
                            }
                        } else if (action === 'reaction' && payload.id) {
                            const targetId = payload.id;
                            const item = history.find(it => it.id === targetId);
                            if (item && item.payload && item.payload.message) {
                                item.payload.message.reaction = payload.reaction;
                                saveRoomHistory(currentRoom);
                            }
                        } else if (action === 'view_once_opened' && payload.id) {
                            const targetId = payload.id;
                            const item = history.find(it => it.id === targetId);
                            if (item && item.payload && item.payload.message) {
                                item.payload.message.isOpened = true;
                                delete item.payload.mediaData; // incinerate media payload
                                saveRoomHistory(currentRoom);
                            }
                        }

                        // Relay chat payload immediately to all other connected peers in the room
                        if (rooms.has(currentRoom)) {
                            const roomClients = rooms.get(currentRoom);
                            for (const [cId, client] of roomClients.entries()) {
                                if (cId !== currentClientId && client.ws.readyState === WebSocket.OPEN) {
                                    client.ws.send(JSON.stringify({
                                        type: 'chat',
                                        roomId: currentRoom,
                                        payload
                                    }));
                                }
                            }
                        }
                    }
                    break;
                }

                case 'call': {
                    if (currentRoom) {
                        activeCalls.set(currentRoom, {
                            callerClientId: currentClientId,
                            isVideo: !!(payload && payload.video),
                            timestamp: Date.now()
                        });
                    }
                    // Relay call to peers
                    if (currentRoom && rooms.has(currentRoom)) {
                        const roomClients = rooms.get(currentRoom);
                        for (const [cId, client] of roomClients.entries()) {
                            if (cId !== currentClientId && client.ws.readyState === WebSocket.OPEN) {
                                client.ws.send(JSON.stringify({ type, roomId: currentRoom, payload }));
                            }
                        }
                    }
                    break;
                }

                case 'offer': {
                    if (currentRoom && activeCalls.has(currentRoom)) {
                        const c = activeCalls.get(currentRoom);
                        c.offer = payload;
                    }
                    // Relay offer to peers
                    if (currentRoom && rooms.has(currentRoom)) {
                        const roomClients = rooms.get(currentRoom);
                        for (const [cId, client] of roomClients.entries()) {
                            if (cId !== currentClientId && client.ws.readyState === WebSocket.OPEN) {
                                client.ws.send(JSON.stringify({ type, roomId: currentRoom, payload }));
                            }
                        }
                    }
                    break;
                }

                case 'answer': {
                    if (currentRoom) {
                        activeCalls.delete(currentRoom);
                    }
                    if (currentRoom && rooms.has(currentRoom)) {
                        const roomClients = rooms.get(currentRoom);
                        for (const [cId, client] of roomClients.entries()) {
                            if (cId !== currentClientId && client.ws.readyState === WebSocket.OPEN) {
                                client.ws.send(JSON.stringify({ type, roomId: currentRoom, payload }));
                            }
                        }
                    }
                    break;
                }

                case 'hangup': {
                    if (currentRoom) {
                        activeCalls.delete(currentRoom);
                    }
                    if (currentRoom && rooms.has(currentRoom)) {
                        const roomClients = rooms.get(currentRoom);
                        for (const [cId, client] of roomClients.entries()) {
                            if (cId !== currentClientId && client.ws.readyState === WebSocket.OPEN) {
                                client.ws.send(JSON.stringify({ type, roomId: currentRoom, payload }));
                            }
                        }
                    }
                    break;
                }

                case 'typing':
                case 'ice-candidate':
                case 'ringing': {
                    // Relay signaling messages to all other peers in the same room
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
