// Ultra-lightweight WebRTC Signaling Server for Calculator Vault
// Can be deployed on Glitch, Render, Railway, Fly.io, or run locally: `node server.js`

const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
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
