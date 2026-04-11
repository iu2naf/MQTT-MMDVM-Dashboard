const ALLOWED_FIELDS = [
    'Callsign', 'Id', 'RXFrequency', 'TXFrequency', 'Power', 
    'Latitude', 'Longitude', 'Height', 'Location', 'Description', 
    'Enable', 'Enabled', 'Name', 'Static', 'Startup', 
    'Reconnect', 'Revert'
];

let activeNodeId = null;
let currentNodesHash = '';
let currentStructureHash = '';
const expandedSections = new Set();
let repeatersMarkersLayer = null;

function toggleSection(sectionId) {
    const el = document.getElementById(sectionId);
    if (!el) return;
    if (el.classList.contains('expanded')) {
        el.classList.remove('expanded');
        expandedSections.delete(sectionId);
    } else {
        el.classList.add('expanded');
        expandedSections.add(sectionId);
    }
}

function safeId(...parts) {
    return 'id_' + btoa(unescape(encodeURIComponent(parts.join('|')))).replace(/[\=\+\/]/g, '');
}

function formatValue(key, value) {
    const lowerKey = key.toLowerCase();
    
    // Gestione specifica per i campi tecnici numerici (evita che valori come 0 o 1 diventino ON/OFF)
    if (lowerKey.includes('power')) return value + 'W';
    if (lowerKey.includes('height')) return value + 'm';
    if (lowerKey.includes('latitude') || lowerKey.includes('longitude')) return value;
    
    // Gestione Frequenze
    if (key === 'RXFrequency' || key === 'TXFrequency') {
        return (parseInt(value) / 1000000).toFixed(4) + ' MHz';
    }

    // Gestione valori booleani / stati
    if (value === '1' || value === 1 || value === true || (typeof value === 'string' && value.toLowerCase() === 'true')) {
        if (lowerKey.includes('enable')) return '<span class="badge on">ABILITATA</span>';
        return '<span class="badge on">ON</span>';
    }
    if (value === '0' || value === 0 || value === false || (typeof value === 'string' && value.toLowerCase() === 'false')) {
        if (lowerKey.includes('enable')) return '<span class="badge off">DISABILITATA</span>';
        return '<span class="badge off">OFF</span>';
    }

    if (typeof value === 'object') return JSON.stringify(value);
    return value;
}


function formatLabel(key) {
    const labels = {
        'RXFrequency': 'Freq. Ricezione',
        'TXFrequency': 'Freq. Trasmissione',
        'Power': 'Potenza TX',
        'Callsign': 'Nominativo',
        'Id': 'DMR ID',
        'Location': 'Posizione',
        'Description': 'Descrizione',
        'Enable': 'Stato',
        'Enabled': 'Stato',
        'Name': 'Nome Rete',
        'Height': 'Altezza',
        'Reconnect': 'Riconnetti',
        'Revert': 'Revert',
        'Startup': 'Server Avvio',
        'Static': 'Static'
    };
    return labels[key] || key.replace(/([A-Z])/g, ' $1').trim();
}

function updateRepeatersDashboard(data) {
    const statusDot = document.getElementById('statusDot');
    const statusText = document.getElementById('statusText');
    const messages = data.messages || {};
    const lastUpdates = data.last_update || {};
    
    const msgKeys = Object.keys(messages);
    if (!statusDot) return; // Prevent errors if tab not loaded
    
    if (msgKeys.length > 0) {
        statusDot.classList.add('active');
        statusText.innerHTML = 'Connesso &bull; Real-time';
        const spin = document.getElementById('loadingSpinner');
        if(spin) spin.style.display = 'none';
    } else {
        statusDot.classList.remove('active');
        statusText.textContent = 'Nessun dato';
        return;
    }

    const nodes = {};
    let totalTopics = 0;

    // Reset markers layer if map is available
    if (typeof map !== 'undefined' && map) {
        if (!repeatersMarkersLayer) {
            repeatersMarkersLayer = L.layerGroup().addTo(map);
        } else {
            repeatersMarkersLayer.clearLayers();
        }
    }

    msgKeys.forEach(topic => {
        const parts = topic.split('/');
        if (parts.length < 3) return;
        const nodeId = parts[1];
        const gateway = parts[2];
        const subTopic = parts.length > 3 ? parts.slice(3).join('/') : 'General';
        
        if (!nodes[nodeId]) nodes[nodeId] = {};
        if (!nodes[nodeId][gateway]) nodes[nodeId][gateway] = {};
        
        const payload = messages[topic] || {};
        totalTopics++;
        
        // --- MAP INTEGRATION ---
        if (typeof map !== 'undefined' && map && repeatersMarkersLayer) {
            if (payload.Latitude && payload.Longitude && payload.Latitude !== 0 && payload.Longitude !== 0) {
                // Pin on map
                const power = payload.Power || 0;
                // draw circle for coverage
                L.circle([payload.Latitude, payload.Longitude], {
                    color: '#00d4ff',
                    fillColor: '#00d4ff',
                    fillOpacity: 0.2,
                    radius: power * 1000 // roughly 1km per watt as proxy
                }).addTo(repeatersMarkersLayer)
                .bindPopup(`<b>${payload.Callsign || nodeId}</b><br>${gateway}<br>Tx: ${power}W`);
                
                L.marker([payload.Latitude, payload.Longitude], {
                    title: payload.Callsign || nodeId
                }).addTo(repeatersMarkersLayer).bindPopup(`<b>${payload.Callsign || nodeId}</b><br>${payload.Description || ''}`);
            }
        }
        // ------------------------

        nodes[nodeId][gateway][subTopic] = {
            data: payload,
            ts: lastUpdates[topic] || 0
        };
    });

    const activeNodes = Object.keys(nodes);
    document.getElementById('statNodes').textContent = activeNodes.length;
    document.getElementById('statTopics').textContent = totalTopics;

    const newNodesHash = activeNodes.sort().join(',');
    if (newNodesHash !== currentNodesHash) {
        currentNodesHash = newNodesHash;
        if (!activeNodeId && activeNodes.length > 0) {
            activeNodeId = activeNodes[0];
        }
        renderTabs(activeNodes);
    }

    if (activeNodeId && nodes[activeNodeId]) {
        renderNodeContent(nodes[activeNodeId]);
    }
}

function renderTabs(activeNodes) {
    const tabsContainer = document.getElementById('nodeTabs');
    if (!tabsContainer) return;
    
    tabsContainer.innerHTML = activeNodes.map(nodeId => `
        <button class="node-tab ${nodeId === activeNodeId ? 'active' : ''}" 
                onclick="switchRepeatersNode('${nodeId}')">
            ${nodeId}
        </button>
    `).join('');
}

function switchRepeatersNode(nodeId) {
    activeNodeId = nodeId;
    document.querySelectorAll('.node-tab').forEach(tab => {
        tab.classList.toggle('active', tab.textContent.trim() === nodeId);
    });
    const dashboardContent = document.getElementById('dashboardContent');
    if(dashboardContent) dashboardContent.innerHTML = '';
    currentStructureHash = ''; 
    fetchRepeatersLoop(); 
}

function renderNodeContent(gateways) {
    const container = document.getElementById('dashboardContent');
    if (!container) return;

    let html = '';
    let hashBuilder = '';
    Object.keys(gateways).sort().forEach(gwName => {
        hashBuilder += gwName + '|';
        const subTopics = gateways[gwName];
        hashBuilder += Object.keys(subTopics).sort().join('|') + '||';
    });
    
    const needsFullRender = (hashBuilder !== currentStructureHash);

    if (needsFullRender) {
        currentStructureHash = hashBuilder;
        
        Object.keys(gateways).sort().forEach(gwName => {
            const subTopics = gateways[gwName];
            const sectionId = safeId(activeNodeId, gwName);
            const isExpanded = expandedSections.has(sectionId) ? 'expanded' : '';

            html += `
            <div class="gateway-section ${isExpanded}" id="${sectionId}">
                <div class="gateway-header" onclick="toggleSection('${sectionId}')">
                    <h2>${gwName}</h2>
                    <span class="gateway-badge">${Object.keys(subTopics).length} Topics</span>
                    <span class="toggle-icon">▼</span>
                </div>
                <div class="topics-grid">
            `;

            Object.keys(subTopics).sort().forEach(topicName => {
                const item = subTopics[topicName];
                const cardId = safeId(activeNodeId, gwName, topicName);
                
                html += `
                    <div class="topic-card" id="${cardId}">
                        <div class="topic-card-header">
                            <div class="topic-title">${topicName}</div>
                            <div class="topic-time" id="time_${cardId}">...</div>
                        </div>
                        <div class="data-grid" id="data_${cardId}"></div>
                    </div>
                `;
            });
            html += `</div></div>`;
        });
        
        container.innerHTML = html;
        if(html === '') {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>Nessun dato relativo ai payload</h3>
                </div>
            `;
        }
    }

    Object.keys(gateways).forEach(gwName => {
        const subTopics = gateways[gwName];
        Object.keys(subTopics).forEach(topicName => {
            const item = subTopics[topicName];
            const cardId = safeId(activeNodeId, gwName, topicName);
            
            const timeEl = document.getElementById(`time_${cardId}`);
            if (timeEl && item.ts > 0) {
                const d = new Date(item.ts * 1000);
                timeEl.textContent = d.toLocaleTimeString();
            }

            const dataGrid = document.getElementById(`data_${cardId}`);
            if (dataGrid) {
                let dataHtml = '';
                const payloadKeys = Object.keys(item.data).sort();
                
                payloadKeys.forEach(k => {
                    if (!ALLOWED_FIELDS.includes(k) && k !== 'Configuration' && k !== 'Status') return;
                    
                    const rawVal = item.data[k];
                    let outHtml = formatValue(k, rawVal);
                    
                    const s = String(rawVal);
                    const isLong = s.length > 25 || typeof rawVal === 'object';
                    
                    dataHtml += `
                        <div class="data-item ${isLong ? 'full-width' : ''}">
                            <div class="data-label">${formatLabel(k)}</div>
                            <div class="data-value">${outHtml}</div>
                        </div>
                    `;
                });
                
                if (dataGrid.innerHTML !== dataHtml) {
                    dataGrid.innerHTML = dataHtml;
                }
            }
        });
    });
}

async function fetchRepeatersLoop() {
    try {
        const res = await fetch('/api/repeaters');
        if (!res.ok) throw new Error("HTTP error " + res.status);
        const data = await res.json();
        updateRepeatersDashboard(data);
    } catch (e) {
        console.error("Fetch repeaters error:", e);
        const statusDot = document.getElementById('statusDot');
        const statusText = document.getElementById('statusText');
        if(statusDot) {
            statusDot.classList.remove('active');
            statusDot.style.background = '#ef4444';
            statusDot.style.boxShadow = 'none';
        }
        if(statusText) statusText.textContent = 'Disconnesso / Errore';
    }
}

// Avvia il loop per i ripetitori
setInterval(fetchRepeatersLoop, 5000);
fetchRepeatersLoop();
