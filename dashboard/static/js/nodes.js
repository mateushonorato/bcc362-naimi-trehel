class NodeMonitor {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.nodes = {}; // { nodeId: { lastHeartbeat, status } }
        setInterval(() => this.updateStatuses(), 1000);
    }

    update(nodeId) {
        this.nodes[nodeId] = {
            lastHeartbeat: Date.now(),
            status: 'healthy'
        };
        this.render();
    }

    updateStatuses() {
        const now = Date.now();
        let changed = false;
        
        for (const [nodeId, data] of Object.entries(this.nodes)) {
            const elapsed = now - data.lastHeartbeat;
            let newStatus = 'healthy';
            
            if (elapsed > 3000 && elapsed < 6000) {
                newStatus = 'warning';
            } else if (elapsed >= 6000) {
                newStatus = 'dead';
            }
            
            if (data.status !== newStatus) {
                data.status = newStatus;
                changed = true;
            }
        }
        
        if (changed) this.render();
    }

    render() {
        const html = Object.entries(this.nodes).map(([nodeId, data]) => {
            const emoji = { healthy: '🟢', warning: '🟡', dead: '🔴' }[data.status];
            return `<span class="node-badge node-${data.status}">${emoji} ${nodeId}</span>`;
        }).join('');
        
        this.container.innerHTML = html;
    }
}