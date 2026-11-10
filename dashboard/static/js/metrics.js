class LatencyHeatmap {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.waitTimes = {}; // { nodeId: [tempos] }
    }

    addWaitTime(nodeId, timeMs) {
        if (!this.waitTimes[nodeId]) this.waitTimes[nodeId] = [];
        this.waitTimes[nodeId].push(timeMs);
        if (this.waitTimes[nodeId].length > 20) this.waitTimes[nodeId].shift();
        this.render();
    }

    render() {
        const html = Object.entries(this.waitTimes).map(([nodeId, times]) => {
            const avg = times.reduce((a, b) => a + b, 0) / times.length;
            const max = Math.max(...times);
            const intensity = Math.min(avg / 5000, 1); // 5s = 100%
            const color = `rgba(231, 76, 60, ${intensity})`;
            
            return `
                <div class="heatmap-row">
                    <span class="heatmap-node">${nodeId}</span>
                    <div class="heatmap-bar" style="background: ${color}; width: ${Math.max(intensity * 100, 5)}%;">
                        ${avg.toFixed(0)}ms
                    </div>
                </div>
            `;
        }).join('');
        
        this.container.innerHTML = html || '<p>Nenhum dado ainda.</p>';
    }
}