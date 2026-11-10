class Timeline {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.events = [];
    }

    addEvent(type, description, nodeId) {
        const event = {
            type: type,
            description: description,
            node: nodeId,
            time: new Date().toLocaleTimeString()
        };
        this.events.push(event);
        this.render();
    }

    render() {
        // Mostra apenas os últimos 15 eventos
        const recent = this.events.slice(-15);
        
        this.container.innerHTML = recent.map(e => {
            const color = this.getColor(e.type);
            return `
                <div class="timeline-item" style="border-left: 3px solid ${color};">
                    <div class="timeline-time">${e.time}</div>
                    <div class="timeline-desc">
                        <b>${e.node}</b>: ${e.description}
                    </div>
                </div>
            `;
        }).join('');
    }

    getColor(type) {
        const colors = {
            'REQUEST': '#1abc9c',
            'TOKEN': '#9b59b6',
            'RELEASE': '#3498db',
            'ELECTION': '#f39c12',
            'COORDINATOR': '#e67e22',
            'ATTACK': '#e74c3c',
            'KILL': '#c0392b'
        };
        return colors[type] || '#95a5a6';
    }
}