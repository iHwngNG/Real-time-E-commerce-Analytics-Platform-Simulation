import { Activity, Clock } from 'lucide-react';

const SystemStatusBar = ({ wsStatus, summary }) => {
    const eventsPerSec = summary?.events_1m ? (parseInt(summary.events_1m) / 60).toFixed(1) : 0;

    return (
        <div className="status-bar">
            <div className="status-badge" title={`WebSocket: ${wsStatus}`}>
                <div className={`status-indicator ${wsStatus}`}></div>
                <span style={{ textTransform: 'capitalize' }}>{wsStatus}</span>
            </div>

            <div className="status-badge">
                <Activity size={14} />
                <span>Rate: {eventsPerSec} evt/s</span>
            </div>

            <div className="status-badge">
                <Clock size={14} />
                <span>Lag: OK</span>
            </div>
        </div>
    );
};

export default SystemStatusBar;
