import { useState, useEffect, useRef } from 'react';

const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8000';

export function useWebSocket() {
    const [data, setData] = useState(null);
    const [status, setStatus] = useState('disconnected'); // connected, disconnected, reconnecting
    const ws = useRef(null);

    useEffect(() => {
        let reconnectTimeout;

        const connect = () => {
            setStatus('reconnecting');
            ws.current = new WebSocket(`${WS_URL}/ws/live`);

            ws.current.onopen = () => {
                setStatus('connected');
                // Tell server to push 'metrics-update'
                ws.current.send(JSON.stringify({ subscribe: ["summary", "top-products"] }));
            };

            ws.current.onmessage = (event) => {
                try {
                    const parsed = JSON.parse(event.data);
                    setData(parsed);
                } catch (e) {
                    console.error("WS Parse error", e);
                }
            };

            ws.current.onclose = () => {
                setStatus('disconnected');
                reconnectTimeout = setTimeout(connect, 5000);
            };

            ws.current.onerror = (err) => {
                console.error("WebSocket error observed:", err);
                ws.current.close();
            };
        };

        connect();

        return () => {
            clearTimeout(reconnectTimeout);
            if (ws.current) {
                ws.current.close();
            }
        };
    }, []);

    return { data, status };
}
