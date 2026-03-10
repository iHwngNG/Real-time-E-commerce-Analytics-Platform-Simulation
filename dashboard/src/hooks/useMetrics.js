import { useState, useEffect } from 'react';
import { fetchSummary, fetchTimeSeries, fetchTopProducts, fetchDailyReport } from '../services/api';

// Poll API every 5 seconds if WS is disconnected, or for initial load
export function useMetrics() {
    const [summary, setSummary] = useState(null);
    const [timeSeries, setTimeSeries] = useState([]);
    const [topProducts, setTopProducts] = useState([]);
    const [dailyReport, setDailyReport] = useState([]);
    const [loading, setLoading] = useState(true);

    const loadData = async () => {
        try {
            const [_summary, _timeSeries, _topProducts, _daily] = await Promise.all([
                fetchSummary(),
                fetchTimeSeries('1m', 30), // get last 30 minutes of 1m windows
                fetchTopProducts(),
                fetchDailyReport()
            ]);

            if (_summary?.data) setSummary(_summary.data);
            if (_timeSeries?.data) setTimeSeries(_timeSeries.data.reverse()); // chronological order
            if (_topProducts?.data) setTopProducts(_topProducts.data);
            if (_daily?.data) setDailyReport(_daily.data.reverse());
            setLoading(false);
        } catch (err) {
            console.error(err);
            setLoading(false);
        }
    };

    useEffect(() => {
        loadData();
        const interval = setInterval(loadData, 5000);
        return () => clearInterval(interval);
    }, []);

    return { summary, timeSeries, topProducts, dailyReport, loading };
}
