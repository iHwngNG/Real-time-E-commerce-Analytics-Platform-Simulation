const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

export const fetchSummary = async () => {
    try {
        const res = await fetch(`${API_URL}/api/v1/metrics/summary`);
        if (!res.ok) throw new Error('Network response was not ok');
        return await res.json();
    } catch (error) {
        console.error("Fetch summary error:", error);
        return null;
    }
};

export const fetchTimeSeries = async (windowType = '1m', limit = 30) => {
    try {
        const res = await fetch(`${API_URL}/api/v1/metrics/timeseries?window_type=${windowType}&limit=${limit}`);
        if (!res.ok) throw new Error('Network response was not ok');
        return await res.json();
    } catch (error) {
        console.error("Fetch timeseries error:", error);
        return { data: [] };
    }
};

export const fetchTopProducts = async () => {
    try {
        const res = await fetch(`${API_URL}/api/v1/metrics/top-products`);
        if (!res.ok) throw new Error('Network response was not ok');
        return await res.json();
    } catch (error) {
        console.error("Fetch top products error:", error);
        return { data: [] };
    }
};

export const fetchBreakdown = async (dimension) => {
    try {
        const res = await fetch(`${API_URL}/api/v1/metrics/breakdown?dimension=${dimension}`);
        if (!res.ok) throw new Error('Network response was not ok');
        return await res.json();
    } catch (error) {
        console.error("Fetch breakdown error:", error);
        return { data: [] };
    }
};

export const fetchDailyReport = async () => {
    try {
        const res = await fetch(`${API_URL}/api/v1/reports/daily`);
        if (!res.ok) throw new Error('Network response was not ok');
        return await res.json();
    } catch (error) {
        console.error("Fetch daily report error:", error);
        return { data: [] };
    }
};

export const fetchFunnelAnalysis = async () => {
    try {
        const res = await fetch(`${API_URL}/api/v1/reports/funnel`);
        if (!res.ok) throw new Error('Network response was not ok');
        return await res.json();
    } catch (error) {
        console.error("Fetch funnel report error:", error);
        return { data: [] };
    }
};
