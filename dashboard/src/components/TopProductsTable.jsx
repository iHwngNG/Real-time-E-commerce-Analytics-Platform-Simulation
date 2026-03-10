import React from 'react';

const TopProductsTable = ({ topProducts }) => {
    return (
        <div className="glass-card">
            <h3 className="card-title" style={{ marginBottom: '20px' }}>Trending Products</h3>
            <div className="glass-table-wrapper">
                <table className="glass-table">
                    <thead>
                        <tr>
                            <th>Rank</th>
                            <th>Product ID</th>
                            <th style={{ textAlign: 'right' }}>Score</th>
                        </tr>
                    </thead>
                    <tbody>
                        {(topProducts || []).slice(0, 5).map((p, idx) => (
                            <tr key={p.product_id}>
                                <td>
                                    <span style={{
                                        display: 'inline-block',
                                        width: '24px',
                                        height: '24px',
                                        borderRadius: '50%',
                                        background: idx < 3 ? 'rgba(59, 130, 246, 0.2)' : 'rgba(255,255,255,0.05)',
                                        color: idx < 3 ? '#60a5fa' : '#94a3b8',
                                        textAlign: 'center',
                                        lineHeight: '24px',
                                        fontWeight: 'bold',
                                        fontSize: '12px'
                                    }}>
                                        {idx + 1}
                                    </span>
                                </td>
                                <td style={{ fontFamily: 'monospace', fontSize: '12px' }}>
                                    {p.product_id.split('-')[0]}...
                                </td>
                                <td style={{ textAlign: 'right', fontWeight: 'bold' }}>{Math.floor(p.score).toLocaleString()}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
};

export default TopProductsTable;
