import React from 'react';

const KPICard = ({ title, value, icon, change, isPositive, suffix = "" }) => {
    return (
        <div className="glass-card">
            <div className="card-header">
                <h3 className="card-title">
                    {icon}
                    {title}
                </h3>
            </div>
            <div style={{ display: 'flex', alignItems: 'baseline', gap: '12px' }}>
                <div style={{ fontSize: '2rem', fontWeight: 'bold' }}>
                    {value}{suffix}
                </div>
                {change && (
                    <div className={isPositive ? 'text-success' : 'text-danger'} style={{ fontSize: '0.875rem', fontWeight: '500' }}>
                        {isPositive ? '↑' : '↓'} {change}%
                    </div>
                )}
            </div>
        </div>
    );
};

export default KPICard;
