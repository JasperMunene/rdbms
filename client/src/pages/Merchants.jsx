import React, { useEffect, useState } from 'react';
import api from '../services/api';
import Table from '../components/Table';

const Merchants = () => {
    const [merchants, setMerchants] = useState([]);
    const [loading, setLoading] = useState(true);

    const fetchMerchants = async () => {
        try {
            const res = await api.get('/merchants');
            setMerchants(res.data);
        } catch (err) {
            console.error(err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchMerchants();
    }, []);

    const handleDelete = async (row) => {
        if (!window.confirm(`Delete merchant ${row.business_name}?`)) return;
        try {
            await api.delete(`/merchants/${row.merchant_id}`); // Needs DELETE endpoint
            fetchMerchants();
        } catch (err) {
            alert('Failed to delete: ' + err.message);
        }
    };

    const columns = [
        { header: 'ID', key: 'merchant_id' },
        { header: 'Business Name', key: 'business_name', render: r => <span className="font-bold text-white">{r.business_name}</span> },
        { header: 'Owner Email (Joined)', key: 'email', render: r => <span className="text-fintech-primary text-sm">{r.email}</span> },
        { header: 'Till Number', key: 'mpesa_till' },
        { header: 'Country', key: 'country' },
        {
            header: 'Status', key: 'status', render: r => (
                <span className={`px-2 py-0.5 rounded text-xs ${r.status === 'active' ? 'bg-fintech-success/10 text-fintech-success' : 'text-gray-500'}`}>
                    {r.status}
                </span>
            )
        }
    ];

    return (
        <div className="space-y-6">
            <div className="flex justify-between items-center">
                <div>
                    <h2 className="text-2xl font-bold text-white">Merchants</h2>
                    <p className="text-gray-400">Manage registered businesses</p>
                </div>
                {/* Admin Add feature could go here */}
            </div>

            <Table
                columns={columns}
                data={merchants}
                loading={loading}
                onDelete={handleDelete}
            />
        </div>
    );
};

export default Merchants;
