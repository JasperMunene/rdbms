import React, { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import api from '../services/api';

const POSTerminal = () => {
    const [amount, setAmount] = useState('0');
    const [phone, setPhone] = useState('');
    const [customer, setCustomer] = useState(null);
    const [loading, setLoading] = useState(false);
    const [status, setStatus] = useState('idle'); // idle, processing, success, error

    // "Index Scan" Simulation: Auto-fetch customer when valid phone length reached
    useEffect(() => {
        const fetchCustomer = async () => {
            if (phone.length >= 10) { // Assuming min phone length
                setLoading(true);
                try {
                    const res = await api.get(`/customers/${phone}`);
                    setCustomer(res.data);
                } catch (err) {
                    setCustomer(null); // Not found or error
                } finally {
                    setLoading(false);
                }
            } else {
                setCustomer(null);
            }
        };

        const timeout = setTimeout(fetchCustomer, 300); // 300ms debounce
        return () => clearTimeout(timeout);
    }, [phone]);

    const handleDigit = (digit) => {
        setAmount(prev => prev === '0' ? digit : prev + digit);
    };

    const handleClear = () => setAmount('0');

    const handlePayment = async () => {
        setStatus('processing');
        try {
            await api.post('/transactions', {
                amount: parseFloat(amount),
                customer_phone: phone,
                business_name: 'Jasper Tech' // Hardcoded for demo if not in merchant context
            });
            setStatus('success');
            setTimeout(() => {
                setStatus('idle');
                setAmount('0');
                setPhone('');
                setCustomer(null);
            }, 3000);
        } catch (err) {
            console.error(err);
            setStatus('error');
            setTimeout(() => setStatus('idle'), 3000);
        }
    };

    return (
        <div className="max-w-4xl mx-auto grid grid-cols-1 md:grid-cols-2 gap-8 h-[calc(100vh-140px)]">
            {/* Left Col: Amount Input & Keypad */}
            <div className="glass-panel rounded-2xl p-8 flex flex-col justify-between">
                <div className="text-right mb-8">
                    <p className="text-gray-400 mb-1">Total Amount</p>
                    <div className="text-5xl font-mono font-bold text-white tracking-tighter">
                        <span className="text-2xl text-fintech-primary mr-2">KES</span>
                        {parseFloat(amount).toLocaleString()}
                    </div>
                </div>

                <div className="grid grid-cols-3 gap-4 flex-1">
                    {[1, 2, 3, 4, 5, 6, 7, 8, 9, 'C', 0, '.'].map((key) => (
                        <button
                            key={key}
                            onClick={() => key === 'C' ? handleClear() : handleDigit(key.toString())}
                            className={`rounded-xl text-2xl font-bold transition-all active:scale-95 ${key === 'C'
                                    ? 'bg-red-500/20 text-red-400 hover:bg-red-500/30'
                                    : 'bg-white/5 text-white hover:bg-white/10'
                                }`}
                        >
                            {key}
                        </button>
                    ))}
                </div>
            </div>

            {/* Right Col: Customer & Action */}
            <div className="flex flex-col gap-6">
                {/* Customer Lookup Card */}
                <div className="glass-card p-6 rounded-2xl relative overflow-hidden">
                    <h3 className="text-lg font-bold text-white mb-4 flex items-center gap-2">
                        <span className="w-2 h-2 rounded-full bg-fintech-primary"></span>
                        Customer Identification
                    </h3>

                    <div className="space-y-4">
                        <div>
                            <label className="text-xs text-gray-400 uppercase">Phone Number (Index Lookup)</label>
                            <input
                                type="text"
                                value={phone}
                                onChange={(e) => setPhone(e.target.value)}
                                placeholder="+254..."
                                className="w-full bg-black/30 border border-white/10 rounded-lg px-4 py-3 text-white text-lg font-mono focus:border-fintech-primary focus:outline-none transition-colors mt-1"
                            />
                        </div>

                        <AnimatePresence>
                            {loading && (
                                <motion.div
                                    initial={{ opacity: 0, height: 0 }}
                                    animate={{ opacity: 1, height: 'auto' }}
                                    exit={{ opacity: 0, height: 0 }}
                                    className="flex items-center gap-2 text-fintech-primary text-sm"
                                >
                                    <div className="w-4 h-4 border-2 border-current border-t-transparent rounded-full animate-spin"></div>
                                    Running Index Scan on 'phone'...
                                </motion.div>
                            )}

                            {customer && (
                                <motion.div
                                    initial={{ opacity: 0, scale: 0.9 }}
                                    animate={{ opacity: 1, scale: 1 }}
                                    className="bg-fintech-success/10 border border-fintech-success/20 rounded-xl p-4"
                                >
                                    <div className="flex justify-between items-start">
                                        <div>
                                            <p className="text-fintech-success font-bold text-lg">{customer.full_name}</p>
                                            <p className="text-white/60 text-sm">Valid Customer Found</p>
                                        </div>
                                        <div className="bg-fintech-success text-black text-xs font-bold px-2 py-1 rounded">MATCH</div>
                                    </div>
                                </motion.div>
                            )}
                        </AnimatePresence>
                    </div>
                </div>

                {/* Action Button */}
                <button
                    onClick={handlePayment}
                    disabled={status === 'processing' || amount === '0' || !phone}
                    className={`mt-auto w-full py-6 rounded-2xl font-bold text-xl transition-all relative overflow-hidden ${status === 'success' ? 'bg-fintech-success text-black' :
                            status === 'error' ? 'bg-red-500 text-white' :
                                'bg-gradient-to-r from-fintech-primary to-fintech-secondary text-white hover:opacity-90 disabled:opacity-50 disabled:cursor-not-allowed'
                        }`}
                >
                    {status === 'processing' ? 'Processing Transaction...' :
                        status === 'success' ? 'Payment Approved!' :
                            status === 'error' ? 'Transaction Failed' :
                                `Charge KES ${parseFloat(amount).toLocaleString()}`}
                </button>
            </div>
        </div>
    );
};

export default POSTerminal;
