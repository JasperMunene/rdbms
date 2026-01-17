import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { motion } from 'framer-motion';

const Login = () => {
    const navigate = useNavigate();
    const [loading, setLoading] = useState(false);

    const handleLogin = (e) => {
        e.preventDefault();
        setLoading(true);
        // Simulate Login (In real app, call API)
        setTimeout(() => {
            navigate('/');
        }, 1500);
    };

    return (
        <div className="min-h-screen flex items-center justify-center bg-fintech-bg relative overflow-hidden">
            {/* Background Effects */}
            <div className="absolute top-0 left-0 w-full h-full overflow-hidden pointer-events-none">
                <div className="absolute top-[10%] left-[20%] w-[400px] h-[400px] bg-fintech-primary/10 rounded-full blur-[100px]"></div>
                <div className="absolute bottom-[20%] right-[20%] w-[300px] h-[300px] bg-fintech-secondary/10 rounded-full blur-[100px]"></div>
            </div>

            <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                className="glass-panel p-8 rounded-2xl w-full max-w-md relative z-10"
            >
                <div className="text-center mb-8">
                    <h1 className="text-3xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-fintech-primary to-fintech-secondary">
                        PesaSQL
                    </h1>
                    <p className="text-gray-400 mt-2">Secure Merchant Access</p>
                </div>

                <form onSubmit={handleLogin} className="space-y-6">
                    <div>
                        <label className="block text-sm text-gray-400 mb-2">Email Address</label>
                        <input
                            type="email"
                            defaultValue="jasper@example.com"
                            className="w-full bg-black/20 border border-white/10 rounded-lg px-4 py-3 text-white focus:outline-none focus:border-fintech-primary transition-colors"
                        />
                    </div>
                    <div>
                        <label className="block text-sm text-gray-400 mb-2">Password</label>
                        <input
                            type="password"
                            defaultValue="password"
                            className="w-full bg-black/20 border border-white/10 rounded-lg px-4 py-3 text-white focus:outline-none focus:border-fintech-primary transition-colors"
                        />
                    </div>

                    <button
                        type="submit"
                        disabled={loading}
                        className="w-full py-4 rounded-lg font-bold text-black bg-gradient-to-r from-fintech-primary to-fintech-secondary hover:opacity-90 transition-opacity disabled:opacity-50"
                    >
                        {loading ? 'Hashing & Verifying...' : 'Login Securely'}
                    </button>
                </form>

                <div className="mt-6 text-center text-xs text-gray-500">
                    Powered by Custom Python SQL Engine
                </div>
            </motion.div>
        </div>
    );
};

export default Login;
