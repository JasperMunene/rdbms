import React from 'react';
import { Outlet, Link, useLocation } from 'react-router-dom';
import { motion } from 'framer-motion';

const Layout = () => {
    const location = useLocation();

    const navItems = [
        { path: '/', label: 'Dashboard' },
        { path: '/pos', label: 'POS Terminal' },
        { path: '/transactions', label: 'Transactions' },
        { path: '/merchants', label: 'Merchants (Admin)' },
    ];

    return (
        <div className="flex h-screen bg-fintech-bg text-fintech-text overflow-hidden">
            {/* Sidebar */}
            <aside className="w-64 glass-panel border-r border-white/10 flex flex-col">
                <div className="p-6 border-b border-white/10">
                    <h1 className="text-2xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-fintech-primary to-fintech-secondary">
                        PesaSQL
                    </h1>
                    <p className="text-xs text-gray-400 mt-1">High-Perf DB Engine</p>
                </div>

                <nav className="flex-1 p-4 space-y-2">
                    {navItems.map((item) => {
                        const isActive = location.pathname === item.path;
                        return (
                            <Link
                                key={item.path}
                                to={item.path}
                                className={`block px-4 py-3 rounded-lg transition-all duration-300 ${isActive
                                        ? 'bg-gradient-to-r from-fintech-primary/20 to-fintech-secondary/20 text-white border border-fintech-primary/30'
                                        : 'text-gray-400 hover:text-white hover:bg-white/5'
                                    }`}
                            >
                                {item.label}
                            </Link>
                        );
                    })}
                </nav>

                <div className="p-4 border-t border-white/10">
                    <div className="flex items-center gap-3">
                        <div className="w-8 h-8 rounded-full bg-gradient-to-tr from-fintech-primary to-fintech-secondary"></div>
                        <div>
                            <p className="text-sm font-medium">Merchant User</p>
                            <p className="text-xs text-fintech-success">● Connected</p>
                        </div>
                    </div>
                </div>
            </aside>

            {/* Main Content */}
            <main className="flex-1 overflow-y-auto relative">
                {/* Background Ambient Glow */}
                <div className="absolute top-0 left-0 w-full h-full overflow-hidden pointer-events-none z-0">
                    <div className="absolute -top-[20%] -left-[10%] w-[50%] h-[50%] bg-fintech-primary/5 rounded-full blur-[120px]"></div>
                    <div className="absolute top-[40%] -right-[10%] w-[40%] h-[60%] bg-fintech-secondary/5 rounded-full blur-[120px]"></div>
                </div>

                <div className="relative z-10 p-8">
                    <Outlet />
                </div>
            </main>
        </div>
    );
};

export default Layout;
